/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.channel;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft-based Committer implementation using Raft consensus for coordinator election.
 *
 * <p>This implementation provides:
 * <ul>
 *   <li>Automatic leader election using Raft consensus algorithm
 *   <li>Split-brain prevention via majority quorum
 *   <li>Fast failover (5-10 seconds)
 *   <li>Zero coordinator churn during rebalancing
 *   <li>No external dependencies (no ZooKeeper/etcd)
 * </ul>
 */
public class RaftCommitterImpl implements Committer {

  private static final Logger LOG = LoggerFactory.getLogger(RaftCommitterImpl.class);

  private ChannelThread coordinatorThread;
  private RaftWorker worker;
  private Catalog catalog;
  private IcebergSinkConfig config;
  private SinkTaskContext context;
  private KafkaClientFactory clientFactory;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  // Raft consensus state
  private RaftState raftState;

  private void initialize(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    if (isInitialized.compareAndSet(false, true)) {
      this.catalog = icebergCatalog;
      this.config = icebergSinkConfig;
      this.context = sinkTaskContext;
      this.clientFactory = new KafkaClientFactory(config.kafkaProps());

      // Initialize Raft consensus state
      this.raftState = new RaftState(config.taskId());
      LOG.info("Initialized Raft consensus for task {}", config.taskId());
    }
  }

  /**
   * Determines if this task should be the coordinator using Raft consensus.
   *
   * @return true if this task is the Raft leader (coordinator)
   */
  private boolean isLeader() {
    ConsumerGroupDescription groupDesc;
    try (Admin admin = clientFactory.createAdmin()) {
      groupDesc = KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
    }

    Collection<MemberDescription> members = groupDesc.members();

    // Update Raft state with current cluster size for quorum calculation
    raftState.updateClusterSize(members.size());

    // Check if we should start an election
    if (raftState.isElectionTimeoutExpired() && !raftState.isLeader()) {
      LOG.info("Election timeout expired, starting new election");
      if (raftState.startElection()) {
        // Send RequestVote to all members via control topic
        if (worker != null) {
          worker.sendRaftRequestVote(config.taskId(), raftState.getCurrentTerm());
        }
      }
    }

    // Return whether this task is the current Raft leader
    return raftState.isLeader();
  }

  /**
   * Handles incoming Raft RequestVote message.
   *
   * @param candidateId ID of candidate requesting vote
   * @param term Election term
   */
  @VisibleForTesting
  void handleRaftRequestVote(String candidateId, long term) {
    boolean voteGranted = raftState.handleVoteRequest(candidateId, term);
    // Send vote response via worker
    if (worker != null) {
      worker.sendRaftVoteResponse(config.taskId(), term, voteGranted);
    }
  }

  /**
   * Handles incoming Raft VoteResponse message.
   *
   * @param voterId ID of voter
   * @param term Election term
   * @param voteGranted Whether vote was granted
   */
  @VisibleForTesting
  void handleRaftVoteResponse(String voterId, long term, boolean voteGranted) {
    boolean becameLeader = raftState.handleVoteResponse(voterId, term, voteGranted);
    if (becameLeader) {
      LOG.info("Task {} became Raft leader, starting coordinator", config.taskId());
      startCoordinator();
    }
  }

  /**
   * Handles incoming Raft Heartbeat message from leader.
   *
   * @param leaderId ID of leader
   * @param term Election term
   */
  @VisibleForTesting
  void handleRaftHeartbeat(String leaderId, long term) {
    raftState.handleHeartbeat(leaderId, term);

    // If we receive heartbeat and we're currently coordinator, step down
    if (coordinatorThread != null && !raftState.isLeader()) {
      LOG.info(
          "Task {} received heartbeat from new leader {}, stepping down",
          config.taskId(),
          leaderId);
      stopCoordinator();
    }
  }

  @Override
  public void start(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    throw new UnsupportedOperationException(
        "The method start(Catalog, IcebergSinkConfig, SinkTaskContext) is deprecated and will be removed in 2.0.0. "
            + "Use open(Catalog, IcebergSinkConfig, SinkTaskContext, Collection<TopicPartition>) instead.");
  }

  @Override
  public void open(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext,
      Collection<TopicPartition> addedPartitions) {
    initialize(icebergCatalog, icebergSinkConfig, sinkTaskContext);
    // With Raft, coordinator election is done via consensus, not partition assignment
    // So we don't immediately start coordinator here
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException(
        "The method stop() is deprecated and will be removed in 2.0.0. "
            + "Use close(Collection<TopicPartition>) instead.");
  }

  @Override
  public void close(Collection<TopicPartition> closedPartitions) {
    // Always try to stop the worker to avoid duplicates
    stopWorker();

    // Defensive: close called without prior initialization (should not happen)
    if (!isInitialized.get()) {
      LOG.warn("Close unexpectedly called without partition assignment");
      return;
    }

    // Empty partitions → task was stopped explicitly. Stop coordinator if running
    if (closedPartitions.isEmpty()) {
      LOG.info("Task stopped. Closing coordinator.");
      stopCoordinator();
      return;
    }

    // With Raft, we step down if we lose leader status, not based on partitions
    // Reset offsets to last committed to avoid data loss
    LOG.info(
        "Seeking to last committed offsets for worker {}-{}.",
        config.connectorName(),
        config.taskId());
    KafkaUtils.seekToLastCommittedOffsets(context);
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      startWorker();
      worker.save(sinkRecords);
    }
    processControlEvents();
  }

  private void processControlEvents() {
    if (coordinatorThread != null && coordinatorThread.isTerminated()) {
      throw new NotRunningException(
          String.format(
              "Coordinator unexpectedly terminated on committer %s-%s",
              config.connectorName(), config.taskId()));
    }

    // Process Raft consensus and check if we should be coordinator
    if (worker != null) {
      worker.process();

      // Check Raft leader status
      if (isLeader()) {
        startCoordinator();
      } else if (coordinatorThread != null && !raftState.isLeader()) {
        LOG.info("Task {} is no longer Raft leader, stopping coordinator", config.taskId());
        stopCoordinator();
      }
    }
  }

  private void startWorker() {
    if (null == this.worker) {
      LOG.info("Starting Raft commit worker {}-{}", config.connectorName(), config.taskId());
      SinkWriter sinkWriter = new SinkWriter(catalog, config);
      worker = new RaftWorker(config, clientFactory, sinkWriter, context, this);
      worker.start();
    }
  }

  private void startCoordinator() {
    if (null == this.coordinatorThread) {
      LOG.info(
          "Task {}-{} elected Raft leader, starting commit coordinator",
          config.connectorName(),
          config.taskId());
      RaftCoordinator coordinator =
          new RaftCoordinator(
              catalog, config, clientFactory, context, raftState);
      coordinatorThread = new ChannelThread(coordinator);
      coordinatorThread.start();
    }
  }

  private void stopWorker() {
    if (worker != null) {
      worker.stop();
      worker = null;
    }
  }

  private void stopCoordinator() {
    if (coordinatorThread != null) {
      coordinatorThread.terminate();
      coordinatorThread = null;
    }
  }

  @VisibleForTesting
  RaftState getRaftState() {
    return raftState;
  }
}
