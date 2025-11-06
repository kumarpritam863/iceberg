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
 *   <li>Fast fail over (5-10 seconds)
 *   <li>Zero coordinator churn during re-balancing
 *   <li>No external dependencies (no ZooKeeper/etcd)
 *   <li>Background election thread independent of data flow
 * </ul>
 *
 * <p>Architecture:
 * <ul>
 *   <li>RaftElectionThread: Background thread handling elections and heartbeats
 *   <li>RaftWorker: Handles data writes and consensus messages from control topic
 *   <li>RaftCoordinator: Leader performs commits
 * </ul>
 */
public class RaftCommitterImpl implements Committer, RaftElectionThread.RaftElectionCallback {

  private static final Logger LOG = LoggerFactory.getLogger(RaftCommitterImpl.class);

  private ChannelThread coordinatorThread;
  private RaftWorker worker;
  private RaftElectionThread electionThread;
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

      // Initialize Raft consensus state with configuration
      this.raftState =
          new RaftState(
              config.taskId(),
              config.raftElectionTimeoutMs(),
              config.raftElectionTimeoutJitterMs(),
              config.raftHeartbeatIntervalMs());

      LOG.info(
          "Initialized Raft consensus for task {} with election timeout {}ms-{}ms, heartbeat {}ms",
          config.taskId(),
          config.raftElectionTimeoutMs(),
          config.raftElectionTimeoutMs() + config.raftElectionTimeoutJitterMs(),
          config.raftHeartbeatIntervalMs());

      // Start background election thread
      this.electionThread = new RaftElectionThread(raftState, config, clientFactory, this);
      this.electionThread.start();
      LOG.info("Started background Raft election thread for task {}", config.taskId());
    }
  }

  // ========================================================================
  // RaftElectionCallback Implementation
  // ========================================================================

  @Override
  public void onBecameLeader() {
    LOG.info(
        "Task {}-{} became Raft leader, starting commit coordinator",
        config.connectorName(),
        config.taskId());
    startCoordinator();
  }

  @Override
  public void onLostLeadership() {
    LOG.info(
        "Task {}-{} lost Raft leadership, stopping commit coordinator",
        config.connectorName(),
        config.taskId());
    stopCoordinator();
  }

  @Override
  public void onSendHeartbeat() {
    // Broadcast heartbeat via worker
    if (worker != null) {
      worker.sendRaftHeartbeat(config.taskId(), raftState.getCurrentTerm());
    }
  }

  @Override
  public void onRequestVotes(String candidateId, long term) {
    // Broadcast RequestVote via worker
    if (worker != null) {
      worker.sendRaftRequestVote(candidateId, term);
    }
  }

  // ========================================================================
  // Raft Message Handlers (Called from RaftWorker)
  // ========================================================================

  /**
   * Handles incoming Raft RequestVote message.
   *
   * @param candidateId ID of candidate requesting vote
   * @param term Election term
   */
  @VisibleForTesting
  void handleRaftRequestVote(String candidateId, long term) {
    boolean voteGranted = raftState.handleVoteRequest(candidateId, term);
    LOG.debug(
        "Task {} {} vote for candidate {} in term {}",
        config.taskId(),
        voteGranted ? "granted" : "denied",
        candidateId,
        term);

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
    // Let RaftState handle vote aggregation
    // The electionThread will detect leadership transition and call onBecameLeader()
    raftState.handleVoteResponse(voterId, term, voteGranted);

    if (voteGranted) {
      LOG.debug(
          "Task {} received vote from {} for term {} ({}/{} votes)",
          config.taskId(),
          voterId,
          term,
          raftState.getVoteCount(),
          raftState.quorum());
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
    LOG.debug("Task {} received heartbeat from leader {} for term {}", config.taskId(), leaderId, term);

    // The electionThread will detect leadership loss and call onLostLeadership()
  }

  // ========================================================================
  // Committer Interface Implementation
  // ========================================================================

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
    // With Raft, coordinator election is done via background thread, not partition assignment
    // Worker will be started on first save()
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
      stopElectionThread();
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

  private void stopElectionThread() {
    // Stop background election thread first
    if (electionThread != null) {
      LOG.info("Stopping Raft election thread for task {}", config.taskId());
      electionThread.stop();
      electionThread = null;
    }
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      startWorker();
      worker.save(sinkRecords);
    }

    // Process control topic events (Raft messages, commit messages)
    // Note: Leader election happens in background thread, not here
    processControlEvents();
  }

  private void processControlEvents() {
    if (coordinatorThread != null && coordinatorThread.isTerminated()) {
      throw new NotRunningException(
          String.format(
              "Coordinator unexpectedly terminated on committer %s-%s",
              config.connectorName(), config.taskId()));
    }

    // Process messages from control topic (Raft consensus messages, commit messages)
    if (worker != null) {
      worker.process();
    }
  }

  // ========================================================================
  // Internal Thread Management
  // ========================================================================

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
          "Task {}-{} starting Raft coordinator",
          config.connectorName(),
          config.taskId());
      RaftCoordinator coordinator =
          new RaftCoordinator(catalog, config, clientFactory, context);
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
}
