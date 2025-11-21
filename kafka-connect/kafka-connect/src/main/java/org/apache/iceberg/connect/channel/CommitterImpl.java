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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterImpl implements Committer {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);

  private CoordinatorThread coordinatorThread;
  private Worker worker;
  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final KafkaClientFactory clientFactory;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private RaftCoordinatorElector raftElector;

    public CommitterImpl(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {
    this.catalog = catalog;
    this.config = config;
    this.context = context;
    this.clientFactory = new KafkaClientFactory(config.kafkaProps());
    LOG.info("[RAFT] Initializing Raft consensus for task {}", config.taskId());

    // Create dedicated transport for election messages
        RaftElectionTransport raftTransport = new KafkaRaftElectionTransport(
                config.electionTopic(),
                config.taskId(),
                config.kafkaProps().get("bootstrap.servers"),
                config.connectGroupId()
        );

    // Create Raft elector with dedicated transport
    raftElector =
            new RaftCoordinatorElector(config.taskId(), config.connectGroupId(), raftTransport);

    // Set listener for coordinator role changes
    raftElector.setChangeListener(
            new RaftCoordinatorElector.CoordinatorChangeListener() {
              @Override
              public void onBecameCoordinator(long term) {
                LOG.warn(
                        "[RAFT] Task {}-{} BECAME COORDINATOR via Raft election (term={})",
                        config.connectorName(),
                        config.taskId(),
                        term);
                startCoordinator();
              }

              @Override
              public void onLostCoordinator(long term) {
                LOG.warn(
                        "[RAFT] Task {}-{} LOST COORDINATOR role (term={})",
                        config.connectorName(),
                        config.taskId(),
                        term);
                stopCoordinator();
              }
            });

    // Discover all tasks in the cluster
    Set<String> allTasks = discoverAllTasks();
    LOG.info("[RAFT] Starting Raft with cluster: {}", allTasks);
    raftElector.start(allTasks);
  }

  @Override
  public void start(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    throw new UnsupportedOperationException(
        "The method start(Catalog, IcebergSinkConfig, SinkTaskContext) is deprecated and will be removed in 2.0.0. "
            + "Use start(Catalog, IcebergSinkConfig, SinkTaskContext, Collection<TopicPartition>) instead.");
  }

  @Override
  public void open(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext,
      Collection<TopicPartition> addedPartitions) {
    startWorker();

    // Raft will automatically elect coordinator via consensus
    // No need to check partitions - all tasks participate in election
    LOG.info(
        "[RAFT] Task {}-{} opened, Raft will handle coordinator election",
        config.connectorName(),
        config.taskId());
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException(
        "The method stop() is deprecated and will be removed in 2.0.0. "
            + "Use stop(Collection<TopicPartition>) instead.");
  }

  @Override
  public void close(Collection<TopicPartition> closedPartitions) {
    // Always try to stop the worker to avoid duplicates.
    stopWorker();

    // Defensive: close called without prior initialization (should not happen).
    if (!isInitialized.get()) {
      LOG.warn("Close unexpectedly called without partition assignment");
      return;
    }

    // Empty partitions → task was stopped explicitly. Stop Raft and coordinator.
    if (closedPartitions.isEmpty()) {
      LOG.info("[RAFT] Task stopped. Stopping Raft and coordinator.");
      if (raftElector != null) {
        raftElector.stop();
        raftElector = null;
      }
      stopCoordinator();
      return;
    }

    startWorker();
    // Normal close with partition changes: update Raft cluster membership
    if (raftElector != null) {
      Set<String> remainingTasks = discoverAllTasks();
      raftElector.updateNodes(remainingTasks);
      LOG.info("[RAFT] Updated cluster membership to: {}", remainingTasks);
    }

    // Reset offsets to last committed to avoid data loss.
    LOG.info(
        "Seeking to last committed offsets for worker {}-{}.",
        config.connectorName(),
        config.taskId());
    KafkaUtils.seekToLastCommittedOffsets(context);
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
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
    if (worker != null) {
      worker.process();
    }
  }

  private void startWorker() {
    if (null == this.worker) {
      LOG.info("Starting commit worker {}-{}", config.connectorName(), config.taskId());
      SinkWriter sinkWriter = new SinkWriter(catalog, config);
      worker = new Worker(config, clientFactory, sinkWriter, context);

      // Start worker (elections are handled independently by transport)
      worker.start();
    }
  }

  private Set<String> discoverAllTasks() {
    try (Admin admin = clientFactory.createAdmin()) {
      ConsumerGroupDescription groupDesc =
          KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);

      return groupDesc.members().stream()
          .map(
              member -> {
                // Extract task ID from client ID (format: connector-name-task-N)
                String clientId = member.clientId();
                int lastDash = clientId.lastIndexOf('-');
                if (lastDash > 0) {
                  return clientId.substring(lastDash + 1);
                }
                return clientId;
              })
          .collect(Collectors.toSet());
    } catch (Exception e) {
      LOG.warn("[RAFT] Failed to discover all tasks, using self only", e);
      return Collections.singleton(config.taskId());
    }
  }

  private void startCoordinator() {
    if (null == this.coordinatorThread) {
      LOG.info(
          "Task {}-{} elected leader, starting commit coordinator",
          config.connectorName(),
          config.taskId());

      // Fetch current consumer group members for coordinator initialization
      Collection<MemberDescription> members;
      try (Admin admin = clientFactory.createAdmin()) {
        ConsumerGroupDescription groupDesc =
            KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
        members = groupDesc.members();
      }

      Coordinator coordinator = new Coordinator(catalog, config, members, clientFactory, context);
      coordinatorThread = new CoordinatorThread(coordinator);
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
