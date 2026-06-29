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
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Committer} that elects the commit coordinator from ownership of a single-partition
 * election topic rather than from source topic-partition assignment.
 *
 * <p>Select it with {@code
 * iceberg.committer.class=org.apache.iceberg.connect.channel.ElectionTopicCommitter}. Every task
 * runs a {@code Worker}; in addition, every task joins a dedicated {@code <connect-group>-election}
 * consumer group subscribed to the election topic (config {@code
 * iceberg.coordinator.election-topic}, which defaults to the control topic). The single task that
 * owns the election topic's only partition runs the {@code Coordinator}. Because election no longer
 * depends on which source partitions a task holds, source rebalances and scaling do not move,
 * churn, or restart the coordinator, and the hot-path {@code describeConsumerGroups} call is
 * removed from the rebalance lifecycle.
 *
 * <p>The election topic <b>must have exactly one partition</b>; this is validated at startup.
 * Operators can reuse the control topic (the default) when it is single-partition, or point the
 * config at a dedicated single-partition topic otherwise.
 *
 * <p>Leadership transfer is handled by Kafka's consumer group coordinator (automatic failover when
 * the leading task dies). The leadership flag is published by a background election thread and
 * reconciled with the coordinator lifecycle on the single Kafka Connect task thread, so the
 * coordinator is only ever started or stopped from one thread.
 */
public class ElectionTopicCommitter implements Committer {

  private static final Logger LOG = LoggerFactory.getLogger(ElectionTopicCommitter.class);

  private CoordinatorThread coordinatorThread;
  private Worker worker;
  private Catalog catalog;
  private IcebergSinkConfig config;
  private SinkTaskContext context;
  private KafkaClientFactory clientFactory;
  private ElectionMembership election;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private String taskId;

  @Override
  public void configure(IcebergSinkConfig icebergSinkConfig) {
    this.config = icebergSinkConfig;
  }

  private void initialize(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    if (isInitialized.compareAndSet(false, true)) {
      this.catalog = icebergCatalog;
      this.config = icebergSinkConfig;
      this.context = sinkTaskContext;
      this.clientFactory = new KafkaClientFactory(config.kafkaProps());
      this.taskId = config.connectorName() + "-" + config.taskId();

      String electionTopic = config.coordinatorElectionTopic();
      validateElectionTopic(electionTopic);

      this.election =
          new ElectionMembership(
              clientFactory.createConsumer(config.connectGroupId() + "-election"),
              electionTopic,
              taskId);
      this.election.start();
      LOG.info("Committer {} initialized, electing coordinator on topic {}", taskId, electionTopic);
    }
  }

  @VisibleForTesting
  void validateElectionTopic(String electionTopic) {
    int partitionCount;
    try (Admin admin = clientFactory.createAdmin()) {
      partitionCount = KafkaUtils.describeTopicPartitionCount(electionTopic, admin);
    }
    if (partitionCount != 1) {
      throw new ConnectException(
          String.format(
              Locale.ROOT,
              "Election topic '%s' must have exactly 1 partition for single-coordinator election, "
                  + "but found %d. Set iceberg.coordinator.election-topic to a single-partition topic "
                  + "(the control topic may be reused if it has a single partition).",
              electionTopic,
              partitionCount));
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
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException(
        "The method stop() is deprecated and will be removed in 2.0.0. "
            + "Use close(Collection<TopicPartition>) instead.");
  }

  @Override
  public void close(Collection<TopicPartition> closedPartitions) {
    // Always stop the worker to avoid duplicates; it is recreated on the next save().
    stopWorker();

    // Defensive: close called without prior initialization (should not happen).
    if (!isInitialized.get()) {
      LOG.warn("Close unexpectedly called on committer {} without initialization", taskId);
      return;
    }

    // Empty partitions → task is being stopped entirely. Tear down coordinator + election.
    if (closedPartitions.isEmpty()) {
      LOG.info("Committer {} stopped. Shutting down coordinator and election.", taskId);
      stopCoordinator();
      stopElection();
      return;
    }

    // Source partitions revoked but task continues. Leadership is independent of source partitions,
    // so the coordinator and election are left running; just reset offsets to avoid duplicates.
    LOG.info("Seeking to last committed offsets for committer {}.", taskId);
    KafkaUtils.seekToLastCommittedOffsets(context);
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      startWorker();
      worker.save(sinkRecords);
    }
    reconcileCoordinator();
    processControlEvents();
  }

  private void reconcileCoordinator() {
    if (election == null) {
      return;
    }
    boolean leader = election.isLeader();
    if (leader && coordinatorThread == null) {
      startCoordinator();
    } else if (!leader && coordinatorThread != null) {
      LOG.info("Committer {} is no longer the leader, stopping coordinator", taskId);
      stopCoordinator();
    }
  }

  private void processControlEvents() {
    if (coordinatorThread != null && coordinatorThread.isTerminated()) {
      throw new NotRunningException(
          String.format("Coordinator unexpectedly terminated on committer %s", taskId));
    }
    if (worker != null) {
      worker.process();
    }
  }

  private void startWorker() {
    if (null == this.worker) {
      LOG.info("Starting commit worker {}", taskId);
      SinkWriter sinkWriter = new SinkWriter(catalog, config);
      worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
    }
  }

  private void startCoordinator() {
    LOG.info("Committer {} elected leader, starting commit coordinator", taskId);
    Collection<MemberDescription> members;
    try (Admin admin = clientFactory.createAdmin()) {
      members = KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin).members();
    }
    Coordinator coordinator = new Coordinator(catalog, config, members, clientFactory, context);
    coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();
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

  private void stopElection() {
    if (election != null) {
      election.close();
      election = null;
    }
  }
}
