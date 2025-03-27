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
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract base class for Committers that manage coordination and worker lifecycles. */
abstract class AbstractCommitter implements Committer {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractCommitter.class);

  private CoordinatorThread coordinatorThread;
  private AbstractChannel worker;
  private Catalog catalog;

  public Catalog getCatalog() {
    return catalog;
  }

  public IcebergSinkConfig getConfig() {
    return config;
  }

  public SinkTaskContext getContext() {
    return context;
  }

  public KafkaClientFactory getClientFactory() {
    return clientFactory;
  }

  private IcebergSinkConfig config;
  private SinkTaskContext context;
  private KafkaClientFactory clientFactory;
  private Collection<MemberDescription> membersWhenWorkerIsCoordinator;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  protected abstract AbstractChannel createWorker();

  protected Coordinator createCoordinator(Collection<MemberDescription> members) {
    return new Coordinator(catalog, config, members, clientFactory, context);
  }

  private void initialize(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    if (isInitialized.compareAndSet(false, true)) {
      this.catalog = icebergCatalog;
      this.config = icebergSinkConfig;
      this.context = sinkTaskContext;
      this.clientFactory = new KafkaClientFactory(config.kafkaProps(), config.sourceKafkaAdminProps());
    }
  }

  @Override
  public void start(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    throw new UnsupportedOperationException(
        "start(Catalog, IcebergSinkConfig, SinkTaskContext) is deprecated and will be removed in 2.0.0.");
  }

  @Override
  public void open(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext,
      Collection<TopicPartition> addedPartitions) {
    initialize(icebergCatalog, icebergSinkConfig, sinkTaskContext);
    if (hasLeaderPartition(addedPartitions)) {
      LOG.info("This task is elected leader. Starting Coordinator.");
      startCoordinator();
    }
  }

  @Override
  public void stop() {
    throw new UnsupportedOperationException("stop() is deprecated and will be removed in 2.0.0.");
  }

  @Override
  public void close(Collection<TopicPartition> closedPartitions) {
    if (hasLeaderPartition(closedPartitions)) {
      LOG.info("This task lost the leader partition. Stopping Coordinator.");
      stopCoordinator();
    }
    worker.seekToLastCommittedOffsets(context.assignment());
    stopWorker();
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      startWorkerIfNeeded();
      worker.save(sinkRecords);
    }
    processControlEvents();
  }

  private void processControlEvents() {
    if (coordinatorThread != null && coordinatorThread.isTerminated()) {
      throw new NotRunningException("Coordinator unexpectedly terminated");
    }
    if (worker != null) {
      worker.process();
    }
  }

  private void startWorkerIfNeeded() {
    if (worker == null) {
      LOG.info("Starting worker...");
      this.worker = createWorker();
      worker.start();
    }
  }

  private void startCoordinator() {
    if (coordinatorThread == null) {
      LOG.info("Starting coordinator thread...");
      Coordinator coordinator = createCoordinator(membersWhenWorkerIsCoordinator);
      this.coordinatorThread = new CoordinatorThread(coordinator);
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

  private boolean hasLeaderPartition(Collection<TopicPartition> currentAssignedPartitions) {
    try (Admin admin = clientFactory.createAdmin()) {
      ConsumerGroupDescription groupDesc =
          KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
      if (groupDesc.state() == ConsumerGroupState.STABLE
          && containsFirstPartition(groupDesc.members(), currentAssignedPartitions)) {
        this.membersWhenWorkerIsCoordinator = groupDesc.members();
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  boolean containsFirstPartition(
      Collection<MemberDescription> members, Collection<TopicPartition> partitions) {
    TopicPartition firstPartition =
        members.stream()
            .flatMap(member -> member.assignment().topicPartitions().stream())
            .min(TopicPartitionComparator.INSTANCE)
            .orElseThrow(
                () -> new ConnectException("No partitions assigned, cannot determine leader"));

    return partitions.contains(firstPartition);
  }

  /** Comparator for Kafka TopicPartition objects. */
  static class TopicPartitionComparator implements Comparator<TopicPartition> {
    static final TopicPartitionComparator INSTANCE = new TopicPartitionComparator();

    @Override
    public int compare(TopicPartition tp1, TopicPartition tp2) {
      int topicCompare = tp1.topic().compareTo(tp2.topic());
      return topicCompare != 0 ? topicCompare : Integer.compare(tp1.partition(), tp2.partition());
    }
  }

  void configure(Map<String, String> committerConfig) {
    // Implement to configure
  }
}
