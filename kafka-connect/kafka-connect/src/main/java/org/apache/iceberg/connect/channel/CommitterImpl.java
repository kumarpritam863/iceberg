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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterImpl implements Committer {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);

  private CoordinatorThread coordinatorThread;
  private Worker worker;
  private Catalog catalog;
  private IcebergSinkConfig config;
  private SinkTaskContext context;
  private KafkaClientFactory clientFactory;
  private String taskId;

  // Partition 0 of the lowest subscribed source topic; whoever owns it runs the coordinator.
  private TopicPartition leaderTopicPartition;

  // The sink task's own consumer, resolved once via reflection and reused as the
  // WorkerSinkTaskContext consumer is created once per task and stable for its lifetime.
  private Consumer<byte[], byte[]> consumer;

  // Per-connector active-coordinator gauge, shared across this connector's tasks in the JVM.
  private CoordinatorMetrics coordinatorMetrics;

  @VisibleForTesting
  static Optional<TopicPartition> leaderPartition(Set<String> subscription) {
    return subscription.stream()
        .min(Comparator.naturalOrder())
        .map(topic -> new TopicPartition(topic, 0));
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
      Collection<TopicPartition> addedPartitions) {}

  @Override
  public void stop() {
    throw new UnsupportedOperationException(
        "The method stop() is deprecated and will be removed in 2.0.0. "
            + "Use stop(Collection<TopicPartition>) instead.");
  }

  @Override
  public void close(Collection<TopicPartition> closedPartitions) {
    // Always try to stop the worker to avoid duplicates.
    if (worker != null) {
      worker.stop();
      worker = null;
    }

    // Empty partitions → the task is being stopped entirely. There will be no further save() to
    // reconcile the coordinator away, so tear it down now (this is the only unconditional stop).
    if (closedPartitions.isEmpty()) {
      if (null != coordinatorThread) {
        LOG.info("Committer {} stopped. Closing coordinator.", taskId);
        stopCoordinator();
      }
      // Task is stopping for good: release its metrics reference so a deleted connector's MBean is
      // unregistered once its last task in this JVM has gone.
      if (coordinatorMetrics != null) {
        coordinatorMetrics.detach();
        coordinatorMetrics = null;
      }
      return;
    }

    // Partial revoke while the task keeps running: if this removed the leader partition, the next
    // save() reconcile stops the coordinator. Just reset offsets to last committed to avoid dupes.
    LOG.info("Seeking to last committed offsets for worker {}.", taskId);
    try {
      KafkaUtils.seekToLastCommittedOffsets(consumer());
    } catch (RuntimeException e) {
      // Best effort: a resolution failure here only risks reprocessing (dedup/OCC keep it safe),
      // so never let it break the revoke path.
      LOG.warn("Committer {} could not seek to last committed offsets", taskId, e);
    }
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      if (null == this.worker) {
        LOG.info("Starting commit worker {}", taskId);
        SinkWriter sinkWriter = new SinkWriter(catalog, config);
        worker = new Worker(config, clientFactory, sinkWriter, context);
        worker.start();
      }
      worker.save(sinkRecords);
    }
    reconcileCoordinator();
    processControlEvents();
  }

  @Override
  public void configure(
      Catalog icebergCatalog,
      IcebergSinkConfig icebergSinkConfig,
      SinkTaskContext sinkTaskContext) {
    this.catalog = icebergCatalog;
    this.config = icebergSinkConfig;
    this.context = sinkTaskContext;
    this.clientFactory = new KafkaClientFactory(config.kafkaProps());
    this.taskId = config.connectorName() + "-" + config.taskId();
    this.coordinatorMetrics = CoordinatorMetrics.attach(config.connectorName());
  }

  /**
   * Level-triggered leadership reconciliation on the Connect task thread. Reads the election key
   * and the source-partition count from the task's own consumer (no Admin call), and starts/stops
   * the coordinator so that exactly the owner of {@code leaderTopicPartition} runs it.
   */
  private void reconcileCoordinator() {
    Set<String> subscription;
    try {
      subscription = consumer().subscription();
    } catch (RuntimeException e) {
      // Degrade rather than crash the task: retry on the next cycle. A persistent failure surfaces
      // as coordinator absence (control-topic lag / no commits), which is diagnosable, instead of a
      // per-save crash loop.
      LOG.warn(
          "Committer {} could not read source consumer subscription for leader election, "
              + "will retry on next cycle",
          taskId,
          e);
      return;
    }

    // Advance the election key only when the subscription is resolvable. Keeping the last-known key
    // on a transient empty subscription avoids flapping the coordinator; re-reading each cycle lets
    // the key converge (self-heal) if the lowest subscribed topic changes under a regex pattern.
    leaderPartition(subscription).ifPresent(tp -> this.leaderTopicPartition = tp);
    if (leaderTopicPartition == null) {
      LOG.debug(
          "Committer {} cannot determine leader partition yet (subscription={})",
          taskId,
          subscription);
      return;
    }

    boolean leader = context.assignment().contains(leaderTopicPartition);
    if (leader && null == this.coordinatorThread) {
      startCoordinator(sourcePartitionCount(consumer, subscription));
    } else if (!leader && null != this.coordinatorThread) {
      LOG.info(
          "Committer {} no longer owns leader partition {}, stopping coordinator",
          taskId,
          leaderTopicPartition);
      stopCoordinator();
    }
  }

  private void startCoordinator(int topicPartitionCount) {
    LOG.info(
        "Task {} elected leader (owns {}), starting commit coordinator for {} source partition(s)",
        taskId,
        leaderTopicPartition,
        topicPartitionCount);
    Coordinator coordinator =
        new Coordinator(catalog, config, topicPartitionCount, clientFactory, context);
    coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();
    coordinatorMetrics.coordinatorStarted();
  }

  private void stopCoordinator() {
    if (coordinatorThread != null) {
      coordinatorThread.terminate();
      coordinatorThread = null;
      coordinatorMetrics.coordinatorStopped();
    }
  }

  /**
   * The sink task's own Kafka consumer, resolved lazily once and cached. Resolution goes through a
   * reflective field lookup ({@link KafkaUtils#kafkaConsumer}); the consumer instance is stable for
   * the task's lifetime, so caching keeps that lookup off the per-{@code save()} hot path. Not
   * cached on failure, so a transient resolution error simply retries on the next cycle.
   */
  private Consumer<byte[], byte[]> consumer() {
    if (consumer == null) {
      consumer = KafkaUtils.kafkaConsumer(context);
    }
    return consumer;
  }

  /**
   * Total source-partition count across the subscribed topics, from the consumer's cached cluster
   * metadata (no Admin call). Used as the coordinator's commit-readiness quorum.
   */
  private int sourcePartitionCount(Consumer<byte[], byte[]> consumer, Set<String> subscription) {
    int total = 0;
    for (String topic : subscription) {
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);
      if (partitions != null) {
        total += partitions.size();
      }
    }
    return total;
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
}
