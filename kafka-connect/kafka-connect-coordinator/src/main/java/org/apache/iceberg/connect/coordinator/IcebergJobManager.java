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
package org.apache.iceberg.connect.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-job coordinator that acts as a JobManager for a single Iceberg Kafka Connect job.
 * Similar to Flink's JobManager, this coordinator is dedicated to managing one specific job
 * and its commit cycles, completely decoupled from Kafka Connect task partition assignments.
 */
public class IcebergJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergJobManager.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VALID_THROUGH_TS_SNAPSHOT_PROP = "kafka.connect.valid-through-ts";
  private static final Duration POLL_DURATION = Duration.ofMillis(1000);

  private final IcebergJobConfig jobConfig;
  private final Catalog catalog;
  private final String snapshotOffsetsProp;
  private final ExecutorService commitExecutor;
  private final Producer<byte[], byte[]> producer;
  private final Consumer<byte[], byte[]> consumer;
  private final JobCommitState commitState;
  private final boolean transactionsEnabled;
  private volatile boolean running;
  private volatile boolean inTransaction;

  public IcebergJobManager(IcebergJobConfig jobConfig) {
    this.jobConfig = jobConfig;
    this.catalog = CatalogLoader.load(jobConfig.catalogName(), jobConfig.catalogProperties());
    this.transactionsEnabled = jobConfig.coordinatorConfig().enableTransactions();
    this.snapshotOffsetsProp = String.format(
        "kafka.connect.offsets.%s.%s", jobConfig.controlTopic(), jobConfig.connectGroupId());

    this.commitExecutor = new ThreadPoolExecutor(
        jobConfig.coordinatorConfig().commitThreads(),
        jobConfig.coordinatorConfig().commitThreads(),
        jobConfig.coordinatorConfig().keepAliveTimeoutMs(),
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("iceberg-job-manager-commit-%d")
            .build());

    this.producer = createTransactionalProducer();
    this.consumer = createTransactionalConsumer();
    this.commitState = new JobCommitState(
        jobConfig.connectGroupId(),
        jobConfig.coordinatorConfig().commitInterval());
    this.inTransaction = false;
  }

  public synchronized void start() {
    if (running) {
      LOG.warn("Job manager for {} is already running", jobConfig.jobId());
      return;
    }

    LOG.info("Starting Iceberg Job Manager for job: {}", jobConfig.jobId());
    LOG.info("  Connect Group ID: {}", jobConfig.connectGroupId());
    LOG.info("  Control Topic: {}", jobConfig.controlTopic());
    LOG.info("  Catalog: {}", jobConfig.catalogName());
    LOG.info("  Transactions Enabled: {}", transactionsEnabled);

    if (transactionsEnabled) {
      try {
        producer.initTransactions();
        LOG.info("Transactional producer initialized for job: {}", jobConfig.jobId());
      } catch (Exception e) {
        LOG.error("Failed to initialize transactional producer for job: {}", jobConfig.jobId(), e);
        throw new RuntimeException("Failed to initialize transactional producer", e);
      }
    }

    consumer.subscribe(java.util.Collections.singletonList(jobConfig.controlTopic()));
    running = true;
    LOG.info("Job Manager started successfully for job: {}", jobConfig.jobId());
  }

  public synchronized void stop() {
    if (!running) {
      return;
    }

    LOG.info("Stopping Iceberg Job Manager for job: {}", jobConfig.jobId());
    running = false;

    try {
      commitExecutor.shutdown();
      if (!commitExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        commitExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      commitExecutor.shutdownNow();
    }

    producer.close();
    consumer.close();

    try {
      if (catalog instanceof AutoCloseable) {
        ((AutoCloseable) catalog).close();
      }
    } catch (Exception e) {
      LOG.warn("Error closing catalog", e);
    }

    LOG.info("Job Manager stopped for job: {}", jobConfig.jobId());
  }

  public void processEvents() {
    if (!running) {
      return;
    }

    processCommitInterval();

    if (transactionsEnabled) {
      processEventsTransactionally();
    } else {
      processEventsNonTransactionally();
    }

    processCommitTimeout();
  }

  private void processEventsTransactionally() {
    ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_DURATION);

    if (records.isEmpty()) {
      return;
    }

    try {
      beginTransactionIfNeeded();

      boolean hasRelevantEvents = false;
      for (ConsumerRecord<byte[], byte[]> record : records) {
        try {
          Event event = Event.decode(record.value());
          if (shouldProcessEvent(event)) {
            handleEvent(event);
            hasRelevantEvents = true;
          }
        } catch (Exception e) {
          LOG.error("Error processing event from topic {} partition {} offset {}",
              record.topic(), record.partition(), record.offset(), e);
          abortTransaction();
          return;
        }
      }

      if (hasRelevantEvents && inTransaction) {
        commitTransaction();
      } else if (inTransaction) {
        abortTransaction();
      }

      // Commit consumer offsets after successful transaction
      consumer.commitSync();

    } catch (Exception e) {
      LOG.error("Error in transactional event processing for job {}", jobConfig.jobId(), e);
      abortTransaction();
    }
  }

  private void processEventsNonTransactionally() {
    ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_DURATION);
    for (ConsumerRecord<byte[], byte[]> record : records) {
      try {
        Event event = Event.decode(record.value());
        if (shouldProcessEvent(event)) {
          handleEvent(event);
        }
      } catch (Exception e) {
        LOG.error("Error processing event from topic {} partition {} offset {}",
            record.topic(), record.partition(), record.offset(), e);
      }
    }
  }

  private void beginTransactionIfNeeded() {
    if (transactionsEnabled && !inTransaction) {
      try {
        producer.beginTransaction();
        inTransaction = true;
        LOG.debug("Started transaction for job: {}", jobConfig.jobId());
      } catch (Exception e) {
        LOG.error("Failed to begin transaction for job: {}", jobConfig.jobId(), e);
        throw new RuntimeException("Failed to begin transaction", e);
      }
    }
  }

  private void commitTransaction() {
    if (transactionsEnabled && inTransaction) {
      try {
        producer.commitTransaction();
        inTransaction = false;
        LOG.debug("Committed transaction for job: {}", jobConfig.jobId());
      } catch (Exception e) {
        LOG.error("Failed to commit transaction for job: {}", jobConfig.jobId(), e);
        abortTransaction();
        throw new RuntimeException("Failed to commit transaction", e);
      }
    }
  }

  private void abortTransaction() {
    if (transactionsEnabled && inTransaction) {
      try {
        producer.abortTransaction();
        inTransaction = false;
        LOG.warn("Aborted transaction for job: {}", jobConfig.jobId());
      } catch (Exception e) {
        LOG.error("Failed to abort transaction for job: {}", jobConfig.jobId(), e);
      }
    }
  }

  private boolean shouldProcessEvent(Event event) {
    return jobConfig.connectGroupId().equals(event.connectGroupId());
  }

  private void processCommitInterval() {
    if (commitState.isCommitIntervalReached()) {
      startCommit();
    }
  }

  private void processCommitTimeout() {
    if (commitState.isCommitTimedOut()) {
      try {
        performCommit(true);
      } catch (Exception e) {
        LOG.warn("Commit timeout failed for job {}, will try again next cycle", jobConfig.jobId(), e);
      } finally {
        commitState.clearCommit();
      }
    }
  }

  private void startCommit() {
    commitState.startNewCommit();
    Event event = new Event(jobConfig.connectGroupId(), new StartCommit(commitState.getCurrentCommitId()));
    sendEvent(event);
    LOG.info("Started commit {} for job {}", commitState.getCurrentCommitId(), jobConfig.jobId());
  }

  private void handleEvent(Event event) {
    switch (event.payload().type()) {
      case DATA_WRITTEN:
        commitState.addDataWritten(event);
        break;
      case DATA_COMPLETE:
        commitState.addDataComplete(event);
        if (commitState.isCommitReady()) {
          try {
            performCommit(false);
          } catch (Exception e) {
            LOG.warn("Commit failed for job {}, will try again next cycle", jobConfig.jobId(), e);
          } finally {
            commitState.clearCommit();
          }
        }
        break;
      default:
        LOG.debug("Ignoring event type: {} for job {}", event.payload().type(), jobConfig.jobId());
    }
  }

  private void performCommit(boolean partialCommit) {
    Map<TableReference, List<DataWritten>> commitMap = commitState.getTableCommitMap();
    String offsetsJson = commitState.getOffsetsJson();
    OffsetDateTime validThroughTs = commitState.getValidThroughTimestamp(partialCommit);

    commitToTables(commitMap, offsetsJson, validThroughTs);
  }

  private void commitToTables(Map<TableReference, List<DataWritten>> commitMap,
                              String offsetsJson, OffsetDateTime validThroughTs) {
    if (transactionsEnabled) {
      commitToTablesTransactionally(commitMap, offsetsJson, validThroughTs);
    } else {
      commitToTablesNonTransactionally(commitMap, offsetsJson, validThroughTs);
    }
  }

  private void commitToTablesTransactionally(Map<TableReference, List<DataWritten>> commitMap,
                                           String offsetsJson, OffsetDateTime validThroughTs) {
    try {
      beginTransactionIfNeeded();

      // Commit to Iceberg tables first
      Tasks.foreach(commitMap.entrySet())
          .executeWith(commitExecutor)
          .stopOnFailure()
          .run(entry -> {
            commitToTable(entry.getKey(), entry.getValue(), offsetsJson, validThroughTs);
          });

      // Send commit complete event within the same transaction
      Event commitCompleteEvent = new Event(
          jobConfig.connectGroupId(),
          new CommitComplete(commitState.getCurrentCommitId(), validThroughTs));
      sendEventInTransaction(commitCompleteEvent);

      // Commit the transaction (includes the event sending)
      commitTransaction();

      LOG.info("Transactional commit {} complete for job {}, committed to {} table(s), valid-through {}",
          commitState.getCurrentCommitId(), jobConfig.jobId(), commitMap.size(), validThroughTs);

    } catch (Exception e) {
      LOG.error("Transactional commit failed for job {}, aborting transaction", jobConfig.jobId(), e);
      abortTransaction();
      throw new RuntimeException("Transactional commit failed", e);
    }
  }

  private void commitToTablesNonTransactionally(Map<TableReference, List<DataWritten>> commitMap,
                                              String offsetsJson, OffsetDateTime validThroughTs) {
    Tasks.foreach(commitMap.entrySet())
        .executeWith(commitExecutor)
        .stopOnFailure()
        .run(entry -> {
          commitToTable(entry.getKey(), entry.getValue(), offsetsJson, validThroughTs);
        });

    Event event = new Event(
        jobConfig.connectGroupId(),
        new CommitComplete(commitState.getCurrentCommitId(), validThroughTs));
    sendEvent(event);

    LOG.info("Commit {} complete for job {}, committed to {} table(s), valid-through {}",
        commitState.getCurrentCommitId(), jobConfig.jobId(), commitMap.size(), validThroughTs);
  }

  private void commitToTable(
      TableReference tableReference,
      List<DataWritten> dataWrittenList,
      String offsetsJson,
      OffsetDateTime validThroughTs) {

    TableIdentifier tableIdentifier = tableReference.identifier();
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
      return;
    }

    Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table);

    List<DataFile> dataFiles = dataWrittenList.stream()
        .filter(payload -> payload.dataFiles() != null)
        .flatMap(payload -> payload.dataFiles().stream())
        .filter(dataFile -> dataFile.recordCount() > 0)
        .filter(distinctByKey(ContentFile::location))
        .collect(Collectors.toList());

    List<DeleteFile> deleteFiles = dataWrittenList.stream()
        .filter(payload -> payload.deleteFiles() != null)
        .flatMap(payload -> payload.deleteFiles().stream())
        .filter(deleteFile -> deleteFile.recordCount() > 0)
        .filter(distinctByKey(ContentFile::location))
        .collect(Collectors.toList());

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
      return;
    }

    if (deleteFiles.isEmpty()) {
      AppendFiles appendOp = table.newAppend();
      appendOp.set(snapshotOffsetsProp, offsetsJson);
      appendOp.set(COMMIT_ID_SNAPSHOT_PROP, String.valueOf(commitState.getCurrentCommitId()));
      if (validThroughTs != null) {
        appendOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
      }
      dataFiles.forEach(appendOp::appendFile);
      appendOp.commit();
    } else {
      RowDelta deltaOp = table.newRowDelta();
      deltaOp.set(snapshotOffsetsProp, offsetsJson);
      deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, String.valueOf(commitState.getCurrentCommitId()));
      if (validThroughTs != null) {
        deltaOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
      }
      dataFiles.forEach(deltaOp::addRows);
      deleteFiles.forEach(deltaOp::addDeletes);
      deltaOp.commit();
    }

    Long snapshotId = table.currentSnapshot().snapshotId();
    Event event = new Event(
        jobConfig.connectGroupId(),
        new CommitToTable(commitState.getCurrentCommitId(), tableReference, snapshotId, validThroughTs));
    sendEvent(event);

    LOG.info("Committed to table {}, snapshot {}, commit ID {}, valid-through {} for job {}",
        tableIdentifier, snapshotId, commitState.getCurrentCommitId(), validThroughTs, jobConfig.jobId());
  }

  private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = Maps.newConcurrentMap();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }

  private Map<Integer, Long> lastCommittedOffsetsForTable(Table table) {
    Snapshot snapshot = table.currentSnapshot();
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(snapshotOffsetsProp);
      if (value != null) {
        TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
        try {
          return MAPPER.readValue(value, typeRef);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return ImmutableMap.of();
  }

  private void sendEvent(Event event) {
    try {
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(jobConfig.controlTopic(), event.encode());
      producer.send(record);
    } catch (Exception e) {
      LOG.error("Failed to send event to topic {} for job {}", jobConfig.controlTopic(), jobConfig.jobId(), e);
    }
  }

  private void sendEventInTransaction(Event event) {
    if (!transactionsEnabled) {
      sendEvent(event);
      return;
    }

    try {
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(jobConfig.controlTopic(), event.encode());
      producer.send(record);
      LOG.debug("Sent event in transaction for job: {}", jobConfig.jobId());
    } catch (Exception e) {
      LOG.error("Failed to send event in transaction to topic {} for job {}", jobConfig.controlTopic(), jobConfig.jobId(), e);
      throw e;
    }
  }

  private Producer<byte[], byte[]> createTransactionalProducer() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, jobConfig.coordinatorConfig().kafkaBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "iceberg-job-manager-" + jobConfig.jobId());

    if (transactionsEnabled) {
      String transactionalId = jobConfig.coordinatorConfig().transactionalId();
      if (transactionalId == null) {
        transactionalId = "iceberg-coordinator-" + jobConfig.jobId();
      }

      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
      props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
      props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
          (int) jobConfig.coordinatorConfig().transactionTimeout().toMillis());
    } else {
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    }

    return new KafkaProducer<>(props);
  }

  private Consumer<byte[], byte[]> createTransactionalConsumer() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, jobConfig.coordinatorConfig().kafkaBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "iceberg-job-manager-" + jobConfig.jobId());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "iceberg-job-manager-consumer-" + jobConfig.jobId());

    if (transactionsEnabled) {
      props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    } else {
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    }

    return new KafkaConsumer<>(props);
  }

  public boolean isRunning() {
    return running;
  }

  public String getJobId() {
    return jobConfig.jobId();
  }
}