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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
 * Standalone coordinator service that decouples from Kafka Connect task partition assignment.
 * This coordinator acts as a JobManager in the Flink architecture, managing multiple jobs
 * and their commit cycles independently of partition ownership.
 */
public class StandaloneCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneCoordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VALID_THROUGH_TS_SNAPSHOT_PROP = "kafka.connect.valid-through-ts";
  private static final Duration POLL_DURATION = Duration.ofMillis(1000);

  private final JobManager jobManager;
  private final Map<String, Catalog> catalogs;
  private final Map<String, JobCommitState> jobCommitStates;
  private final ExecutorService commitExecutor;
  private final Producer<byte[], byte[]> producer;
  private final Consumer<byte[], byte[]> consumer;
  private final CoordinatorConfig config;
  private volatile boolean running;

  public StandaloneCoordinator(CoordinatorConfig config, JobManager jobManager) {
    this.config = config;
    this.jobManager = jobManager;
    this.catalogs = new ConcurrentHashMap<>();
    this.jobCommitStates = new ConcurrentHashMap<>();
    this.commitExecutor = new ThreadPoolExecutor(
        config.commitThreads(),
        config.commitThreads(),
        config.keepAliveTimeoutMs(),
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("iceberg-coordinator-commit-%d")
            .build());

    this.producer = createProducer();
    this.consumer = createConsumer();
  }

  public synchronized void start() throws IOException {
    if (running) {
      LOG.warn("Coordinator is already running");
      return;
    }

    LOG.info("Starting standalone coordinator");

    List<JobConfig> jobs = jobManager.listJobsByStatus(JobConfig.JobStatus.RUNNING);
    LOG.info("Found {} running jobs to manage", jobs.size());

    for (JobConfig job : jobs) {
      initializeJobState(job);
    }

    consumer.subscribe(getControlTopics());
    running = true;
    LOG.info("Standalone coordinator started successfully");
  }

  public synchronized void stop() {
    if (!running) {
      return;
    }

    LOG.info("Stopping standalone coordinator");
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

    for (Catalog catalog : catalogs.values()) {
      try {
        if (catalog instanceof AutoCloseable) {
          ((AutoCloseable) catalog).close();
        }
      } catch (Exception e) {
        LOG.warn("Error closing catalog", e);
      }
    }

    LOG.info("Standalone coordinator stopped");
  }

  public void processEvents() {
    if (!running) {
      return;
    }

    processCommitIntervals();

    ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_DURATION);
    for (ConsumerRecord<byte[], byte[]> record : records) {
      try {
        Event event = Event.decode(record.value());
        handleEvent(event);
      } catch (Exception e) {
        LOG.error("Error processing event from topic {} partition {} offset {}",
            record.topic(), record.partition(), record.offset(), e);
      }
    }

    processCommitTimeouts();
  }

  public void registerJob(JobConfig jobConfig) throws IOException {
    LOG.info("Registering job with coordinator: {}", jobConfig.jobId());

    if (running) {
      initializeJobState(jobConfig);
    }

    jobManager.updateJobStatus(jobConfig.jobId(), JobConfig.JobStatus.RUNNING);
    LOG.info("Job {} registered and marked as running", jobConfig.jobId());
  }

  public void unregisterJob(String jobId) throws IOException {
    LOG.info("Unregistering job from coordinator: {}", jobId);

    jobCommitStates.remove(jobId);

    Optional<JobConfig> job = jobManager.getJob(jobId);
    if (job.isPresent()) {
      jobManager.updateJobStatus(jobId, JobConfig.JobStatus.PAUSED);
    }

    LOG.info("Job {} unregistered", jobId);
  }

  private void initializeJobState(JobConfig jobConfig) {
    try {
      Catalog catalog = getOrCreateCatalog(jobConfig);
      JobCommitState commitState = new JobCommitState(jobConfig, config.commitInterval());
      jobCommitStates.put(jobConfig.jobId(), commitState);
      LOG.info("Initialized state for job: {}", jobConfig.jobId());
    } catch (Exception e) {
      LOG.error("Failed to initialize job state for {}", jobConfig.jobId(), e);
    }
  }

  private Catalog getOrCreateCatalog(JobConfig jobConfig) {
    return catalogs.computeIfAbsent(jobConfig.catalogName(), name -> {
      try {
        return CatalogLoader.load(jobConfig.catalogName(), jobConfig.catalogProperties());
      } catch (Exception e) {
        throw new RuntimeException("Failed to load catalog: " + name, e);
      }
    });
  }

  private Collection<String> getControlTopics() throws IOException {
    return jobManager.listJobs().stream()
        .map(JobConfig::controlTopic)
        .distinct()
        .collect(Collectors.toList());
  }

  private void processCommitIntervals() {
    for (Map.Entry<String, JobCommitState> entry : jobCommitStates.entrySet()) {
      String jobId = entry.getKey();
      JobCommitState commitState = entry.getValue();

      if (commitState.isCommitIntervalReached()) {
        try {
          startCommit(jobId, commitState);
        } catch (Exception e) {
          LOG.error("Failed to start commit for job {}", jobId, e);
        }
      }
    }
  }

  private void processCommitTimeouts() {
    for (Map.Entry<String, JobCommitState> entry : jobCommitStates.entrySet()) {
      String jobId = entry.getKey();
      JobCommitState commitState = entry.getValue();

      if (commitState.isCommitTimedOut()) {
        try {
          commitToTables(jobId, commitState, true);
        } catch (Exception e) {
          LOG.error("Failed to process commit timeout for job {}", jobId, e);
        }
      }
    }
  }

  private void startCommit(String jobId, JobCommitState commitState) {
    commitState.startNewCommit();
    Event event = new Event(
        commitState.getJobConfig().connectGroupId(),
        new StartCommit(commitState.getCurrentCommitId()));

    sendEvent(commitState.getJobConfig().controlTopic(), event);
    LOG.info("Started commit {} for job {}", commitState.getCurrentCommitId(), jobId);
  }

  private void handleEvent(Event event) {
    String connectGroupId = event.connectGroupId();
    JobCommitState commitState = findCommitStateByGroupId(connectGroupId);

    if (commitState == null) {
      LOG.debug("No active job found for connect group: {}", connectGroupId);
      return;
    }

    switch (event.payload().type()) {
      case DATA_WRITTEN:
        commitState.addDataWritten(event);
        break;
      case DATA_COMPLETE:
        commitState.addDataComplete(event);
        if (commitState.isCommitReady()) {
          try {
            commitToTables(findJobIdByGroupId(connectGroupId), commitState, false);
          } catch (Exception e) {
            LOG.error("Failed to commit tables for group {}", connectGroupId, e);
          }
        }
        break;
      default:
        LOG.debug("Ignoring event type: {}", event.payload().type());
    }
  }

  private JobCommitState findCommitStateByGroupId(String connectGroupId) {
    return jobCommitStates.values().stream()
        .filter(state -> state.getJobConfig().connectGroupId().equals(connectGroupId))
        .findFirst()
        .orElse(null);
  }

  private String findJobIdByGroupId(String connectGroupId) {
    return jobCommitStates.entrySet().stream()
        .filter(entry -> entry.getValue().getJobConfig().connectGroupId().equals(connectGroupId))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElse(null);
  }

  private void commitToTables(String jobId, JobCommitState commitState, boolean partialCommit) {
    JobConfig jobConfig = commitState.getJobConfig();
    Catalog catalog = getOrCreateCatalog(jobConfig);

    Map<TableReference, List<DataWritten>> commitMap = commitState.getTableCommitMap();
    String offsetsJson = commitState.getOffsetsJson();
    OffsetDateTime validThroughTs = commitState.getValidThroughTimestamp(partialCommit);

    Tasks.foreach(commitMap.entrySet())
        .executeWith(commitExecutor)
        .stopOnFailure()
        .run(entry -> {
          commitToTable(catalog, entry.getKey(), entry.getValue(),
              offsetsJson, validThroughTs, jobConfig);
        });

    Event event = new Event(
        jobConfig.connectGroupId(),
        new CommitComplete(commitState.getCurrentCommitId(), validThroughTs));
    sendEvent(jobConfig.controlTopic(), event);

    commitState.clearCommit();
    LOG.info("Commit {} complete for job {}, committed to {} table(s)",
        commitState.getCurrentCommitId(), jobId, commitMap.size());
  }

  private void commitToTable(
      Catalog catalog,
      TableReference tableReference,
      List<DataWritten> dataWrittenList,
      String offsetsJson,
      OffsetDateTime validThroughTs,
      JobConfig jobConfig) {

    TableIdentifier tableIdentifier = tableReference.identifier();
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
      return;
    }

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

    String snapshotOffsetsProp = String.format(
        "kafka.connect.offsets.%s.%s", jobConfig.controlTopic(), jobConfig.connectGroupId());

    if (deleteFiles.isEmpty()) {
      AppendFiles appendOp = table.newAppend();
      appendOp.set(snapshotOffsetsProp, offsetsJson);
      appendOp.set(COMMIT_ID_SNAPSHOT_PROP, String.valueOf(System.currentTimeMillis()));
      if (validThroughTs != null) {
        appendOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
      }
      dataFiles.forEach(appendOp::appendFile);
      appendOp.commit();
    } else {
      RowDelta deltaOp = table.newRowDelta();
      deltaOp.set(snapshotOffsetsProp, offsetsJson);
      deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, String.valueOf(System.currentTimeMillis()));
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
        new CommitToTable(System.currentTimeMillis(), tableReference, snapshotId, validThroughTs));
    sendEvent(jobConfig.controlTopic(), event);

    LOG.info("Committed to table {}, snapshot {}", tableIdentifier, snapshotId);
  }

  private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = Maps.newConcurrentMap();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }

  private void sendEvent(String topic, Event event) {
    try {
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, event.encode());
      producer.send(record);
    } catch (Exception e) {
      LOG.error("Failed to send event to topic {}", topic, e);
    }
  }

  private Producer<byte[], byte[]> createProducer() {
    Map<String, Object> props = Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers(),
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
        ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE,
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
    );

    return new KafkaProducer<>(props);
  }

  private Consumer<byte[], byte[]> createConsumer() {
    Map<String, Object> props = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG, "iceberg-coordinator-" + System.currentTimeMillis(),
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true
    );

    return new KafkaConsumer<>(props);
  }
}