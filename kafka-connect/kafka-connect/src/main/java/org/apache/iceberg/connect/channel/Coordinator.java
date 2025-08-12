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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
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
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Coordinator extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Duration POLL_DURATION = Duration.ofSeconds(1);

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final List<Envelope> commitBuffer = Lists.newArrayList();

  Coordinator(Catalog catalog, IcebergSinkConfig config, KafkaClientFactory clientFactory) {
    // pass consumer group ID to which we commit low watermark offsets
    super(config.connectGroupId() + "-coord", config, clientFactory);

    this.catalog = catalog;
    this.config = config;
    this.snapshotOffsetsProp =
        String.format(
            "kafka.connect.offsets.%s.%s", config.controlTopic(), config.connectGroupId());
    this.exec =
        new ThreadPoolExecutor(
            config.commitThreads(),
            config.commitThreads(),
            config.keepAliveTimeoutInMs(),
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-committer" + "-%d")
                .build());
  }

  void process() {
    LOG.info(
            "Coordinator on task = {}-{}, polling for file metadata",
            config.connectorName(),
            config.taskId());
    consumeAvailable(POLL_DURATION);
  }

  @Override
  protected void receive(Envelope envelope) {
    commitBuffer.add(envelope);
    if (commitBuffer.size() >= config.coordinatorCommitBufferThreshold()) {
      LOG.info("Either commit timeout or commit size reached for coordinator {}-{}. Starting commit.", config.connectorName(), config.taskId());
      commit();
    }
  }

  @Override
  public void commit() {
    try {
      doCommit();
    } catch (Exception e) {
      LOG.warn("Commit failed for coordinator {}-{}, will try again next cycle", config.connectorName(), config.taskId(), e);
    }
  }

  private void doCommit() {
    Map<TableReference, List<Envelope>> commitMap = commitBuffer.stream()
            .collect(
                    Collectors.groupingBy(
                            envelope -> ((DataWritten) envelope.event().payload()).tableReference()));

    String offsetsJson = offsetsJson();

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(entry -> commitToTable(entry.getKey(), entry.getValue(), offsetsJson));

    // we should only get here if all tables committed successfully...
    LOG.info("Coordinator {}-{} successfully committed to following tables = {}", config.connectorName(), config.taskId(), commitMap.keySet()
            .stream()
            .map(tableReference -> tableReference.identifier().namespace() + "." + tableReference.identifier().name())
            .collect(Collectors.toList()));
    commitConsumerOffsets();
    commitBuffer.clear();
    LOG.info("Coordinator {}-{} committed it's control offsets.", config.connectorName(), config.taskId());
  }

  private String offsetsJson() {
    try {
      return MAPPER.writeValueAsString(controlTopicOffsets());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void commitToTable(
      TableReference tableReference, List<Envelope> envelopeList, String offsetsJson) {
    TableIdentifier tableIdentifier = tableReference.identifier();
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found by coordinator = {}-{}, skipping commit: {}", config.connectorName(), config.taskId(), tableIdentifier, e);
      return;
    }

    String branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table, branch);

    List<DataWritten> payloads =
        envelopeList.stream()
            .filter(
                envelope -> {
                  Long minOffset = committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .map(envelope -> (DataWritten) envelope.event().payload())
            .collect(Collectors.toList());

    List<DataFile> dataFiles =
        payloads.stream()
            .filter(payload -> payload.dataFiles() != null)
            .flatMap(payload -> payload.dataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

    List<DeleteFile> deleteFiles =
        payloads.stream()
            .filter(payload -> payload.deleteFiles() != null)
            .flatMap(payload -> payload.deleteFiles().stream())
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Coordinator {}-{} found nothing to commit to table {}, skipping", config.connectorName(), config.taskId(), tableIdentifier);
    } else {
      if (deleteFiles.isEmpty()) {
        AppendFiles appendOp = table.newAppend();
        if (branch != null) {
          appendOp.toBranch(branch);
        }
        appendOp.set(snapshotOffsetsProp, offsetsJson);
        dataFiles.forEach(appendOp::appendFile);
        appendOp.commit();
      } else {
        RowDelta deltaOp = table.newRowDelta();
        if (branch != null) {
          deltaOp.toBranch(branch);
        }
        deltaOp.set(snapshotOffsetsProp, offsetsJson);
        dataFiles.forEach(deltaOp::addRows);
        deleteFiles.forEach(deltaOp::addDeletes);
        deltaOp.commit();
      }
      LOG.info("Coordinator {}-{} committed successfully.", config.connectorName(), config.taskId());
    }
  }

  private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = Maps.newConcurrentMap();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  private Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(snapshotOffsetsProp);
      if (value != null) {
        TypeReference<Map<Integer, Long>> typeRef = new TypeReference<>() {};
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

  void terminate() {
    exec.shutdownNow();
    // wait for coordinator termination, else cause the sink task to fail
    try {
      if (!exec.awaitTermination(1, TimeUnit.MINUTES)) {
        throw new ConnectException("Timed out waiting for coordinator shutdown");
      }
    } catch (InterruptedException e) {
      throw new ConnectException("Interrupted while waiting for coordinator shutdown", e);
    }
  }

  @Override
  public void stop() {
    terminate();
    super.stop();
  }
}
