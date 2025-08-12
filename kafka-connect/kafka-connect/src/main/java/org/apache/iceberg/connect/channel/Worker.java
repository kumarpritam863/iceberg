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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.IcebergWriterResult;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

class Worker extends Channel {

  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final Map<TopicPartition, SinkWriter> sinkWriters;
  private final Catalog catalog;

  Worker(
      Catalog catalog,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkTaskContext context) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "worker",
        config.controlGroupIdPrefix() + UUID.randomUUID(),
        config,
        clientFactory,
        context);

    this.catalog = catalog;
    this.config = config;
    this.context = context;
    this.sinkWriters = Maps.newHashMap();
  }

  void process() {
    consumeAvailable(Duration.ZERO);
  }

  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    if (event.payload().type() != PayloadType.START_COMMIT) {
      return false;
    }

    WorkerWriteResult results = completeWrite();

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                tp -> {
                  Offset offset = results.sourceOffsets().get(tp);
                  if (offset == null) {
                    offset = Offset.NULL_OFFSET;
                  }
                  return new TopicPartitionOffset(
                      tp.topic(), tp.partition(), offset.offset(), offset.timestamp());
                })
            .collect(Collectors.toList());

    UUID commitId = ((StartCommit) event.payload()).commitId();

    List<Event> events =
        results.writerResults().stream()
            .map(
                writeResult ->
                    new Event(
                        config.connectGroupId(),
                        new DataWritten(
                            writeResult.partitionStruct(),
                            commitId,
                            TableReference.of(config.catalogName(), writeResult.tableIdentifier()),
                            writeResult.dataFiles(),
                            writeResult.deleteFiles())))
            .collect(Collectors.toList());

    Event readyEvent = new Event(config.connectGroupId(), new DataComplete(commitId, assignments));
    events.add(readyEvent);

    send(events, results.sourceOffsets());

    return true;
  }

  private WorkerWriteResult completeWrite() {
    List<IcebergWriterResult> writerResults = Lists.newArrayList();
    Map<TopicPartition, Offset> sourceOffsets = Maps.newHashMap();
    for (Map.Entry<TopicPartition, SinkWriter> entry : sinkWriters.entrySet()) {
      SinkWriterResult result = entry.getValue().completeWrite();
      writerResults.addAll(result.writerResults());
      sourceOffsets.put(entry.getKey(), result.sourceOffset());
    }
    return new WorkerWriteResult(writerResults, sourceOffsets);
  }

  @Override
  void stop() {
    super.stop();
    sinkWriters.values().forEach(SinkWriter::close);
  }

  void save(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord sinkRecord : sinkRecords) {
      SinkWriter writer =
          sinkWriters.computeIfAbsent(
              new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
              tp -> new SinkWriter(catalog, config));
      writer.save(sinkRecord);
    }
  }

  static class WorkerWriteResult {
    private final List<IcebergWriterResult> writerResults;

    public WorkerWriteResult(
        List<IcebergWriterResult> writerResults, Map<TopicPartition, Offset> sourceOffsets) {
      this.writerResults = writerResults;
      this.sourceOffsets = sourceOffsets;
    }

    public List<IcebergWriterResult> writerResults() {
      return writerResults;
    }

    public Map<TopicPartition, Offset> sourceOffsets() {
      return sourceOffsets;
    }

    private final Map<TopicPartition, Offset> sourceOffsets;
  }

  public void close(TopicPartition topicPartition) {
    SinkWriter sinkWriter = sinkWriters.remove(topicPartition);
    if (null != sinkWriter) {
      sinkWriter.close();
    }
  }
}
