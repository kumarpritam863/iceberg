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

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CrossRegionWorker extends AbstractChannel {

  private static final Logger LOG = LoggerFactory.getLogger(CrossRegionWorker.class);

  private final String controlTopic;
  private final Producer<byte[], byte[]> producer;
  private final String producerId;
  private final IcebergOffsetBackingStore offsetStore;
  private final CloseableOffsetStorageReader offsetReader;
  private final OffsetStorageWriter offsetWriter;
  private final SinkTaskContext context;
  private final TopicAdmin admin;
  private final SinkWriter sinkWriter;
  private final IcebergSinkConfig config;

  CrossRegionWorker(
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkWriter sinkWriter,
      SinkTaskContext context) {
    super(
        ChannelType.CROSS_REGION_WORKER,
        config.controlGroupIdPrefix() + UUID.randomUUID(),
        config,
        clientFactory,
        context);
    this.controlTopic = config.controlTopic();
    String transactionalId =
        config.transactionalPrefix() + "cross-region-worker" + config.transactionalSuffix();
    this.producerId = UUID.randomUUID().toString();
    this.producer = clientFactory.createProducer(transactionalId);
    Consumer<byte[], byte[]> consumer =
        clientFactory.createConsumer(
            "offset_committer" + UUID.randomUUID(),
            Map.of(
                ConsumerConfig.CLIENT_ID_CONFIG,
                "cross-region-consumer_" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"));
    admin = clientFactory.topicAdmin();
    Converter keyConverter = new JsonConverter();
    keyConverter.configure(
        Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);
    Converter valueConverter = new JsonConverter();
    valueConverter.configure(
        Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), false);
    offsetStore =
        KafkaOffsetBackingStore.readWriteStore(
            config.offsetStorageTopic(), producer, consumer, admin, keyConverter);
    offsetStore.configure(Map.of());
    this.offsetReader =
        new OffsetStorageReaderImpl(
            offsetStore, config.connectorName(), keyConverter, valueConverter);
    this.offsetWriter =
        new OffsetStorageWriter(offsetStore, config.connectorName(), keyConverter, valueConverter);
    this.context = context;
    this.sinkWriter = sinkWriter;
    this.config = config;
  }

  @Override
  boolean receive(Envelope envelope) {
    Event event = envelope.event();
    if (event.payload().type() != PayloadType.START_COMMIT) {
      return false;
    }

    SinkWriterResult results = sinkWriter.completeWrite();

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

    send(events);

    return true;
  }

  @Override
  void send(List<Event> events) {
    List<ProducerRecord<byte[], byte[]>> recordList =
        events.stream()
            .map(
                event -> {
                  LOG.info("Sending event of type: {}", event.type().name());
                  byte[] data = AvroUtil.encode(event);
                  // key by producer ID to keep event order
                  return new ProducerRecord<>(
                      controlTopic, producerId.getBytes(StandardCharsets.UTF_8), data);
                })
            .collect(Collectors.toList());

    synchronized (producer) {
      producer.beginTransaction();
      try {
        // NOTE: we shouldn't call get() on the future in a transactional context,
        // see docs for org.apache.kafka.clients.producer.KafkaProducer
        recordList.forEach(producer::send);
        if (offsetWriter.beginFlush()) {
          offsetWriter.doFlush(
              (error, result) -> {
                if (error != null) {
                  LOG.error("Failed to flush offsets to storage: ", error);
                  offsetWriter.cancelFlush();
                  throw new ConnectException("failed to flush offsets");
                } else {
                  LOG.trace("Finished flushing offsets to storage");
                }
              });
        } else {
          throw new ConnectException("Offset flush in progress");
        }
        producer.commitTransaction();
      } catch (Exception e) {
        try {
          producer.abortTransaction();
        } catch (Exception ex) {
          LOG.warn("Error aborting producer transaction", ex);
        }
        throw e;
      }
    }
  }

  @Override
  void seekToLastCommittedOffsets(Collection<TopicPartition> topicPartitions) {
    Map<TopicPartition, Long> offsetsToBeCommitted =
        topicPartitions.stream()
            .collect(
                Collectors.toMap(
                    tp -> tp,
                    tp -> {
                      Map<String, Object> storedOffset =
                          offsetReader.offset(Collections.singletonMap(tp.topic(), tp.partition()));
                      if (null != storedOffset) {
                        Long offset = (Long) storedOffset.get(tp.topic());
                        return null != offset ? offset : -1L;
                      } else {
                        return -1L;
                      }
                    }));
    KafkaUtils.seekToLastCommittedOffsets(context, offsetsToBeCommitted);
  }

  @Override
  void recordOffset(Map<String, ?> partition, Map<String, ?> offset) {
    offsetWriter.offset(partition, offset);
  }

  @Override
  void start() {
    super.start();
    offsetStore.start();
  }

  @Override
  void save(Collection<SinkRecord> sinkRecords) {
    sinkRecords.forEach(
        sinkRecord -> {
          sinkWriter.save(sinkRecord);
          recordOffset(
              Collections.singletonMap(sinkRecord.topic(), sinkRecord.kafkaPartition()),
              Collections.singletonMap(sinkRecord.topic(), sinkRecord.kafkaPartition()));
        });
  }

  @Override
  void stop() {
    super.stop();
    sinkWriter.close();
    Utils.closeQuietly(admin, "topic admin");
    Utils.closeQuietly(offsetReader, "offset reader");
    Utils.closeQuietly(offsetStore::stop, "offset backing store");
  }
}
