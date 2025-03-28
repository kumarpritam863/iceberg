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
import org.apache.iceberg.connect.KafkaOffsetBackingStore;
import org.apache.iceberg.connect.OffsetStorageReaderImpl;
import org.apache.iceberg.connect.OffsetStorageWriter;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class CrossRegionChannel extends AbstractChannel {

  private static final Logger LOG = LoggerFactory.getLogger(CrossRegionChannel.class);

  private final String controlTopic;
  private final Producer<byte[], byte[]> producer;
  private final String producerId;
  private final KafkaOffsetBackingStore offsetStore;
  private final CloseableOffsetStorageReader offsetReader;
  private final OffsetStorageWriter offsetWriter;
  private final SinkTaskContext context;

  CrossRegionChannel(
      String name,
      String consumerGroupId,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkTaskContext context) {
    super(consumerGroupId, config, clientFactory);
    this.controlTopic = config.controlTopic();
    String transactionalId = config.transactionalPrefix() + name + config.transactionalSuffix();
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
    TopicAdmin admin = clientFactory.topicAdmin();
    Converter keyConverter = new JsonConverter();
    keyConverter.configure(
        Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);
    Converter valueConverter = new JsonConverter();
    valueConverter.configure(
        Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), false);
    offsetStore =
        new KafkaOffsetBackingStore(
            keyConverter, config.offsetStorageTopic(), producer, consumer, admin);
    this.offsetReader =
        new OffsetStorageReaderImpl(
            offsetStore, config.connectorName(), keyConverter, valueConverter);
    this.offsetWriter =
        new OffsetStorageWriter(offsetStore, config.connectorName(), keyConverter, valueConverter);
    this.context = context;
  }

  @Override
  protected void send(List<Event> events) {
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
                  throw new RuntimeException("failed to flush offsets");
                } else {
                  LOG.trace("Finished flushing offsets to storage");
                }
              });
        } else {
          throw new RuntimeException("Offset flush in progress");
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
  public void seekToLastCommittedOffsets(Collection<TopicPartition> topicPartitions) {
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
                        if (null != offset) {
                          return offset;
                        } else {
                          return -1L;
                        }
                      } else {
                        return -1L;
                      }
                    }));
    KafkaUtils.seekToLastCommittedOffsets(context, offsetsToBeCommitted);
  }

  @Override
  public void recordOffset(Map<String, ?> partition, Map<String, ?> offset) {
    offsetWriter.offset(partition, offset);
  }

  @Override
  public void start() {
    super.start();
    offsetStore.start();
  }

  @Override
  void stop() {
    super.stop();
    Utils.closeQuietly(offsetReader, "offset reader");
    Utils.closeQuietly(offsetStore::stop, "offset backing store");
  }
}
