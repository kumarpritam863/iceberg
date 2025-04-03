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
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractChannel {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractChannel.class);

  private final String controlTopic;
  private final String connectGroupId;

  private final Consumer<byte[], byte[]> consumer;
  private final Producer<byte[], byte[]> producer;
  private final Map<Integer, Long> controlTopicOffsets = Maps.newHashMap();
  private final SinkTaskContext context;

  AbstractChannel(
      ChannelType channelType,
      String consumerGroupId,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkTaskContext context) {
    this.controlTopic = config.controlTopic();
    this.connectGroupId = config.connectGroupId();
    this.consumer =
        clientFactory.createConsumer(
            consumerGroupId,
            Map.of(
                ConsumerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest"));
    this.producer =
        channelType.equals(ChannelType.WORKER) || channelType.equals(ChannelType.COORDINATOR)
            ? clientFactory.createProducer(
                config.transactionalPrefix()
                    + channelType.name().toLowerCase(Locale.ROOT)
                    + config.transactionalSuffix())
            : null;
    this.context = context;
  }

  void send(Event event) {
    send(ImmutableList.of(event), ImmutableMap.of());
  }

  void send(List<Event> events, Map<TopicPartition, Offset> sourceOffsets) {
    if (null == producer) {
      throw new ConnectException("Attempting to write with only readable channel");
    }
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
    sourceOffsets.forEach((k, v) -> offsetsToCommit.put(k, new OffsetAndMetadata(v.offset())));

    List<ProducerRecord<byte[], byte[]>> recordList =
        events.stream()
            .map(
                event -> {
                  LOG.info("Sending event of type: {}", event.type().name());
                  byte[] data = AvroUtil.encode(event);
                  // key by producer ID to keep event order
                  return new ProducerRecord<>(
                      controlTopic,
                      UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8),
                      data);
                })
            .collect(Collectors.toList());

    synchronized (producer) {
      producer.beginTransaction();
      try {
        // NOTE: we shouldn't call get() on the future in a transactional context,
        // see docs for org.apache.kafka.clients.producer.KafkaProducer
        recordList.forEach(producer::send);
        if (!sourceOffsets.isEmpty()) {
          producer.sendOffsetsToTransaction(
              offsetsToCommit, KafkaUtils.consumerGroupMetadata(context));
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

  abstract boolean receive(Envelope envelope);

  void consumeAvailable(Duration pollDuration) {
    ConsumerRecords<byte[], byte[]> records = consumer.poll(pollDuration);
    while (!records.isEmpty()) {
      records.forEach(
          record -> {
            // the consumer stores the offsets that corresponds to the next record to consume,
            // so increment the record offset by one
            controlTopicOffsets.put(record.partition(), record.offset() + 1);

            Event event = AvroUtil.decode(record.value());

            if (event.groupId().equals(connectGroupId)) {
              LOG.debug("Received event of type: {}", event.type().name());
              if (receive(new Envelope(event, record.partition(), record.offset()))) {
                LOG.info("Handled event of type: {}", event.type().name());
              }
            }
          });
      records = consumer.poll(pollDuration);
    }
  }

  Map<Integer, Long> controlTopicOffsets() {
    return controlTopicOffsets;
  }

  void commitConsumerOffsets() {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
    controlTopicOffsets()
        .forEach(
            (k, v) ->
                offsetsToCommit.put(new TopicPartition(controlTopic, k), new OffsetAndMetadata(v)));
    consumer.commitSync(offsetsToCommit);
  }

  void start() {
    consumer.subscribe(ImmutableList.of(controlTopic));

    // initial poll with longer duration so the consumer will initialize...
    consumeAvailable(Duration.ofSeconds(1));
  }

  void recordOffset(Map<String, ?> partition, Map<String, ?> offset) {}

  void seekToLastCommittedOffsets(Collection<TopicPartition> topicPartitions) {}

  void stop() {
    LOG.info("Channel stopping");
    if (null != producer) {
      producer.close();
    }
    consumer.close();
  }

  /** Save incoming records from Kafka Connect. */
  void save(Collection<SinkRecord> records) {}

  /** Process any pending control events or tasks. */
  void process() {
    consumeAvailable(Duration.ZERO);
  }
}
