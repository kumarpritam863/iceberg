/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.connect.channel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class CrossRegionChannel extends BaseChannel {

    private static final Logger LOG = LoggerFactory.getLogger(CrossRegionChannel.class);

    private final String controlTopic;
    private final String offsetStorageTopic;
    private final Producer<byte[], byte[]> producer;
    private final Consumer<byte[], byte[]> consumer;
    private final String producerId;
    private final ConnectorOffsetBackingStore offsetStore;
    private final CloseableOffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;
    private final Converter keyConverter, valueConverter;
    private final KafkaClientFactory clientFactory;
    private final TopicAdmin admin;



    CrossRegionChannel(String name, String consumerGroupId, IcebergSinkConfig config, KafkaClientFactory clientFactory, SinkTaskContext context) {
        super(consumerGroupId, config, clientFactory);
        this.controlTopic = config.controlTopic();
        this.offsetStorageTopic = config.offsetStorageTopic();
        this.clientFactory = new KafkaClientFactory(config.kafkaProps());
        String transactionalId = config.transactionalPrefix() + name + config.transactionalSuffix();
        this.producerId = UUID.randomUUID().toString();
        this.producer = clientFactory.crossRegionProducer(transactionalId);
        this.consumer = clientFactory.crossRegionConsumer("cross_region_control_consumer" + UUID.randomUUID());
        this.admin = clientFactory.topicAdmin();
        this.keyConverter = new JsonConverter();
        this.keyConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);
        this.valueConverter = new JsonConverter();
        this.valueConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), false);
        KafkaOffsetBackingStore connectorStore = KafkaOffsetBackingStore.readWriteStore(offsetStorageTopic, producer, consumer, admin, keyConverter);
        this.offsetStore = ConnectorOffsetBackingStore.withOnlyConnectorStore(
                () -> LoggingContext.forConnector(config.connectorName()),
                connectorStore,
                offsetStorageTopic,
                admin
        );
        this.offsetReader = new OffsetStorageReaderImpl(offsetStore, config.connectorName(), keyConverter, valueConverter);
        this.offsetWriter = new OffsetStorageWriter(offsetStore, config.connectorName(), keyConverter, valueConverter);
    }

    @Override
    protected void send(List<Event> events, Map<TopicPartition, Offset> sourceOffsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();

        List<ProducerRecord<byte[], byte[]>> recordList =
                events.stream()
                        .map(
                                event -> {
                                    LOG.info("Sending event of type: {}", event.type().name());
                                    byte[] data = AvroUtil.encode(event);
                                    // key by producer ID to keep event order
                                    return new ProducerRecord<>(controlTopic, producerId.getBytes(), data);
                                })
                        .collect(Collectors.toList());

        synchronized (producer) {
            producer.beginTransaction();
            try {
                // NOTE: we shouldn't call get() on the future in a transactional context,
                // see docs for org.apache.kafka.clients.producer.KafkaProducer
                recordList.forEach(producer::send);
                if (!sourceOffsets.isEmpty()) {
                    boolean shouldFlush = false;
                    try {
                        shouldFlush = offsetWriter.beginFlush();
                    } catch (Exception ex) {
                        LOG.error("Already open transaction. Failing to ensure exactly once", ex);
                        throw ex;
                    }
                    if (shouldFlush) {
                        offsetWriter.doFlush((error, result) -> {
                            if (error != null) {
                                LOG.error("Failed to flush offsets to storage: ", error);
                                offsetWriter.cancelFlush();
                                throw new RuntimeException("failed to flush offsets");
                            } else {
                                LOG.trace("Finished flushing offsets to storage");
                            }
                        });
                    }
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
    public void recordOffset(Map<String, ?> partition, Map<String, ?> offset) {
        offsetWriter.offset(partition, offset);
    }

    @Override
    public void start() {
        offsetStore.start();
    }

    @Override
    public void stop() {
        Utils.closeQuietly(offsetReader, "offset reader");
        Utils.closeQuietly(offsetStore::stop, "offset backing store");
    }
}
