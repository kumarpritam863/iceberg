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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.util.TopicAdmin;

class KafkaClientFactory {
  private final Map<String, String> kafkaProps;

  KafkaClientFactory(Map<String, String> kafkaProps) {
    this.kafkaProps = kafkaProps;
  }

  Producer<String, byte[]> createProducer(String transactionalId) {
    Map<String, Object> producerProps = Maps.newHashMap(kafkaProps);
    producerProps.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    KafkaProducer<String, byte[]> result =
        new KafkaProducer<>(producerProps, new StringSerializer(), new ByteArraySerializer());
    result.initTransactions();
    return result;
  }

  Consumer<String, byte[]> createConsumer(String consumerGroupId) {
    Map<String, Object> consumerProps = Maps.newHashMap(kafkaProps);
    consumerProps.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    return new KafkaConsumer<>(
        consumerProps, new StringDeserializer(), new ByteArrayDeserializer());
  }

  Admin createAdmin() {
    Map<String, Object> adminProps = Maps.newHashMap(kafkaProps);
    return Admin.create(adminProps);
  }

  Producer<byte[], byte[]> crossRegionProducer(String transactionalId) {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "cross-region_" + UUID.randomUUID());
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    producerProps.putAll(kafkaProps);
    KafkaProducer<byte[], byte[]> result =
            new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
    result.initTransactions();
    return result;
  }

  Consumer<byte[], byte[]> crossRegionConsumer(String consumerGroupId) {
    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "cross-region-consumer_" + consumerGroupId);
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "cross-region-consumer_" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString());
    consumerProps.putAll(kafkaProps);
    return new KafkaConsumer<>(
            consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  TopicAdmin topicAdmin() {
    return new TopicAdmin(Maps.newHashMap(kafkaProps));
  }
}
