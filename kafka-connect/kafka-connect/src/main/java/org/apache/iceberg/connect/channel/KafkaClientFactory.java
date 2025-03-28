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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.util.TopicAdmin;

class KafkaClientFactory {
  private final Map<String, String> kafkaProps;
  private final Map<String, String> sourceKafkaAdminProps;

  KafkaClientFactory(Map<String, String> kafkaProps, Map<String, String> sourceKafkaAdminProps) {
    this.kafkaProps = kafkaProps;
    this.sourceKafkaAdminProps = sourceKafkaAdminProps;
  }

  Producer<byte[], byte[]> createProducer(String transactionalId) {
    Map<String, Object> producerProps = Maps.newHashMap();
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    producerProps.put(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "cross-region_" + UUID.randomUUID());
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    producerProps.putAll(kafkaProps);
    KafkaProducer<byte[], byte[]> result =
        new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
    result.initTransactions();
    return result;
  }

  Consumer<byte[], byte[]> createConsumer(
      String consumerGroupId, Map<String, Object> consumerPropOverrides) {
    Map<String, Object> consumerProps = Maps.newHashMap(kafkaProps);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    consumerProps.putAll(consumerPropOverrides);
    return new KafkaConsumer<>(
        consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  Admin createAdmin() {
    Map<String, Object> adminProps = Maps.newHashMap(sourceKafkaAdminProps);
    return Admin.create(adminProps);
  }

  TopicAdmin topicAdmin() {
    return new TopicAdmin(Maps.newHashMap(kafkaProps));
  }
}
