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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka-based implementation of RaftElectionTransport.
 *
 * <p>Features:
 * <ul>
 *   <li>Dedicated election topic (separate from control topic)
 *   <li>Dedicated consumer/producer (isolated from main data flow)
 *   <li>Background thread for message consumption (non-blocking)
 *   <li>JSON serialization for election messages
 *   <li>Error handling with automatic retry
 *   <li>Graceful shutdown with timeout
 * </ul>
 */
public class KafkaRaftElectionTransport implements RaftElectionTransport {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaRaftElectionTransport.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Configuration
  private final String electionTopic;
  private final String nodeId;
  private final Properties consumerProps;
  private final Properties producerProps;

  // Kafka clients (dedicated for elections only)
  private KafkaProducer<String, String> producer;
  private KafkaConsumer<String, String> consumer;

  // Background processing
  private final ExecutorService consumerThread;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile RaftMessageListener messageListener;

  // Retry configuration
  private static final int MAX_SEND_RETRIES = 3;
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

  /**
   * Create a new Kafka-based Raft election transport.
   *
   * @param electionTopic Dedicated Kafka topic for election messages
   * @param nodeId This node's unique identifier
   * @param bootstrapServers Kafka bootstrap servers
   * @param groupId Consumer group ID for election messages
   */
  public KafkaRaftElectionTransport(
      String electionTopic,
      String nodeId,
      String bootstrapServers,
      String groupId) {
    this.electionTopic = electionTopic;
    this.nodeId = nodeId;

    // Configure dedicated consumer for election messages
    this.consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-raft-election");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

    // Configure dedicated producer for election messages
    this.producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(MAX_SEND_RETRIES));
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0"); // No batching for low latency

    // Create background thread for consuming election messages
    this.consumerThread = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat("raft-election-consumer-%d")
            .setDaemon(true)
            .build());
  }

  @Override
  public void start() {
    if (running.compareAndSet(false, true)) {
      LOG.info("[RAFT-TRANSPORT] [{}] Starting election transport (topic={})", nodeId, electionTopic);
      LOG.debug("[RAFT-TRANSPORT] [{}] Bootstrap servers: {}", nodeId, consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));

      // Create dedicated producer
      LOG.debug("[RAFT-TRANSPORT] [{}] Creating dedicated producer with idempotence enabled", nodeId);
      this.producer = new KafkaProducer<>(producerProps);

      // Create dedicated consumer
      LOG.debug("[RAFT-TRANSPORT] [{}] Creating dedicated consumer (group={}-raft-election)",
          nodeId, consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG));
      this.consumer = new KafkaConsumer<>(consumerProps);
      consumer.subscribe(Collections.singleton(electionTopic));
      LOG.debug("[RAFT-TRANSPORT] [{}] Subscribed to election topic: {}", nodeId, electionTopic);

      // Start background consumer thread
      LOG.debug("[RAFT-TRANSPORT] [{}] Starting background consumer thread", nodeId);
      consumerThread.execute(this::consumeLoop);

      LOG.info("[RAFT-TRANSPORT] [{}] Election transport started successfully", nodeId);
    } else {
      LOG.warn("[RAFT-TRANSPORT] [{}] Election transport already running, ignoring start()", nodeId);
    }
  }

  @Override
  public void stop() {
    if (running.compareAndSet(true, false)) {
      LOG.info("[RAFT-TRANSPORT] [{}] Stopping election transport", nodeId);

      // Wake up consumer to exit poll loop
      if (consumer != null) {
        LOG.debug("[RAFT-TRANSPORT] [{}] Waking up consumer to exit poll loop", nodeId);
        consumer.wakeup();
      }

      // Shutdown consumer thread
      LOG.debug("[RAFT-TRANSPORT] [{}] Shutting down consumer thread", nodeId);
      consumerThread.shutdown();
      try {
        if (!consumerThread.awaitTermination(5, TimeUnit.SECONDS)) {
          LOG.warn("[RAFT-TRANSPORT] [{}] Consumer thread did not terminate in time, forcing shutdown", nodeId);
          consumerThread.shutdownNow();
        } else {
          LOG.debug("[RAFT-TRANSPORT] [{}] Consumer thread terminated gracefully", nodeId);
        }
      } catch (InterruptedException e) {
        LOG.warn("[RAFT-TRANSPORT] [{}] Interrupted while waiting for consumer thread shutdown", nodeId, e);
        Thread.currentThread().interrupt();
        consumerThread.shutdownNow();
      }

      // Close Kafka clients
      if (producer != null) {
        try {
          LOG.debug("[RAFT-TRANSPORT] [{}] Closing producer", nodeId);
          producer.close(Duration.ofSeconds(5));
          LOG.debug("[RAFT-TRANSPORT] [{}] Producer closed successfully", nodeId);
        } catch (Exception e) {
          LOG.error("[RAFT-TRANSPORT] [{}] Error closing producer", nodeId, e);
        }
      }

      if (consumer != null) {
        try {
          LOG.debug("[RAFT-TRANSPORT] [{}] Closing consumer", nodeId);
          consumer.close(Duration.ofSeconds(5));
          LOG.debug("[RAFT-TRANSPORT] [{}] Consumer closed successfully", nodeId);
        } catch (Exception e) {
          LOG.error("[RAFT-TRANSPORT] [{}] Error closing consumer", nodeId, e);
        }
      }

      LOG.info("[RAFT-TRANSPORT] [{}] Election transport stopped", nodeId);
    } else {
      LOG.debug("[RAFT-TRANSPORT] [{}] Election transport already stopped, ignoring stop()", nodeId);
    }
  }

  @Override
  public void send(Payload payload, String groupId) {
    if (!running.get()) {
      LOG.warn("[RAFT-TRANSPORT] [{}] Cannot send {} - transport not running", nodeId, payload.type());
      return;
    }

    LOG.debug("[RAFT-TRANSPORT] [{}] Sending {} to group {}", nodeId, payload.type(), groupId);

    try {
      // Create election message envelope
      RaftElectionMessage message = RaftElectionMessage.create(nodeId, groupId, payload);
      String json = MAPPER.writeValueAsString(message);

      LOG.trace("[RAFT-TRANSPORT] [{}] Serialized {} message ({} bytes)",
          nodeId, payload.type(), json.length());

      // Send to election topic (async with callback)
      ProducerRecord<String, String> record = new ProducerRecord<>(
          electionTopic,
          groupId, // Key by group for partitioning
          json
      );

      @SuppressWarnings("FutureReturnValueIgnored")
      Future<RecordMetadata> unused = producer.send(record, (metadata, exception) -> {
        if (exception != null) {
          LOG.error(
              "[RAFT-TRANSPORT] [{}] Failed to send {} message to group {}",
              nodeId,
              payload.type(),
              groupId,
              exception);
        } else {
          LOG.trace(
              "[RAFT-TRANSPORT] [{}] Sent {} to partition {} offset {} (group={})",
              nodeId,
              payload.type(),
              metadata.partition(),
              metadata.offset(),
              groupId);
        }
      });

    } catch (Exception e) {
      LOG.error("[RAFT-TRANSPORT] [{}] Error serializing {} election message for group {}",
          nodeId, payload.type(), groupId, e);
    }
  }

  @Override
  public void setMessageListener(RaftMessageListener listener) {
    this.messageListener = listener;
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Background consumer loop - runs in dedicated thread.
   * Completely decoupled from main Kafka Connect data flow.
   */
  private void consumeLoop() {
    LOG.info("[RAFT-TRANSPORT] [{}] Consumer loop started", nodeId);
    long messagesProcessed = 0;
    long lastLogTime = System.currentTimeMillis();

    try {
      while (running.get()) {
        try {
          ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);

          if (!records.isEmpty()) {
            LOG.debug("[RAFT-TRANSPORT] [{}] Polled {} election messages", nodeId, records.count());
          }

          for (ConsumerRecord<String, String> record : records) {
            try {
              processElectionMessage(record);
              messagesProcessed++;

              // Log throughput stats every 100 messages
              if (messagesProcessed % 100 == 0) {
                long now = System.currentTimeMillis();
                long elapsed = now - lastLogTime;
                double rate = (100.0 / elapsed) * 1000;
                LOG.debug("[RAFT-TRANSPORT] [{}] Processed {} messages (rate: {:.1f} msg/sec)",
                    nodeId, messagesProcessed, rate);
                lastLogTime = now;
              }

            } catch (Exception e) {
              LOG.error(
                  "[RAFT-TRANSPORT] [{}] Error processing election message from partition {} offset {}",
                  nodeId,
                  record.partition(),
                  record.offset(),
                  e);
              // Continue processing other messages
            }
          }

        } catch (WakeupException e) {
          // Expected during shutdown
          LOG.debug("[RAFT-TRANSPORT] [{}] Consumer wakeup received, exiting loop", nodeId, e);
          break;
        } catch (Exception e) {
          if (running.get()) {
            LOG.error("[RAFT-TRANSPORT] [{}] Error in consumer poll loop", nodeId, e);
            // Brief pause before retry to avoid tight loop on persistent errors
            try {
              LOG.debug("[RAFT-TRANSPORT] [{}] Sleeping 1s before retry after error", nodeId);
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              LOG.debug("[RAFT-TRANSPORT] [{}] Sleep interrupted, exiting loop", nodeId, ie);
              break;
            }
          }
        }
      }
    } finally {
      LOG.info("[RAFT-TRANSPORT] [{}] Consumer loop exited (total messages processed: {})",
          nodeId, messagesProcessed);
    }
  }

  /**
   * Process a single election message from Kafka.
   *
   * @param record The Kafka record containing election message
   */
  private void processElectionMessage(ConsumerRecord<String, String> record) {
    LOG.trace("[RAFT-TRANSPORT] [{}] Processing message from partition {} offset {}",
        nodeId, record.partition(), record.offset());

    try {
      // Deserialize message
      RaftElectionMessage message = MAPPER.readValue(record.value(), RaftElectionMessage.class);

      LOG.trace("[RAFT-TRANSPORT] [{}] Deserialized {} from sender {} (group={})",
          nodeId, message.payloadType(), message.senderId(), message.groupId());

      // Skip messages from self (broadcast includes sender)
      if (message.senderId().equals(nodeId)) {
        LOG.trace("[RAFT-TRANSPORT] [{}] Skipping self-sent {} message",
            nodeId, message.payloadType());
        return;
      }

      // Decode payload
      Payload payload = message.decodePayload();

      LOG.debug(
          "[RAFT-TRANSPORT] [{}] Received {} from {} (group={})",
          nodeId,
          payload.type(),
          message.senderId(),
          message.groupId());

      // Deliver to listener
      if (messageListener != null) {
        messageListener.onMessage(payload, message.senderId(), message.groupId());
      } else {
        LOG.warn("[RAFT-TRANSPORT] [{}] No message listener registered, dropping {} message from {}",
            nodeId, payload.type(), message.senderId());
      }

    } catch (Exception e) {
      LOG.error("[RAFT-TRANSPORT] [{}] Failed to process election message from partition {} offset {}",
          nodeId, record.partition(), record.offset(), e);
      LOG.debug("[RAFT-TRANSPORT] [{}] Problematic message content: {}", nodeId, record.value());
    }
  }
}
