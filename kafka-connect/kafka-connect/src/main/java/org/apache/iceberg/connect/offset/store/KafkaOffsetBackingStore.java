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
package org.apache.iceberg.connect.offset.store;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetUtils;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConvertingFutureCallback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaOffsetBackingStore implements IcebergOffsetBackingStore {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetBackingStore.class);

  private KafkaBasedLog<byte[], byte[]> offsetLog;
  // Visible for testing
  private final Map<ByteBuffer, ByteBuffer> data = Maps.newHashMap();
  private final Map<String, Set<Map<String, Object>>> connectorPartitions = Maps.newHashMap();
  private Converter keyConverter;

  public KafkaOffsetBackingStore(Converter keyConverter) {
    this.keyConverter = keyConverter;
  }

  public static KafkaOffsetBackingStore readWriteStore(
      String topic,
      Producer<byte[], byte[]> producer,
      Consumer<byte[], byte[]> consumer,
      TopicAdmin topicAdmin,
      Converter keyConverter) {
    return new KafkaOffsetBackingStore(keyConverter) {
      @Override
      public void configure(final Map<String, Object> config) {
        setKafkaBasedLog(topic, producer, consumer, topicAdmin);
      }
    };
  }

  void setKafkaBasedLog(
      String topic,
      Producer<byte[], byte[]> producer,
      Consumer<byte[], byte[]> consumer,
      TopicAdmin topicAdmin) {
    this.offsetLog =
        KafkaBasedLog.withExistingClients(
            topic,
            consumer,
            producer,
            topicAdmin,
            consumedCallback,
            Time.SYSTEM,
            null,
            ignored -> true);
  }

  @Override
  public void start() {
    LOG.info("Starting KafkaOffsetBackingStore");
    try {
      offsetLog.start();
    } catch (UnsupportedVersionException e) {
      String message =
          "When "
              + ConsumerConfig.ISOLATION_LEVEL_CONFIG
              + "is set to "
              + IsolationLevel.READ_COMMITTED
              + ", a Kafka broker version that allows admin clients to read consumer offsets is required. "
              + "Please either reconfigure the worker or connector, or upgrade to a newer Kafka broker version.";
      throw new ConnectException(message, e);
    }
    LOG.info("Finished reading offsets topic and starting KafkaOffsetBackingStore");
  }

  /**
   * Stop reading from and writing to the offsets topic, and relinquish resources allocated for
   * interacting with it, including Kafka clients.
   *
   * <p>The admin client derived from the given {@link Supplier} will not be closed and it is the
   * caller's responsibility to manage its lifecycle accordingly.
   */
  @Override
  public void stop() {
    LOG.info("Stopping KafkaOffsetBackingStore");
    offsetLog.stop();
    LOG.info("Stopped KafkaOffsetBackingStore");
  }

  @Override
  public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
    ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>> future =
        new ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>>() {
          @Override
          public Map<ByteBuffer, ByteBuffer> convert(Void result) {
            Map<ByteBuffer, ByteBuffer> values = Maps.newHashMap();
            for (ByteBuffer key : keys) {
              values.put(key, data.get(key));
            }
            return values;
          }
        };
    // This operation may be relatively (but not too) expensive since it always requires checking
    // end offsets, even
    // if we've already read up to the end. However, it also should not be common (offsets should
    // only be read when
    // resetting a task). Always requiring that we read to the end is simpler than trying to
    // differentiate when it
    // is safe not to (which should only be if we *know* we've maintained ownership since the last
    // write).
    offsetLog.readToEnd(future);
    return future;
  }

  @Override
  public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values, final Callback<Void> callback) {
    SetCallbackFuture producerCallback = new SetCallbackFuture(values.size(), callback);

    for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
      ByteBuffer key = entry.getKey();
      ByteBuffer value = entry.getValue();
      offsetLog.send(
          key == null ? null : key.array(), value == null ? null : value.array(), producerCallback);
    }

    return producerCallback;
  }

  @Override
  public Set<Map<String, Object>> connectorPartitions(String connectorName) {
    return connectorPartitions.getOrDefault(connectorName, Collections.emptySet());
  }

  private final Callback<ConsumerRecord<byte[], byte[]>> consumedCallback =
      (error, record) -> {
        if (error != null) {
          LOG.error("Failed to read from the offsets topic", error);
          return;
        }

        OffsetUtils.processPartitionKey(
            record.key(), record.value(), keyConverter, connectorPartitions);

        ByteBuffer key = record.key() != null ? ByteBuffer.wrap(record.key()) : null;

        if (record.value() == null) {
          data.remove(key);
        } else {
          data.put(key, ByteBuffer.wrap(record.value()));
        }
      };

  private static class SetCallbackFuture
      implements org.apache.kafka.clients.producer.Callback, Future<Void> {
    private int numLeft;
    private boolean completed = false;
    private Throwable exception = null;
    private final Callback<Void> callback;

    SetCallbackFuture(int numRecords, Callback<Void> callback) {
      numLeft = numRecords;
      this.callback = callback;
    }

    @Override
    public synchronized void onCompletion(RecordMetadata metadata, Exception ex) {
      if (ex != null) {
        if (!completed) {
          this.exception = ex;
          callback.onCompletion(ex, null);
          completed = true;
          this.notify();
        }
        return;
      }

      numLeft -= 1;
      if (numLeft == 0) {
        callback.onCompletion(null, null);
        completed = true;
        this.notify();
      }
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public synchronized boolean isCancelled() {
      return false;
    }

    @Override
    public synchronized boolean isDone() {
      return completed;
    }

    @Override
    public synchronized Void get() throws InterruptedException, ExecutionException {
      while (!completed) {
        this.wait();
      }
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return null;
    }

    @Override
    public synchronized Void get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      long started = System.currentTimeMillis();
      long limit = started + unit.toMillis(timeout);
      while (!completed) {
        long leftMs = limit - System.currentTimeMillis();
        if (leftMs < 0) {
          throw new TimeoutException("KafkaOffsetBackingStore Future timed out.");
        }
        this.wait(leftMs);
      }
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return null;
    }
  }

  @Override
  public void configure(Map<String, Object> config) {
    // No Op
  }
}
