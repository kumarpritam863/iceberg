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
package org.apache.iceberg.connect.data;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controls backpressure by pausing Kafka Connect ingestion when the number of buffered records
 * exceeds a high watermark, and resuming when records are flushed below a low watermark.
 *
 * <p>Uses the standard {@link SinkTaskContext#pause} / {@link SinkTaskContext#resume} API. When
 * disabled (default), all methods are effectively no-ops with only a cheap atomic counter
 * increment.
 */
public class BackpressureController {

  private static final Logger LOG = LoggerFactory.getLogger(BackpressureController.class);

  public static final BackpressureController NOOP =
      new BackpressureController(null, false, Long.MAX_VALUE);

  private final SinkTaskContext context;
  private final boolean enabled;
  private final long highWatermark;
  private final AtomicLong bufferedRecords = new AtomicLong(0);
  private volatile boolean paused = false;

  public BackpressureController(SinkTaskContext context, IcebergSinkConfig config) {
    this.context = context;
    this.enabled = config.backpressureEnabled();
    this.highWatermark = config.backpressureMaxBufferedRecords();
  }

  private BackpressureController(SinkTaskContext context, boolean enabled, long highWatermark) {
    this.context = context;
    this.enabled = enabled;
    this.highWatermark = highWatermark;
  }

  /** Called after each record is written to a table writer. */
  void recordBuffered() {
    long count = bufferedRecords.incrementAndGet();
    if (enabled && !paused && count >= highWatermark) {
      pause();
    }
  }

  /** Called after completeWrite() flushes all writers. */
  void recordsFlushed() {
    bufferedRecords.set(0);
    if (enabled && paused) {
      resume();
    }
  }

  private void pause() {
    Collection<TopicPartition> partitions = context.assignment();
    if (!partitions.isEmpty()) {
      context.pause(partitions.toArray(new TopicPartition[0]));
      paused = true;
      LOG.warn(
          "Backpressure activated: {} records buffered (high watermark: {})",
          bufferedRecords.get(),
          highWatermark);
    }
  }

  private void resume() {
    Collection<TopicPartition> partitions = context.assignment();
    if (!partitions.isEmpty()) {
      context.resume(partitions.toArray(new TopicPartition[0]));
      paused = false;
      LOG.info("Backpressure released: records flushed");
    }
  }

  long bufferedRecords() {
    return bufferedRecords.get();
  }

  boolean isPaused() {
    return paused;
  }
}
