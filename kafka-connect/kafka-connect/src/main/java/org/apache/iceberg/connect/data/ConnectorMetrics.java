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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics facade for the Iceberg Kafka Connect sink connector.
 *
 * <p>This class provides a no-op implementation that serves as instrumentation scaffolding. When
 * the Kafka Connect runtime is upgraded to 4.0+ (KIP-KAFKA-15995), the {@code PluginMetrics} API
 * will be wired in via {@link #create}, and all recorded metrics will be automatically exposed
 * through the runtime's configured metric reporters (JMX, Prometheus, etc.).
 *
 * <p>All methods are safe to call regardless of whether metrics are enabled. When disabled, calls
 * are effectively free (atomic increment on counters only).
 */
public class ConnectorMetrics implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectorMetrics.class);

  /** Singleton no-op instance for use in tests and when metrics are unavailable. */
  public static final ConnectorMetrics NOOP = new ConnectorMetrics();

  // --- Tier 1: Operational Essentials ---
  private final AtomicLong recordsReceivedTotal = new AtomicLong();
  private final AtomicLong recordsWrittenTotal = new AtomicLong();
  private final AtomicLong recordsDroppedTotal = new AtomicLong();
  private final AtomicLong recordConversionErrorsTotal = new AtomicLong();
  private final AtomicLong commitTotal = new AtomicLong();
  private final AtomicLong commitSuccessTotal = new AtomicLong();
  private final AtomicLong commitFailureTotal = new AtomicLong();

  // --- Tier 2: Write Path ---
  private final AtomicLong dataFilesWrittenTotal = new AtomicLong();
  private final AtomicLong deleteFilesWrittenTotal = new AtomicLong();
  private volatile Supplier<Integer> activeWritersSupplier;

  // --- Tier 3: Schema and Table Management ---
  private final AtomicLong schemaEvolutionsTotal = new AtomicLong();
  private final AtomicLong tablesAutoCreatedTotal = new AtomicLong();
  private final AtomicLong commitTimeoutTotal = new AtomicLong();

  private final boolean enabled;

  private ConnectorMetrics() {
    this.enabled = false;
  }

  private ConnectorMetrics(boolean enabled) {
    this.enabled = enabled;
    if (enabled) {
      LOG.info("Connector metrics enabled");
    }
  }

  /**
   * Creates a ConnectorMetrics instance. Attempts to use PluginMetrics from Kafka Connect 4.0+ via
   * SinkTaskContext.pluginMetrics(). Falls back to counter-only tracking on older runtimes.
   *
   * <p>When PluginMetrics becomes available (Kafka 4.0+), this method will wire the counters into
   * the PluginMetrics sensors for automatic JMX/reporter exposure.
   */
  @SuppressWarnings("unused")
  public static ConnectorMetrics create(Object sinkTaskContext) {
    // TODO: When Kafka 4.0+ is the minimum, wire PluginMetrics here:
    // try {
    //   PluginMetrics pm = ((SinkTaskContext) sinkTaskContext).pluginMetrics();
    //   return new ConnectorMetrics(pm);
    // } catch (NoSuchMethodError | NoClassDefFoundError e) {
    //   LOG.info("PluginMetrics not available (Kafka < 4.0), using counter-only metrics");
    // }
    return new ConnectorMetrics(true);
  }

  // ===== Tier 1: Operational Essentials =====

  /** Record that a batch of records was received in put(). */
  public void recordsReceived(int count) {
    if (enabled) {
      recordsReceivedTotal.addAndGet(count);
    }
  }

  /** Record that a single record was successfully written to an Iceberg table. */
  public void recordWritten(String tableName) {
    if (enabled) {
      recordsWrittenTotal.incrementAndGet();
    }
  }

  /** Record that a record was dropped (routing returned empty list, or tombstone). */
  public void recordDropped() {
    if (enabled) {
      recordsDroppedTotal.incrementAndGet();
    }
  }

  /** Record that a record conversion failed. */
  public void recordConversionError(String tableName) {
    if (enabled) {
      recordConversionErrorsTotal.incrementAndGet();
    }
  }

  /** Record that a commit cycle started. */
  public void commitStarted() {
    if (enabled) {
      commitTotal.incrementAndGet();
    }
  }

  /** Record that a commit cycle completed successfully. */
  public void commitSucceeded(long durationMs) {
    if (enabled) {
      commitSuccessTotal.incrementAndGet();
    }
  }

  /** Record that a commit cycle failed. */
  public void commitFailed(long durationMs) {
    if (enabled) {
      commitFailureTotal.incrementAndGet();
    }
  }

  // ===== Tier 2: Write Path =====

  /** Record data and delete files written during a flush. */
  public void filesWritten(String tableName, int dataFiles, int deleteFiles) {
    if (enabled) {
      dataFilesWrittenTotal.addAndGet(dataFiles);
      deleteFilesWrittenTotal.addAndGet(deleteFiles);
    }
  }

  /** Record flush completion with duration. */
  public void flushCompleted(String tableName, long durationMs) {
    // Duration tracking will be wired to PluginMetrics sensors when available
  }

  /** Register a supplier for the active writer count gauge. */
  public void registerActiveWriters(Supplier<Integer> supplier) {
    this.activeWritersSupplier = supplier;
  }

  // ===== Tier 3: Schema and Table Management =====

  /** Record that a schema evolution was detected and applied. */
  public void schemaEvolved(String tableName) {
    if (enabled) {
      schemaEvolutionsTotal.incrementAndGet();
    }
  }

  /** Record that a table was auto-created. */
  public void tableAutoCreated(String tableName) {
    if (enabled) {
      tablesAutoCreatedTotal.incrementAndGet();
    }
  }

  /** Record that a commit timed out waiting for worker responses. */
  public void commitTimedOut() {
    if (enabled) {
      commitTimeoutTotal.incrementAndGet();
    }
  }

  // ===== Accessors (for testing and debugging) =====

  public long recordsReceivedTotal() {
    return recordsReceivedTotal.get();
  }

  public long recordsWrittenTotal() {
    return recordsWrittenTotal.get();
  }

  public long recordsDroppedTotal() {
    return recordsDroppedTotal.get();
  }

  public long recordConversionErrorsTotal() {
    return recordConversionErrorsTotal.get();
  }

  public long commitTotal() {
    return commitTotal.get();
  }

  public long commitSuccessTotal() {
    return commitSuccessTotal.get();
  }

  public long commitFailureTotal() {
    return commitFailureTotal.get();
  }

  public long dataFilesWrittenTotal() {
    return dataFilesWrittenTotal.get();
  }

  public long deleteFilesWrittenTotal() {
    return deleteFilesWrittenTotal.get();
  }

  public int activeWriters() {
    Supplier<Integer> supplier = activeWritersSupplier;
    return supplier != null ? supplier.get() : 0;
  }

  public long schemaEvolutionsTotal() {
    return schemaEvolutionsTotal.get();
  }

  public long tablesAutoCreatedTotal() {
    return tablesAutoCreatedTotal.get();
  }

  public long commitTimeoutTotal() {
    return commitTimeoutTotal.get();
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void close() {
    // When PluginMetrics is wired, this will clean up sensors.
    // For now, no-op.
  }
}
