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
package org.apache.iceberg.connect;

import java.util.Collection;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * Pluggable strategy that owns the Kafka Connect sink's commit lifecycle for Iceberg.
 *
 * <p>An implementation is selected via the {@code iceberg.committer.class} configuration property.
 * When unset, the built-in {@code CommitterImpl} (leader-elected coordinator + per-task worker over
 * a Kafka control topic) is used.
 *
 * <h2>Lifecycle</h2>
 *
 * The framework drives a Committer in this order:
 *
 * <ol>
 *   <li>Public no-arg constructor — required for dynamic loading.
 *   <li>{@link #configure(IcebergSinkConfig)} — called exactly once, before any other method.
 *   <li>{@link #open(Catalog, IcebergSinkConfig, SinkTaskContext, Collection)} — called when
 *       partitions are assigned. May be invoked multiple times across the task's lifetime as
 *       partitions are reassigned.
 *   <li>{@link #save(Collection)} — called repeatedly while the task is open.
 *   <li>{@link #close(Collection)} — called when partitions are revoked. May be followed by another
 *       {@link #open} if more partitions are assigned later.
 *   <li>A final {@link #close(Collection)} with an empty collection on task shutdown.
 * </ol>
 *
 * <h2>Threading</h2>
 *
 * All methods are invoked from the single Kafka Connect SinkTask thread. Implementations do not
 * need internal synchronization for fields written in {@link #configure} or {@link #open} and read
 * elsewhere on the same task.
 *
 * <h2>Implementation requirements</h2>
 *
 * <ul>
 *   <li>Provide a {@code public} no-arg constructor.
 *   <li>Implement {@link #open}, {@link #close(Collection)}, and {@link #save}.
 *   <li>Optionally override {@link #configure} to capture the sink config (including any {@code
 *       iceberg.committer.*} properties exposed via {@link IcebergSinkConfig#committerProps()}).
 * </ul>
 */
public interface Committer {

  /**
   * @deprecated will be removed in 2.0.0. Use {@link #open(Catalog, IcebergSinkConfig,
   *     SinkTaskContext, Collection)} instead.
   */
  @Deprecated
  void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context);

  default void open(
      Catalog catalog,
      IcebergSinkConfig config,
      SinkTaskContext context,
      Collection<TopicPartition> addedPartitions) {
    // Default implementation calls the deprecated method. Implementations may override this.
    start(catalog, config, context);
  }

  /**
   * @deprecated will be removed in 2.0.0. Use {@link #close(Collection)} instead.
   */
  @Deprecated
  void stop();

  default void close(Collection<TopicPartition> closedPartitions) {
    // Default implementation calls the deprecated method. Implementations may override this.
    stop();
  }

  void save(Collection<SinkRecord> sinkRecords);

  /**
   * Configures this committer with the sink configuration.
   *
   * <p>Called exactly once by the framework immediately after instantiation, before any {@link
   * #open}, {@link #save}, or {@link #close} call. The default implementation is a no-op.
   *
   * <p>Custom Committer implementations may override this to capture the {@link IcebergSinkConfig}
   * for later use, including reading properties under their own {@code iceberg.committer.*} prefix
   * via {@link IcebergSinkConfig#committerProps()}.
   *
   * <p>Threading: invoked from the Kafka Connect SinkTask thread. Should be cheap and non-blocking
   * — defer network or filesystem I/O to {@link #open}.
   *
   * @param config the sink config; never {@code null}
   * @throws RuntimeException if the committer cannot be configured; the framework wraps this in a
   *     {@code ConfigException} and fails the task at startup
   */
  default void configure(IcebergSinkConfig config) {
    // no-op
  }
}
