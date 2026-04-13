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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestConnectorMetrics {

  @Test
  public void testNoopInstance() {
    ConnectorMetrics metrics = ConnectorMetrics.NOOP;
    assertThat(metrics.isEnabled()).isFalse();

    // should not throw
    metrics.recordsReceived(10);
    metrics.recordWritten("db.table");
    metrics.recordDropped();
    metrics.recordConversionError("db.table");
    metrics.commitStarted();
    metrics.commitSucceeded(100L);
    metrics.commitFailed(50L);
    metrics.filesWritten("db.table", 3, 1);
    metrics.flushCompleted("db.table", 200L);
    metrics.schemaEvolved("db.table");
    metrics.tableAutoCreated("db.table");
    metrics.commitTimedOut();
    metrics.close();

    // NOOP counters stay at 0
    assertThat(metrics.recordsReceivedTotal()).isEqualTo(0);
    assertThat(metrics.recordsWrittenTotal()).isEqualTo(0);
  }

  @Test
  public void testCreateReturnsEnabledInstance() {
    // create() with a mock context — falls back to counter-only since PluginMetrics unavailable
    ConnectorMetrics metrics = ConnectorMetrics.create(null);
    assertThat(metrics.isEnabled()).isTrue();
  }

  @Test
  public void testRecordsReceivedCounter() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);
    assertThat(metrics.recordsReceivedTotal()).isEqualTo(0);

    metrics.recordsReceived(5);
    assertThat(metrics.recordsReceivedTotal()).isEqualTo(5);

    metrics.recordsReceived(3);
    assertThat(metrics.recordsReceivedTotal()).isEqualTo(8);
  }

  @Test
  public void testRecordsWrittenCounter() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.recordWritten("db.orders");
    metrics.recordWritten("db.orders");
    metrics.recordWritten("db.users");
    assertThat(metrics.recordsWrittenTotal()).isEqualTo(3);
  }

  @Test
  public void testRecordsDroppedCounter() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.recordDropped();
    metrics.recordDropped();
    assertThat(metrics.recordsDroppedTotal()).isEqualTo(2);
  }

  @Test
  public void testConversionErrorCounter() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.recordConversionError("db.orders");
    assertThat(metrics.recordConversionErrorsTotal()).isEqualTo(1);
  }

  @Test
  public void testCommitCounters() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.commitStarted();
    metrics.commitStarted();
    metrics.commitSucceeded(100L);
    metrics.commitFailed(50L);

    assertThat(metrics.commitTotal()).isEqualTo(2);
    assertThat(metrics.commitSuccessTotal()).isEqualTo(1);
    assertThat(metrics.commitFailureTotal()).isEqualTo(1);
  }

  @Test
  public void testFilesWrittenCounter() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.filesWritten("db.orders", 3, 1);
    metrics.filesWritten("db.users", 2, 0);

    assertThat(metrics.dataFilesWrittenTotal()).isEqualTo(5);
    assertThat(metrics.deleteFilesWrittenTotal()).isEqualTo(1);
  }

  @Test
  public void testActiveWritersGauge() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    // no supplier registered
    assertThat(metrics.activeWriters()).isEqualTo(0);

    // register supplier
    metrics.registerActiveWriters(() -> 42);
    assertThat(metrics.activeWriters()).isEqualTo(42);
  }

  @Test
  public void testSchemaEvolutionCounter() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.schemaEvolved("db.orders");
    metrics.schemaEvolved("db.orders");
    assertThat(metrics.schemaEvolutionsTotal()).isEqualTo(2);
  }

  @Test
  public void testTableAutoCreatedCounter() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.tableAutoCreated("db.new_table");
    assertThat(metrics.tablesAutoCreatedTotal()).isEqualTo(1);
  }

  @Test
  public void testCommitTimeoutCounter() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.commitTimedOut();
    metrics.commitTimedOut();
    assertThat(metrics.commitTimeoutTotal()).isEqualTo(2);
  }
}
