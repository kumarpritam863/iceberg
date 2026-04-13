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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

public class TestBackpressureController {

  private static final TopicPartition TP = new TopicPartition("topic", 0);

  private static SinkTaskContext mockContext() {
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(Set.of(TP));
    return context;
  }

  private static IcebergSinkConfig mockConfig(boolean enabled, long high) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.backpressureEnabled()).thenReturn(enabled);
    when(config.backpressureMaxBufferedRecords()).thenReturn(high);
    return config;
  }

  @Test
  public void testNoopNeverPauses() {
    BackpressureController bp = BackpressureController.NOOP;
    long before = bp.bufferedRecords();

    for (int i = 0; i < 10_000; i++) {
      bp.recordBuffered();
    }

    assertThat(bp.isPaused()).isFalse();
    assertThat(bp.bufferedRecords()).isEqualTo(before + 10_000);
  }

  @Test
  public void testDisabledNeverPauses() {
    SinkTaskContext context = mockContext();
    IcebergSinkConfig config = mockConfig(false, 100);
    BackpressureController bp = new BackpressureController(context, config);

    for (int i = 0; i < 200; i++) {
      bp.recordBuffered();
    }

    assertThat(bp.isPaused()).isFalse();
    verify(context, never()).pause(any(TopicPartition[].class));
  }

  @Test
  public void testPauseAtHighWatermark() {
    SinkTaskContext context = mockContext();
    IcebergSinkConfig config = mockConfig(true, 100);
    BackpressureController bp = new BackpressureController(context, config);

    for (int i = 0; i < 100; i++) {
      bp.recordBuffered();
    }

    assertThat(bp.isPaused()).isTrue();
    verify(context, times(1)).pause(any(TopicPartition[].class));
  }

  @Test
  public void testNoPauseBeforeHighWatermark() {
    SinkTaskContext context = mockContext();
    IcebergSinkConfig config = mockConfig(true, 100);
    BackpressureController bp = new BackpressureController(context, config);

    for (int i = 0; i < 99; i++) {
      bp.recordBuffered();
    }

    assertThat(bp.isPaused()).isFalse();
    verify(context, never()).pause(any(TopicPartition[].class));
  }

  @Test
  public void testResumeAfterFlush() {
    SinkTaskContext context = mockContext();
    IcebergSinkConfig config = mockConfig(true, 100);
    BackpressureController bp = new BackpressureController(context, config);

    // trigger pause
    for (int i = 0; i < 100; i++) {
      bp.recordBuffered();
    }
    assertThat(bp.isPaused()).isTrue();

    // flush
    bp.recordsFlushed();

    assertThat(bp.isPaused()).isFalse();
    assertThat(bp.bufferedRecords()).isEqualTo(0);
    verify(context, times(1)).resume(any(TopicPartition[].class));
  }

  @Test
  public void testIdempotentPause() {
    SinkTaskContext context = mockContext();
    IcebergSinkConfig config = mockConfig(true, 100);
    BackpressureController bp = new BackpressureController(context, config);

    // go well past high watermark
    for (int i = 0; i < 500; i++) {
      bp.recordBuffered();
    }

    // pause should only be called once
    assertThat(bp.isPaused()).isTrue();
    verify(context, times(1)).pause(any(TopicPartition[].class));
  }

  @Test
  public void testBufferedRecordCount() {
    SinkTaskContext context = mockContext();
    IcebergSinkConfig config = mockConfig(false, Long.MAX_VALUE);
    BackpressureController bp = new BackpressureController(context, config);

    assertThat(bp.bufferedRecords()).isEqualTo(0);
    bp.recordBuffered();
    bp.recordBuffered();
    bp.recordBuffered();
    assertThat(bp.bufferedRecords()).isEqualTo(3);
  }

  @Test
  public void testResetOnFlush() {
    SinkTaskContext context = mockContext();
    IcebergSinkConfig config = mockConfig(false, Long.MAX_VALUE);
    BackpressureController bp = new BackpressureController(context, config);

    bp.recordBuffered();
    bp.recordBuffered();
    assertThat(bp.bufferedRecords()).isEqualTo(2);

    bp.recordsFlushed();
    assertThat(bp.bufferedRecords()).isEqualTo(0);
  }

  @Test
  public void testPauseResumeWithEmptyAssignment() {
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(Set.of());
    IcebergSinkConfig config = mockConfig(true, 10);
    BackpressureController bp = new BackpressureController(context, config);

    // should not throw even with empty assignment
    for (int i = 0; i < 20; i++) {
      bp.recordBuffered();
    }
    bp.recordsFlushed();

    // pause/resume never called because assignment was empty
    assertThat(bp.isPaused()).isFalse();
    verify(context, never()).pause(any(TopicPartition[].class));
    verify(context, never()).resume(any(TopicPartition[].class));
  }

  @Test
  public void testFlushWithoutPauseDoesNotResume() {
    SinkTaskContext context = mockContext();
    IcebergSinkConfig config = mockConfig(true, 100);
    BackpressureController bp = new BackpressureController(context, config);

    // buffer some records but stay below watermark
    for (int i = 0; i < 50; i++) {
      bp.recordBuffered();
    }

    // flush — should NOT call resume since we were never paused
    bp.recordsFlushed();

    assertThat(bp.isPaused()).isFalse();
    verify(context, never()).resume(any(TopicPartition[].class));
  }
}
