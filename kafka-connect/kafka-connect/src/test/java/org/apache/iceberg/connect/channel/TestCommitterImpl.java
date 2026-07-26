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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

public class TestCommitterImpl {

  @Test
  public void testLeaderPartitionIsLowestSubscribedTopicPartitionZero() {
    // Minimum under (topic, partition) ordering is always partition 0 of the smallest topic name.
    assertThat(CommitterImpl.leaderPartition(ImmutableSet.of("topic-b", "topic-a", "topic-c")))
        .contains(new TopicPartition("topic-a", 0));
  }

  @Test
  public void testLeaderPartitionEmptyWhenSubscriptionUnresolved() {
    assertThat(CommitterImpl.leaderPartition(ImmutableSet.of())).isEmpty();
  }

  @Test
  public void testReconcileDoesNotStartCoordinatorWhenNotLeader() throws Exception {
    CommitterImpl committer = new CommitterImpl();

    SinkTaskContext context = mock(SinkTaskContext.class);
    // Owns a partition of the lowest topic, but not partition 0 → not the leader.
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition("topic-a", 1)));
    setField(committer, "context", context);
    setField(committer, "consumer", mockConsumer(ImmutableSet.of("topic-a", "topic-b")));

    committer.save(Collections.emptyList());

    assertThat(getField(committer, "coordinatorThread")).isNull();
  }

  @Test
  public void testReconcileStopsCoordinatorWhenLeaderPartitionLost() throws Exception {
    CommitterImpl committer = new CommitterImpl();

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition("topic-a", 1)));
    setField(committer, "context", context);
    setField(committer, "consumer", mockConsumer(ImmutableSet.of("topic-a")));
    setField(committer, "coordinatorMetrics", CoordinatorMetrics.attach("test-connector"));

    CoordinatorThread coordinatorThread = mock(CoordinatorThread.class);
    when(coordinatorThread.isTerminated()).thenReturn(false);
    setField(committer, "coordinatorThread", coordinatorThread);

    committer.save(Collections.emptyList());

    verify(coordinatorThread).terminate();
    assertThat(getField(committer, "coordinatorThread")).isNull();
  }

  @Test
  public void testCommitFailurePropagatesAsNotRunningException() throws Exception {
    Coordinator coordinator = mock(Coordinator.class);
    doThrow(new RuntimeException("commit failed")).when(coordinator).process();

    CoordinatorThread coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();

    // wait for the thread to catch the exception, set terminated, and call stop
    verify(coordinator, timeout(1000)).stop();
    assertThat(coordinatorThread.isTerminated()).isTrue();

    CommitterImpl committer = new CommitterImpl();
    setField(committer, "coordinatorThread", coordinatorThread);
    // Empty subscription → reconcile resolves no leader and returns before touching the
    // coordinator, so processControlEvents surfaces the terminated thread.
    setField(committer, "consumer", mockConsumer(ImmutableSet.of()));

    assertThatThrownBy(() -> committer.save(Collections.emptyList()))
        .isInstanceOf(NotRunningException.class)
        .hasMessageContaining("Coordinator unexpectedly terminated");
  }

  @Test
  public void testStartFailurePropagatesAsNotRunningException() throws Exception {
    Coordinator coordinator = mock(Coordinator.class);
    doThrow(new RuntimeException("start failed")).when(coordinator).start();

    CoordinatorThread coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();

    // wait for the thread to catch the exception, set terminated, and call stop
    verify(coordinator, timeout(1000)).stop();
    assertThat(coordinatorThread.isTerminated()).isTrue();

    CommitterImpl committer = new CommitterImpl();
    setField(committer, "coordinatorThread", coordinatorThread);
    // Empty subscription → reconcile resolves no leader and returns before touching the
    // coordinator, so processControlEvents surfaces the terminated thread.
    setField(committer, "consumer", mockConsumer(ImmutableSet.of()));

    assertThatThrownBy(() -> committer.save(Collections.emptyList()))
        .isInstanceOf(NotRunningException.class)
        .hasMessageContaining("Coordinator unexpectedly terminated");
  }

  @SuppressWarnings("unchecked")
  private static Consumer<byte[], byte[]> mockConsumer(Set<String> subscription) {
    Consumer<byte[], byte[]> consumer = mock(Consumer.class);
    when(consumer.subscription()).thenReturn(subscription);
    return consumer;
  }

  private static void setField(CommitterImpl committer, String name, Object value)
      throws Exception {
    Field field = CommitterImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(committer, value);
  }

  private static Object getField(CommitterImpl committer, String name) throws Exception {
    Field field = CommitterImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(committer);
  }
}
