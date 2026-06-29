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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Elects a single commit coordinator decoupled from source topic-partition assignment.
 *
 * <p>All tasks of a connector join one shared consumer group subscribed to a single-partition
 * election topic. Kafka's group coordinator assigns that one partition to exactly one live member;
 * that member is the leader and runs the {@code Coordinator}. Leadership therefore depends only on
 * ownership of the election partition, not on which source partitions a task happens to hold, and
 * Kafka provides at-most-one-owner plus automatic failover for free.
 *
 * <p>The election consumer only polls to maintain group membership and observe its assignment; it
 * never processes records and never commits offsets, so its isolation level and offset-reset policy
 * are irrelevant. The poll loop runs on a dedicated daemon thread and publishes the current
 * leadership state via a volatile flag, which the task thread reads in {@code save()} to start or
 * stop the coordinator.
 */
class ElectionMembership implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ElectionMembership.class);
  private static final Duration POLL_DURATION = Duration.ofMillis(500);
  private static final long CLOSE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

  private final Consumer<String, byte[]> consumer;
  private final String electionTopic;
  private final String taskId;
  private final AtomicBoolean leader = new AtomicBoolean(false);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread thread;

  ElectionMembership(Consumer<String, byte[]> consumer, String electionTopic, String taskId) {
    this.consumer = consumer;
    this.electionTopic = electionTopic;
    this.taskId = taskId;
  }

  void start() {
    if (running.compareAndSet(false, true)) {
      consumer.subscribe(ImmutableList.of(electionTopic));
      thread = new Thread(this::run);
      thread.setName("iceberg-election-" + taskId);
      thread.setDaemon(true);
      thread.start();
      LOG.info(
          "Started coordinator election for task {} on election topic {}", taskId, electionTopic);
    }
  }

  boolean isLeader() {
    return leader.get();
  }

  private void run() {
    try {
      while (running.get()) {
        // records are intentionally discarded; the poll only maintains group membership
        consumer.poll(POLL_DURATION);
        updateLeadership();
      }
    } catch (WakeupException e) {
      // expected when close() wakes up the consumer
    } catch (Exception e) {
      LOG.error(
          "Coordinator election thread failed for task {}, relinquishing leadership", taskId, e);
    } finally {
      leader.set(false);
    }
  }

  @VisibleForTesting
  void updateLeadership() {
    boolean ownsPartition =
        consumer.assignment().stream().anyMatch(tp -> electionTopic.equals(tp.topic()));
    if (leader.compareAndSet(!ownsPartition, ownsPartition)) {
      if (ownsPartition) {
        LOG.info(
            "Task {} owns the election topic {} partition, elected coordinator",
            taskId,
            electionTopic);
      } else {
        LOG.info(
            "Task {} lost the election topic {} partition, relinquishing coordinator",
            taskId,
            electionTopic);
      }
    }
  }

  @Override
  public void close() {
    if (running.compareAndSet(true, false)) {
      consumer.wakeup();
      if (thread != null) {
        try {
          thread.join(CLOSE_TIMEOUT_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      try {
        consumer.close();
      } catch (Exception e) {
        LOG.warn("Error closing election consumer for task {}", taskId, e);
      }
      leader.set(false);
      LOG.info("Stopped coordinator election for task {}", taskId);
    }
  }
}
