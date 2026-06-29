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

import java.util.Collections;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class TestElectionMembership {

  private static final String ELECTION_TOPIC = "election-topic";

  @Test
  public void testLeadershipReflectsElectionPartitionOwnership() {
    MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    ElectionMembership membership = new ElectionMembership(consumer, ELECTION_TOPIC, "task-0");

    assertThat(membership.isLeader()).isFalse();

    consumer.assign(ImmutableList.of(new TopicPartition(ELECTION_TOPIC, 0)));
    membership.updateLeadership();
    assertThat(membership.isLeader()).isTrue();

    consumer.assign(Collections.emptyList());
    membership.updateLeadership();
    assertThat(membership.isLeader()).isFalse();
  }

  @Test
  public void testNonElectionPartitionDoesNotGrantLeadership() {
    MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    ElectionMembership membership = new ElectionMembership(consumer, ELECTION_TOPIC, "task-0");

    consumer.assign(ImmutableList.of(new TopicPartition("some-other-topic", 0)));
    membership.updateLeadership();
    assertThat(membership.isLeader()).isFalse();
  }
}
