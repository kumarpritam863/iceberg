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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestElectionTopicCommitter {

  @Test
  public void testValidateElectionTopicAcceptsSinglePartition() throws Exception {
    ElectionTopicCommitter committer = newCommitterWithMockClientFactory();
    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      kafkaUtils.when(() -> KafkaUtils.describeTopicPartitionCount(any(), any())).thenReturn(1);
      assertThatCode(() -> committer.validateElectionTopic("election-topic"))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void testValidateElectionTopicRejectsMultiPartition() throws Exception {
    ElectionTopicCommitter committer = newCommitterWithMockClientFactory();
    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      kafkaUtils.when(() -> KafkaUtils.describeTopicPartitionCount(any(), any())).thenReturn(3);
      assertThatThrownBy(() -> committer.validateElectionTopic("multi-partition-topic"))
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("must have exactly 1 partition");
    }
  }

  private ElectionTopicCommitter newCommitterWithMockClientFactory()
      throws NoSuchFieldException, IllegalAccessException {
    ElectionTopicCommitter committer = new ElectionTopicCommitter();
    KafkaClientFactory clientFactory = mock(KafkaClientFactory.class);
    when(clientFactory.createAdmin()).thenReturn(mock(Admin.class));
    Field field = ElectionTopicCommitter.class.getDeclaredField("clientFactory");
    field.setAccessible(true);
    field.set(committer, clientFactory);
    return committer;
  }
}
