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

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

public class TestCoordinatorMetrics {

  private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();

  @Test
  public void testStartAndStopAdjustGaugeSymmetrically() {
    CoordinatorMetrics metrics = CoordinatorMetrics.attach("connector-symmetry");
    try {
      // Use deltas: the counter is shared across a connector's tasks in the JVM.
      int before = metrics.activeCoordinatorCount();

      metrics.coordinatorStarted();
      metrics.coordinatorStarted();
      assertThat(metrics.activeCoordinatorCount()).isEqualTo(before + 2);

      metrics.coordinatorStopped();
      assertThat(metrics.activeCoordinatorCount()).isEqualTo(before + 1);

      metrics.coordinatorStopped();
      assertThat(metrics.activeCoordinatorCount()).isEqualTo(before);
    } finally {
      metrics.detach();
    }
  }

  @Test
  public void testSameConnectorSharesInstanceDistinctConnectorsDoNot() {
    CoordinatorMetrics shared1 = CoordinatorMetrics.attach("connector-shared");
    CoordinatorMetrics shared2 = CoordinatorMetrics.attach("connector-shared");
    CoordinatorMetrics other = CoordinatorMetrics.attach("connector-other");
    try {
      assertThat(shared1).isSameAs(shared2);
      assertThat(shared1).isNotSameAs(other);
    } finally {
      shared1.detach();
      shared2.detach();
      other.detach();
    }
  }

  @Test
  public void testMBeanIsRegisteredPerConnectorAndReadable() throws Exception {
    CoordinatorMetrics metrics = CoordinatorMetrics.attach("connector-mbean");
    ObjectName objectName = CoordinatorMetrics.objectName("connector-mbean");
    try {
      assertThat(objectName.getKeyProperty("connector"))
          .isEqualTo(ObjectName.quote("connector-mbean"));
      assertThat(SERVER.isRegistered(objectName)).isTrue();
      assertThat((Integer) SERVER.getAttribute(objectName, "ActiveCoordinatorCount"))
          .isEqualTo(metrics.activeCoordinatorCount());
    } finally {
      metrics.detach();
    }
  }

  @Test
  public void testMBeanUnregisteredOnlyWhenLastTaskDetaches() throws Exception {
    CoordinatorMetrics task1 = CoordinatorMetrics.attach("connector-refcount");
    CoordinatorMetrics task2 = CoordinatorMetrics.attach("connector-refcount");
    ObjectName objectName = CoordinatorMetrics.objectName("connector-refcount");

    assertThat(SERVER.isRegistered(objectName)).isTrue();

    // First task leaves: sibling task still running, MBean must remain.
    task1.detach();
    assertThat(SERVER.isRegistered(objectName)).isTrue();

    // Last task leaves: MBean is torn down.
    task2.detach();
    assertThat(SERVER.isRegistered(objectName)).isFalse();
  }

  @Test
  public void testReattachAfterFullDetachReregisters() throws Exception {
    ObjectName objectName = CoordinatorMetrics.objectName("connector-reattach");

    CoordinatorMetrics first = CoordinatorMetrics.attach("connector-reattach");
    first.detach();
    assertThat(SERVER.isRegistered(objectName)).isFalse();

    // A new task for the same connector re-registers a fresh MBean.
    CoordinatorMetrics second = CoordinatorMetrics.attach("connector-reattach");
    try {
      assertThat(SERVER.isRegistered(objectName)).isTrue();
      assertThat(second).isNotSameAs(first);
    } finally {
      second.detach();
    }
  }

  @Test
  public void testUnbalancedDetachIsIgnored() {
    CoordinatorMetrics metrics = CoordinatorMetrics.attach("connector-unbalanced");
    metrics.detach();
    // Extra detach must not throw or unregister anything else.
    metrics.detach();
  }
}
