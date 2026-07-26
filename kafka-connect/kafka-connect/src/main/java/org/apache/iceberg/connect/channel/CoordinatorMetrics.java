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

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-connector gauge of how many commit {@link Coordinator}s are currently running in this Connect
 * worker JVM.
 *
 * <p>Each sink task runs at most one coordinator, so this counts the task instances of a given
 * connector, in this worker, that currently own the connector's leader partition. There is one
 * instance (and one JMX MBean) per connector name, shared across that connector's tasks in the JVM;
 * the cluster-wide total for a connector is the sum across worker JVMs.
 *
 * <p>The gauge is exposed as the JMX attribute {@code ActiveCoordinatorCount} on {@code
 * org.apache.iceberg.connect:type=CoordinatorMetrics,connector="<name>"}. Expected steady state is
 * 1 per connector cluster-wide; a sustained value {@code > 1} indicates a split-brain (multiple
 * coordinators), which is safe under the offsets CAS but wasteful, and {@code 0} for longer than a
 * commit interval indicates a coordinator-absent gap.
 *
 * <p><b>Lifecycle:</b> a task {@link #attach(String) attaches} at start and {@link #detach()
 * detaches} at stop. The MBean is registered when the first task of a connector attaches in this
 * JVM and unregistered when the last one detaches, so a deleted connector leaves no lingering
 * MBean. Attach/detach transitions are serialized to avoid registering/unregistering races (e.g. a
 * new task attaching while the last one is tearing down); the {@code ActiveCoordinatorCount} reads
 * and updates stay lock-free.
 *
 * <p>Metric failures never affect the connector: registration errors are logged and swallowed, and
 * the counter is updated independently of whether the MBean registered.
 */
public class CoordinatorMetrics implements CoordinatorMetricsMBean {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorMetrics.class);
  private static final String DOMAIN = "org.apache.iceberg.connect";

  // One instance (and MBean) per connector name, shared across that connector's tasks in this JVM.
  // Both the map and each instance's reference count are guarded by REGISTRY_LOCK.
  private static final Object REGISTRY_LOCK = new Object();
  private static final Map<String, CoordinatorMetrics> REGISTRY = Maps.newHashMap();

  private final String connectorName;
  private final ObjectName objectName;
  private final AtomicInteger activeCoordinators = new AtomicInteger();
  private int taskReferences;

  private CoordinatorMetrics(String connectorName, ObjectName objectName) {
    this.connectorName = connectorName;
    this.objectName = objectName;
  }

  /**
   * Attaches a task to its connector's metrics, registering the MBean on the first attach in this
   * JVM. Tasks of the same connector share one instance. Balance each call with {@link #detach()}.
   */
  static CoordinatorMetrics attach(String connectorName) {
    synchronized (REGISTRY_LOCK) {
      CoordinatorMetrics metrics =
          REGISTRY.computeIfAbsent(connectorName, CoordinatorMetrics::create);
      metrics.taskReferences++;
      return metrics;
    }
  }

  /**
   * Detaches a task from its connector's metrics, unregistering the MBean once the last task of the
   * connector in this JVM has detached.
   */
  void detach() {
    synchronized (REGISTRY_LOCK) {
      if (taskReferences == 0) {
        // Defensive: unbalanced detach. Nothing to release.
        return;
      }
      taskReferences--;
      if (taskReferences == 0) {
        REGISTRY.remove(connectorName);
        if (activeCoordinators.get() != 0) {
          LOG.warn(
              "Coordinator metrics for connector {} torn down with {} coordinator(s) still marked "
                  + "active; count was likely leaked by an abnormal shutdown",
              connectorName,
              activeCoordinators.get());
        }
        unregister();
      }
    }
  }

  /** Increments the active-coordinator gauge for this connector. */
  void coordinatorStarted() {
    activeCoordinators.incrementAndGet();
  }

  /** Decrements the active-coordinator gauge for this connector. */
  void coordinatorStopped() {
    activeCoordinators.decrementAndGet();
  }

  @VisibleForTesting
  int activeCoordinatorCount() {
    return activeCoordinators.get();
  }

  @Override
  public int getActiveCoordinatorCount() {
    return activeCoordinators.get();
  }

  private static CoordinatorMetrics create(String connectorName) {
    ObjectName objectName = null;
    try {
      objectName = objectName(connectorName);
    } catch (JMException e) {
      // Never let a metrics failure affect the connector; the gauge simply stays unavailable.
      LOG.warn(
          "Failed to build coordinator metrics object name for connector {}", connectorName, e);
    }

    CoordinatorMetrics metrics = new CoordinatorMetrics(connectorName, objectName);
    if (objectName != null) {
      try {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        if (!server.isRegistered(objectName)) {
          server.registerMBean(metrics, objectName);
          LOG.info("Registered coordinator metrics MBean {}", objectName);
        }
      } catch (JMException e) {
        LOG.warn("Failed to register coordinator metrics MBean {}", objectName, e);
      }
    }
    return metrics;
  }

  private void unregister() {
    if (objectName == null) {
      return;
    }
    try {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      if (server.isRegistered(objectName)) {
        server.unregisterMBean(objectName);
        LOG.info("Unregistered coordinator metrics MBean {}", objectName);
      }
    } catch (JMException e) {
      LOG.warn("Failed to unregister coordinator metrics MBean {}", objectName, e);
    }
  }

  @VisibleForTesting
  static ObjectName objectName(String connectorName) throws JMException {
    // Build the canonical name as a string (rather than via a Hashtable) and quote the connector
    // name: it may contain characters (comma, colon, =, space) illegal in an unquoted value.
    return new ObjectName(
        DOMAIN + ":type=CoordinatorMetrics,connector=" + ObjectName.quote(connectorName));
  }
}
