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

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enhanced Iceberg Sink Connector that automatically deploys HA coordinators
 * with leader election when connectors are created via the Connect REST API.
 *
 * This connector eliminates the need for an external Kubernetes operator by
 * directly deploying coordinator pods using the Kubernetes Java client.
 *
 * Key Features:
 * - High Availability: Deploys multiple coordinator replicas (default: 3)
 * - Leader Election: Uses Kubernetes leases for coordinator leadership
 * - Auto-scaling: Configurable replica count (1-5 replicas)
 * - Resource Management: Configurable CPU/memory limits and requests
 * - Health Checks: Built-in liveness and readiness probes
 * - Fault Tolerance: Automatic failover when leader fails
 *
 * Configuration Properties:
 *
 * Basic Configuration:
 * - iceberg.coordinator.replicas: Number of coordinator replicas (default: 3, range: 1-5)
 * - iceberg.coordinator.image: Docker image for coordinator pods (defaults to current Kafka Connect image)
 * - iceberg.coordinator.service-account: Kubernetes service account (default: default)
 *
 * Resource Configuration:
 * - iceberg.coordinator.memory.request: Memory request (default: 512Mi)
 * - iceberg.coordinator.memory.limit: Memory limit (default: 1Gi)
 * - iceberg.coordinator.cpu.request: CPU request (default: 200m)
 * - iceberg.coordinator.cpu.limit: CPU limit (default: 1000m)
 *
 * Deployment Configuration:
 * - iceberg.coordinator.strict-mode: Fail connector start if coordinator deployment fails (default: false)
 * - iceberg.coordinator.wait-for-readiness: Wait for coordinator readiness (default: true)
 * - iceberg.coordinator.readiness-timeout-seconds: Readiness check timeout (default: 120)
 *
 * Environment Variables:
 * - KUBERNETES_NAMESPACE: Target namespace for coordinator deployment (default: default)
 * - KAFKA_CONNECT_IMAGE: Default coordinator image (uses current Kafka Connect image)
 * - POD_NAME: Current pod name for image detection (set automatically in Kubernetes)
 *
 * Example Configuration:
 * {
 *   "name": "my-iceberg-connector",
 *   "config": {
 *     "connector.class": "org.apache.iceberg.connect.IcebergSinkConnectorV2",
 *     "tasks.max": "3",
 *     "topics": "my-topic",
 *     "iceberg.connect.group-id": "my-connector-group",
 *     "iceberg.control.topic": "iceberg-control-my-connector",
 *     "iceberg.catalog": "hadoop",
 *     "iceberg.catalog.type": "hadoop",
 *     "iceberg.catalog.warehouse": "s3://my-bucket/warehouse",
 *     "iceberg.tables": "my_namespace.my_table",
 *     "iceberg.coordinator.replicas": "3",
 *     "iceberg.coordinator.memory.limit": "2Gi",
 *     "iceberg.coordinator.cpu.limit": "2000m"
 *   }
 * }
 */
public class IcebergSinkConnectorV2 extends IcebergSinkConnector {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkConnectorV2.class);

    private CoordinatorDeploymentManager deploymentManager;
    private String connectorName;

    @Override
    public void start(Map<String, String> connectorProps) {
        // Initialize the deployment manager
        if (deploymentManager == null) {
            deploymentManager = new CoordinatorDeploymentManager();
        }

        try {
            // Validate configuration first
            LOG.info("Validating Iceberg connector configuration...");
            IcebergConnectorConfigValidator.validateAndRecommend(connectorProps);

            // Extract connector name from context or props
            connectorName = extractConnectorName(connectorProps);

            // Deploy coordinator first
            LOG.info("Auto-deploying coordinator for Iceberg connector: {}", connectorName);
            deploymentManager.deployCoordinator(connectorName, connectorProps);

            // Wait for coordinator to be ready
            boolean coordinatorReady = waitForCoordinatorReady(connectorName, connectorProps);
            if (!coordinatorReady) {
                LOG.warn("Coordinator readiness check failed for connector: {}. Proceeding anyway.", connectorName);
            }

            // Start the connector normally
            super.start(connectorProps);

            LOG.info("Iceberg connector {} started with coordinator deployed", connectorName);

        } catch (Exception e) {
            LOG.error("Failed to start Iceberg connector {} with coordinator", connectorName, e);
            // Don't fail the connector start if coordinator deployment fails in non-strict mode
            if (isStrictCoordinatorMode(connectorProps)) {
                throw new RuntimeException("Failed to deploy coordinator: " + e.getMessage(), e);
            } else {
                LOG.warn("Coordinator deployment failed but proceeding with connector start in non-strict mode");
                super.start(connectorProps);
            }
        }
    }

    @Override
    public void stop() {
        try {
            if (deploymentManager != null && connectorName != null) {
                LOG.info("Cleaning up coordinator for connector: {}", connectorName);
                deploymentManager.removeCoordinator(connectorName);
            }
        } catch (Exception e) {
            LOG.warn("Failed to cleanup coordinator during connector stop", e);
        } finally {
            super.stop();
        }
    }

    private boolean waitForCoordinatorReady(String connectorName, Map<String, String> props) {
        if (!shouldWaitForCoordinator(props)) {
            LOG.info("Coordinator readiness check disabled for connector: {}", connectorName);
            return true;
        }

        int timeoutSeconds = getCoordinatorReadinessTimeout(props);
        LOG.info("Waiting for coordinator readiness for connector: {} (timeout: {}s)",
                 connectorName, timeoutSeconds);

        try {
            boolean ready = deploymentManager.isCoordinatorReady(connectorName, timeoutSeconds);
            if (ready) {
                LOG.info("Coordinator is ready for connector: {}", connectorName);
                return true;
            } else {
                LOG.warn("Coordinator readiness check timed out for connector: {}", connectorName);
                return false;
            }
        } catch (Exception e) {
            LOG.error("Error during coordinator readiness check for connector: {}", connectorName, e);
            return false;
        }
    }

    private String extractConnectorName(Map<String, String> props) {
        // Try multiple ways to get connector name
        String name = props.get("name");
        if (name != null && !name.trim().isEmpty()) {
            return name;
        }

        // Try from group ID
        String groupId = props.get("iceberg.connect.group-id");
        if (groupId != null && !groupId.trim().isEmpty()) {
            return groupId.replaceAll("[^a-zA-Z0-9-]", "-");
        }

        // Try from control topic
        String controlTopic = props.get("iceberg.control.topic");
        if (controlTopic != null && !controlTopic.trim().isEmpty()) {
            return controlTopic.replaceAll("[^a-zA-Z0-9-]", "-");
        }

        throw new IllegalArgumentException("Cannot determine connector name from properties. " +
            "Please set 'name', 'iceberg.connect.group-id', or 'iceberg.control.topic'");
    }

    private boolean isStrictCoordinatorMode(Map<String, String> props) {
        String strict = props.get("iceberg.coordinator.strict-mode");
        return "true".equalsIgnoreCase(strict);
    }

    private boolean shouldWaitForCoordinator(Map<String, String> props) {
        String wait = props.get("iceberg.coordinator.wait-for-readiness");
        return !"false".equalsIgnoreCase(wait); // Default to true
    }

    private int getCoordinatorReadinessTimeout(Map<String, String> props) {
        String timeout = props.get("iceberg.coordinator.readiness-timeout-seconds");
        if (timeout != null) {
            try {
                return Integer.parseInt(timeout);
            } catch (NumberFormatException e) {
                LOG.warn("Invalid coordinator readiness timeout: {}. Using default.", timeout, e);
            }
        }
        return 120; // Default 2 minutes
    }
}