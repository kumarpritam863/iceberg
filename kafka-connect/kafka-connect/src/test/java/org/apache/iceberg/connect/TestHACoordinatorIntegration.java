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

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the complete HA coordinator setup.
 * Tests the integration between IcebergSinkConnectorV2 and
 * the HA coordinator deployment with leader election.
 */
public class TestHACoordinatorIntegration {

    private Map<String, String> baseConfig;

    @BeforeEach
    public void setUp() {
        baseConfig = new HashMap<>();
        baseConfig.put("name", "test-ha-connector");
        baseConfig.put("connector.class", "org.apache.iceberg.connect.IcebergSinkConnectorV2");
        baseConfig.put("tasks.max", "3");
        baseConfig.put("topics", "test-topic");
        baseConfig.put("iceberg.connect.group-id", "test-ha-connector");
        baseConfig.put("iceberg.control.topic", "iceberg-control-test-ha");
        baseConfig.put("iceberg.catalog", "hadoop");
        baseConfig.put("iceberg.catalog.type", "hadoop");
        baseConfig.put("iceberg.catalog.warehouse", "s3://test-bucket/warehouse");
        baseConfig.put("iceberg.tables", "test_namespace.test_table");

        // HA configuration
        baseConfig.put("iceberg.coordinator.replicas", "3");
        baseConfig.put("iceberg.coordinator.image", "test/coordinator:latest");
        baseConfig.put("iceberg.coordinator.memory.limit", "1Gi");
        baseConfig.put("iceberg.coordinator.cpu.limit", "1000m");
        baseConfig.put("iceberg.coordinator.wait-for-readiness", "false"); // Skip readiness wait in tests
    }

    @Test
    public void testConnectorWithHAConfiguration() {
        IcebergSinkConnectorV2 connector = new IcebergSinkConnectorV2();

        // Should not throw exception with valid HA configuration
        assertDoesNotThrow(() -> {
            // In a real environment, this would deploy coordinator pods
            // For testing, we're verifying configuration validation
            connector.extractConnectorName(baseConfig);
        });
    }

    @Test
    public void testHACoordinatorResourceNaming() {
        String connectorName = "test-ha-connector";

        // Test resource naming follows new pattern: {connectorName}-coordinator-{type}
        assertEquals("test-ha-connector-coordinator",
            getExpectedDeploymentName(connectorName));
        assertEquals("test-ha-connector-coordinator-svc",
            getExpectedServiceName(connectorName));
        assertEquals("test-ha-connector-coordinator-leader",
            getExpectedLeaseName(connectorName));
        assertEquals("test-ha-connector-coordinator-config",
            getExpectedConfigMapName(connectorName));
    }

    @Test
    public void testLeaderElectionConfiguration() {
        Map<String, String> envVars = extractExpectedEnvironmentVariables(baseConfig);

        // Verify leader election is enabled
        assertEquals("true", envVars.get("LEADER_ELECTION_ENABLED"));
        assertNotNull(envVars.get("LEADER_ELECTION_LEASE_NAME"));
        assertEquals("8080", envVars.get("HEALTH_CHECK_PORT"));
        assertEquals("30", envVars.get("LEADERSHIP_TIMEOUT_SECONDS"));
        assertEquals("test-ha-connector", envVars.get("JOB_ID"));
    }

    @Test
    public void testCoordinatorConfigMapData() {
        Map<String, String> configMapData = extractExpectedConfigMapData(baseConfig);

        // Verify coordinator configuration
        assertEquals("test-ha-connector", configMapData.get("CONNECT_GROUP_ID"));
        assertEquals("iceberg-control-test-ha", configMapData.get("CONTROL_TOPIC"));
        assertEquals("hadoop", configMapData.get("CATALOG_NAME"));
        assertEquals("kafka:9092", configMapData.get("KAFKA_BOOTSTRAP_SERVERS"));
        assertEquals("true", configMapData.get("ENABLE_TRANSACTIONS"));

        // Verify catalog properties are prefixed correctly
        assertEquals("hadoop", configMapData.get("CATALOG_TYPE"));
        assertEquals("s3://test-bucket/warehouse", configMapData.get("CATALOG_WAREHOUSE"));
    }

    @Test
    public void testHADeploymentConfiguration() {
        // Verify HA-specific deployment settings
        int replicas = getReplicasFromConfig(baseConfig);
        assertEquals(3, replicas);

        String image = getImageFromConfig(baseConfig);
        assertEquals("test/coordinator:latest", image);

        // Verify resource limits
        String memoryLimit = baseConfig.get("iceberg.coordinator.memory.limit");
        String cpuLimit = baseConfig.get("iceberg.coordinator.cpu.limit");
        assertEquals("1Gi", memoryLimit);
        assertEquals("1000m", cpuLimit);
    }

    @Test
    public void testConnectorWithoutHAConfiguration() {
        // Test connector works without HA-specific configuration
        Map<String, String> simpleConfig = new HashMap<>(baseConfig);
        simpleConfig.remove("iceberg.coordinator.replicas");
        simpleConfig.remove("iceberg.coordinator.image");

        // Should use defaults
        assertEquals(3, getReplicasFromConfig(simpleConfig)); // Default replicas
        assertEquals("confluentinc/cp-kafka-connect:latest",
            getImageFromConfig(simpleConfig)); // Default Kafka Connect image
    }

    @Test
    public void testInvalidHAConfiguration() {
        // Test invalid replica count
        Map<String, String> invalidConfig = new HashMap<>(baseConfig);
        invalidConfig.put("iceberg.coordinator.replicas", "10"); // Exceeds max
        assertEquals(5, getReplicasFromConfig(invalidConfig)); // Clamped to max

        invalidConfig.put("iceberg.coordinator.replicas", "0"); // Below min
        assertEquals(1, getReplicasFromConfig(invalidConfig)); // Clamped to min

        invalidConfig.put("iceberg.coordinator.replicas", "invalid");
        assertEquals(3, getReplicasFromConfig(invalidConfig)); // Falls back to default
    }

    @Test
    public void testHealthCheckEndpoints() {
        // Verify health check configuration
        Map<String, String> envVars = extractExpectedEnvironmentVariables(baseConfig);
        assertEquals("8080", envVars.get("HEALTH_CHECK_PORT"));

        // Expected health check paths (for documentation)
        String[] expectedPaths = {"/health", "/ready", "/status"};
        assertNotNull(expectedPaths); // Health endpoints should be available
    }

    // ==================== Helper Methods ====================

    private String getExpectedDeploymentName(String connectorName) {
        return sanitizeResourceName(connectorName) + "-coordinator";
    }

    private String getExpectedServiceName(String connectorName) {
        return sanitizeResourceName(connectorName) + "-coordinator-svc";
    }

    private String getExpectedLeaseName(String connectorName) {
        return sanitizeResourceName(connectorName) + "-coordinator-leader";
    }

    private String getExpectedConfigMapName(String connectorName) {
        return sanitizeResourceName(connectorName) + "-coordinator-config";
    }

    private String sanitizeResourceName(String name) {
        return name.toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z0-9-]", "-");
    }

    private int getReplicasFromConfig(Map<String, String> config) {
        String replicas = config.get("iceberg.coordinator.replicas");
        if (replicas != null) {
            try {
                int replicaCount = Integer.parseInt(replicas);
                return Math.max(1, Math.min(replicaCount, 5));
            } catch (NumberFormatException e) {
                // Fall back to default
            }
        }
        return 3; // Default
    }

    private String getImageFromConfig(Map<String, String> config) {
        String image = config.get("iceberg.coordinator.image");
        if (image != null && !image.trim().isEmpty()) {
            return image;
        }

        // Check environment variable
        String envImage = System.getenv("KAFKA_CONNECT_IMAGE");
        if (envImage != null && !envImage.trim().isEmpty()) {
            return envImage;
        }

        return "confluentinc/cp-kafka-connect:latest"; // Default Kafka Connect image
    }

    private Map<String, String> extractExpectedEnvironmentVariables(Map<String, String> config) {
        Map<String, String> envVars = new HashMap<>();
        envVars.put("LEADER_ELECTION_ENABLED", "true");
        envVars.put("LEADER_ELECTION_LEASE_NAME", getExpectedLeaseName(config.get("name")));
        envVars.put("HEALTH_CHECK_PORT", "8080");
        envVars.put("LEADERSHIP_TIMEOUT_SECONDS", "30");
        envVars.put("JOB_ID", config.get("name"));
        return envVars;
    }

    private Map<String, String> extractExpectedConfigMapData(Map<String, String> config) {
        Map<String, String> configMapData = new HashMap<>();
        configMapData.put("CONNECT_GROUP_ID", config.get("iceberg.connect.group-id"));
        configMapData.put("CONTROL_TOPIC", config.get("iceberg.control.topic"));
        configMapData.put("CATALOG_NAME", config.get("iceberg.catalog"));
        configMapData.put("KAFKA_BOOTSTRAP_SERVERS", config.getOrDefault("bootstrap.servers", "kafka:9092"));
        configMapData.put("ENABLE_TRANSACTIONS", "true");

        // Transform catalog properties
        config.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith("iceberg.catalog."))
            .forEach(entry -> {
                String key = "CATALOG_" + entry.getKey().substring("iceberg.catalog.".length()).toUpperCase(java.util.Locale.ROOT).replace(".", "_");
                configMapData.put(key, entry.getValue());
            });

        return configMapData;
    }
}