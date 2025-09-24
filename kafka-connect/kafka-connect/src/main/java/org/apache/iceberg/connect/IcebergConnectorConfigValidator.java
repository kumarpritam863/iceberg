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
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comprehensive configuration validator for Iceberg connector
 * with automatic coordinator deployment.
 */
public class IcebergConnectorConfigValidator {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergConnectorConfigValidator.class);

    // Required configuration keys
    private static final String GROUP_ID_KEY = "iceberg.connect.group-id";
    private static final String CONTROL_TOPIC_KEY = "iceberg.control.topic";
    private static final String CATALOG_NAME_KEY = "iceberg.catalog";

    // Coordinator configuration keys
    private static final String COORDINATOR_STRICT_MODE = "iceberg.coordinator.strict-mode";
    private static final String COORDINATOR_WAIT_FOR_READINESS = "iceberg.coordinator.wait-for-readiness";
    private static final String COORDINATOR_READINESS_TIMEOUT = "iceberg.coordinator.readiness-timeout-seconds";
    private static final String COORDINATOR_IMAGE = "iceberg.coordinator.image";

    // Validation patterns
    private static final Pattern VALID_GROUP_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
    private static final Pattern VALID_TOPIC_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");

    /**
     * Validates the complete Iceberg connector configuration.
     *
     * @param config The connector configuration map
     * @throws ConfigException if validation fails
     */
    public static void validateConfiguration(Map<String, String> config) {
        List<String> errors = new ArrayList<>();

        // Validate required core configuration
        validateRequiredConfig(config, errors);

        // Validate Iceberg catalog configuration
        validateCatalogConfig(config, errors);

        // Validate coordinator configuration
        validateCoordinatorConfig(config, errors);

        // Validate Kafka configuration
        validateKafkaConfig(config, errors);

        // Validate naming conventions
        validateNamingConventions(config, errors);

        // Validate timeout and numeric configurations
        validateNumericConfig(config, errors);

        if (!errors.isEmpty()) {
            String errorMessage = "Configuration validation failed:\n" + String.join("\n", errors);
            LOG.error("Configuration validation failed for connector: {}", errorMessage);
            throw new ConfigException(errorMessage);
        }

        LOG.info("Configuration validation passed for Iceberg connector");
    }

    private static void validateRequiredConfig(Map<String, String> config, List<String> errors) {
        // Check required fields for coordinator deployment
        if (isBlank(config.get(GROUP_ID_KEY))) {
            errors.add("'" + GROUP_ID_KEY + "' is required for automatic coordinator deployment");
        }

        if (isBlank(config.get(CONTROL_TOPIC_KEY))) {
            errors.add("'" + CONTROL_TOPIC_KEY + "' is required for automatic coordinator deployment");
        }

        if (isBlank(config.get(CATALOG_NAME_KEY))) {
            errors.add("'" + CATALOG_NAME_KEY + "' is required for automatic coordinator deployment");
        }

        // Check connector name can be determined
        if (isBlank(config.get("name")) &&
            isBlank(config.get(GROUP_ID_KEY)) &&
            isBlank(config.get(CONTROL_TOPIC_KEY))) {
            errors.add("Cannot determine connector name. Please set 'name', '" + GROUP_ID_KEY + "', or '" + CONTROL_TOPIC_KEY + "'");
        }
    }

    private static void validateCatalogConfig(Map<String, String> config, List<String> errors) {
        // Check catalog properties exist
        boolean hasCatalogImpl = config.keySet().stream()
            .anyMatch(key -> key.equals("iceberg.catalog.catalog-impl"));

        if (!hasCatalogImpl) {
            errors.add("'iceberg.catalog.catalog-impl' is required to specify the catalog implementation");
        }

        // Check warehouse location
        boolean hasWarehouse = config.keySet().stream()
            .anyMatch(key -> key.equals("iceberg.catalog.warehouse"));

        if (!hasWarehouse) {
            errors.add("'iceberg.catalog.warehouse' is required to specify the warehouse location");
        }

        // Validate catalog-specific properties
        String catalogImpl = config.get("iceberg.catalog.catalog-impl");
        if (catalogImpl != null) {
            validateCatalogSpecificConfig(catalogImpl, config, errors);
        }
    }

    private static void validateCatalogSpecificConfig(String catalogImpl, Map<String, String> config, List<String> errors) {
        switch (catalogImpl) {
            case "org.apache.iceberg.jdbc.JdbcCatalog":
                if (isBlank(config.get("iceberg.catalog.uri"))) {
                    errors.add("'iceberg.catalog.uri' is required for JDBC catalog");
                }
                if (isBlank(config.get("iceberg.catalog.jdbc.user"))) {
                    errors.add("'iceberg.catalog.jdbc.user' is required for JDBC catalog");
                }
                if (isBlank(config.get("iceberg.catalog.jdbc.password"))) {
                    errors.add("'iceberg.catalog.jdbc.password' is required for JDBC catalog");
                }
                break;

            case "org.apache.iceberg.hive.HiveCatalog":
                if (isBlank(config.get("iceberg.catalog.uri"))) {
                    errors.add("'iceberg.catalog.uri' is required for Hive catalog");
                }
                break;

            case "org.apache.iceberg.rest.RESTCatalog":
                if (isBlank(config.get("iceberg.catalog.uri"))) {
                    errors.add("'iceberg.catalog.uri' is required for REST catalog");
                }
                break;

            default:
                LOG.warn("Unknown catalog implementation: {}. Skipping specific validation.", catalogImpl);
        }
    }

    private static void validateCoordinatorConfig(Map<String, String> config, List<String> errors) {
        // Validate strict mode
        String strictMode = config.get(COORDINATOR_STRICT_MODE);
        if (strictMode != null && !isValidBoolean(strictMode)) {
            errors.add("'" + COORDINATOR_STRICT_MODE + "' must be 'true' or 'false'");
        }

        // Validate wait for readiness
        String waitForReadiness = config.get(COORDINATOR_WAIT_FOR_READINESS);
        if (waitForReadiness != null && !isValidBoolean(waitForReadiness)) {
            errors.add("'" + COORDINATOR_WAIT_FOR_READINESS + "' must be 'true' or 'false'");
        }

        // Validate coordinator image format
        String coordinatorImage = config.get(COORDINATOR_IMAGE);
        if (coordinatorImage != null && !isValidImageName(coordinatorImage)) {
            errors.add("'" + COORDINATOR_IMAGE + "' must be a valid container image name (e.g., registry/image:tag)");
        }
    }

    private static void validateKafkaConfig(Map<String, String> config, List<String> errors) {
        // Validate bootstrap servers
        String bootstrapServers = config.get("bootstrap.servers");
        if (isBlank(bootstrapServers)) {
            errors.add("'bootstrap.servers' is required for Kafka connectivity");
        } else if (!isValidBootstrapServers(bootstrapServers)) {
            errors.add("'bootstrap.servers' format is invalid. Expected format: 'host1:port1,host2:port2'");
        }

        // Validate tasks.max
        String tasksMax = config.get("tasks.max");
        if (tasksMax != null) {
            try {
                int maxTasks = Integer.parseInt(tasksMax);
                if (maxTasks <= 0) {
                    errors.add("'tasks.max' must be greater than 0");
                }
                if (maxTasks > 100) {
                    LOG.warn("'tasks.max' is set to {}, which is quite high. Consider if this is intentional.", maxTasks);
                }
            } catch (NumberFormatException e) {
                errors.add("'tasks.max' must be a valid integer");
            }
        }
    }

    private static void validateNamingConventions(Map<String, String> config, List<String> errors) {
        // Validate group ID format
        String groupId = config.get(GROUP_ID_KEY);
        if (groupId != null && !VALID_GROUP_ID_PATTERN.matcher(groupId).matches()) {
            errors.add("'" + GROUP_ID_KEY + "' contains invalid characters. Only alphanumeric, dots, underscores, and hyphens are allowed");
        }

        // Validate control topic format
        String controlTopic = config.get(CONTROL_TOPIC_KEY);
        if (controlTopic != null && !VALID_TOPIC_NAME_PATTERN.matcher(controlTopic).matches()) {
            errors.add("'" + CONTROL_TOPIC_KEY + "' contains invalid characters. Only alphanumeric, dots, underscores, and hyphens are allowed");
        }

        // Validate topic names
        String topics = config.get("topics");
        if (topics != null) {
            String[] topicArray = topics.split(",", -1);
            for (String topic : topicArray) {
                topic = topic.trim();
                if (!VALID_TOPIC_NAME_PATTERN.matcher(topic).matches()) {
                    errors.add("Topic '" + topic + "' contains invalid characters. Only alphanumeric, dots, underscores, and hyphens are allowed");
                }
            }
        }
    }

    private static void validateNumericConfig(Map<String, String> config, List<String> errors) {
        // Validate readiness timeout
        String readinessTimeout = config.get(COORDINATOR_READINESS_TIMEOUT);
        if (readinessTimeout != null) {
            try {
                int timeout = Integer.parseInt(readinessTimeout);
                if (timeout <= 0) {
                    errors.add("'" + COORDINATOR_READINESS_TIMEOUT + "' must be greater than 0");
                }
                if (timeout > 3600) {
                    errors.add("'" + COORDINATOR_READINESS_TIMEOUT + "' should not exceed 3600 seconds (1 hour)");
                }
            } catch (NumberFormatException e) {
                errors.add("'" + COORDINATOR_READINESS_TIMEOUT + "' must be a valid integer");
            }
        }

        // Validate commit interval
        String commitInterval = config.get("iceberg.control.commit.interval-ms");
        if (commitInterval != null) {
            try {
                long interval = Long.parseLong(commitInterval);
                if (interval <= 0) {
                    errors.add("'iceberg.control.commit.interval-ms' must be greater than 0");
                }
                if (interval < 1000) {
                    LOG.warn("'iceberg.control.commit.interval-ms' is set to {}ms, which is very frequent. Consider increasing for better performance.", interval);
                }
            } catch (NumberFormatException e) {
                errors.add("'iceberg.control.commit.interval-ms' must be a valid long integer");
            }
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static boolean isValidBoolean(String value) {
        return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
    }

    private static boolean isValidImageName(String imageName) {
        // Basic validation for container image name format
        return imageName.matches("^[a-zA-Z0-9._/-]+:[a-zA-Z0-9._-]+$") ||
               imageName.matches("^[a-zA-Z0-9._/-]+$"); // Allow without tag
    }

    private static boolean isValidBootstrapServers(String servers) {
        if (isBlank(servers)) {
            return false;
        }

        String[] serverArray = servers.split(",", -1);
        for (String server : serverArray) {
            server = server.trim();
            if (!server.matches("^[a-zA-Z0-9.-]+:\\d+$")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validates configuration and provides recommendations.
     */
    public static void validateAndRecommend(Map<String, String> config) {
        validateConfiguration(config);

        // Provide recommendations
        List<String> recommendations = new ArrayList<>();

        String strictMode = config.get(COORDINATOR_STRICT_MODE);
        if (!"true".equalsIgnoreCase(strictMode)) {
            recommendations.add("Consider enabling '" + COORDINATOR_STRICT_MODE + "=true' for production deployments to ensure coordinators are ready before processing data");
        }

        String readinessTimeout = config.get(COORDINATOR_READINESS_TIMEOUT);
        if (readinessTimeout == null) {
            recommendations.add("Consider setting '" + COORDINATOR_READINESS_TIMEOUT + "' to control how long to wait for coordinator readiness");
        }

        if (!recommendations.isEmpty()) {
            LOG.info("Configuration recommendations:\n{}", String.join("\n", recommendations));
        }
    }
}