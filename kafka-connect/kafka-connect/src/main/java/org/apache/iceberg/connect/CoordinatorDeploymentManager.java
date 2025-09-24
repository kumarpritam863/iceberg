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

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CoordinationV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HA Coordinator deployment manager that directly deploys coordinator pods
 * with leader election, eliminating the need for an external operator.
 */
public class CoordinatorDeploymentManager {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorDeploymentManager.class);

    private static final String ICEBERG_CONNECTOR_CLASS = "org.apache.iceberg.connect.IcebergSinkConnector";
    private static final String COORDINATOR_MAIN_CLASS = "org.apache.iceberg.connect.coordinator.IcebergJobManagerApplication";

    private final AppsV1Api appsV1Api;
    private final CoreV1Api coreV1Api;
    private final CoordinationV1Api coordinationV1Api;
    private final String namespace;

    public CoordinatorDeploymentManager() {
        try {
            // Initialize Kubernetes API client (works both in-cluster and out-of-cluster)
            ApiClient apiClient = Config.defaultClient();
            Configuration.setDefaultApiClient(apiClient);

            this.appsV1Api = new AppsV1Api(apiClient);
            this.coreV1Api = new CoreV1Api(apiClient);
            this.coordinationV1Api = new CoordinationV1Api(apiClient);

            // Get namespace - try environment first, then auto-detect from current pod
            String envNamespace = System.getenv("KUBERNETES_NAMESPACE");
            if (envNamespace != null && !envNamespace.trim().isEmpty()) {
                this.namespace = envNamespace;
            } else {
                this.namespace = getCurrentNamespace();
            }

            LOG.info("HA Coordinator deployment manager initialized for namespace: {}", namespace);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize Kubernetes client", e);
        }
    }

    private String getCurrentNamespace() {
        try {
            // Try to read namespace from service account token (when running in-cluster)
            String tokenNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
            java.nio.file.Path path = java.nio.file.Paths.get(tokenNamespacePath);
            if (java.nio.file.Files.exists(path)) {
                return java.nio.file.Files.readString(path).trim();
            }
        } catch (Exception e) {
            LOG.debug("Could not read namespace from service account token: {}", e.getMessage(), e);
        }

        try {
            // Fallback: try to get from current pod if POD_NAME is available
            String podName = System.getenv("POD_NAME");
            if (podName != null && !podName.trim().isEmpty()) {
                // Try with default namespace first
                V1Pod pod = coreV1Api.readNamespacedPod(podName, "default", null);
                if (pod.getMetadata() != null && pod.getMetadata().getNamespace() != null) {
                    return pod.getMetadata().getNamespace();
                }
            }
        } catch (Exception e) {
            LOG.debug("Could not detect namespace from current pod: {}", e.getMessage(), e);
        }

        // Final fallback
        LOG.warn("Could not auto-detect Kubernetes namespace, using 'default'");
        return "default";
    }

    /**
     * Deploys HA coordinator pods for the given connector configuration.
     */
    public void deployCoordinator(String connectorName, Map<String, String> config) {
        String connectorClass = config.get("connector.class");
        if (!ICEBERG_CONNECTOR_CLASS.equals(connectorClass) &&
            !"org.apache.iceberg.connect.IcebergSinkConnectorV2".equals(connectorClass)) {
            LOG.debug("Skipping coordinator deployment for non-Iceberg connector: {}", connectorName);
            return;
        }

        LOG.info("Deploying HA coordinator for Iceberg connector: {}", connectorName);

        try {
            validateIcebergConfig(config);

            // Get HA configuration
            int replicas = getCoordinatorReplicas(config);
            String coordinatorImage = getCoordinatorImage(config);

            // Create leader election lease
            createLeaderElectionLease(connectorName);

            // Create ConfigMap with coordinator configuration
            createCoordinatorConfigMap(connectorName, config);

            // Create Service for coordinator pods
            createCoordinatorService(connectorName);

            // Create Deployment with multiple replicas and leader election
            createCoordinatorDeployment(connectorName, config, replicas, coordinatorImage);

            LOG.info("HA coordinator deployment created for connector: {} with {} replicas",
                    connectorName, replicas);

        } catch (Exception e) {
            LOG.error("Failed to deploy HA coordinator for connector: {}", connectorName, e);
            throw new ConfigException("Failed to deploy HA coordinator: " + e.getMessage(), e);
        }
    }

    /**
     * Removes coordinator deployment for the given connector.
     */
    public void removeCoordinator(String connectorName) {
        try {
            LOG.info("Removing HA coordinator for connector: {}", connectorName);

            String deploymentName = getCoordinatorDeploymentName(connectorName);
            String serviceName = getCoordinatorServiceName(connectorName);
            String configMapName = getCoordinatorConfigMapName(connectorName);
            String leaseName = getLeaderElectionLeaseName(connectorName);

            // Delete deployment
            try {
                appsV1Api.deleteNamespacedDeployment(deploymentName, namespace, null, null, null, null, null, null);
                LOG.info("Deleted deployment: {}", deploymentName);
            } catch (ApiException e) {
                if (e.getCode() != 404) {
                    LOG.warn("Failed to delete deployment {}: {}", deploymentName, e.getMessage(), e);
                }
            }

            // Delete service
            try {
                coreV1Api.deleteNamespacedService(serviceName, namespace, null, null, null, null, null, null);
                LOG.info("Deleted service: {}", serviceName);
            } catch (ApiException e) {
                if (e.getCode() != 404) {
                    LOG.warn("Failed to delete service {}: {}", serviceName, e.getMessage(), e);
                }
            }

            // Delete configmap
            try {
                coreV1Api.deleteNamespacedConfigMap(configMapName, namespace, null, null, null, null, null, null);
                LOG.info("Deleted configmap: {}", configMapName);
            } catch (ApiException e) {
                if (e.getCode() != 404) {
                    LOG.warn("Failed to delete configmap {}: {}", configMapName, e.getMessage(), e);
                }
            }

            // Delete leader election lease
            try {
                coordinationV1Api.deleteNamespacedLease(leaseName, namespace, null, null, null, null, null, null);
                LOG.info("Deleted lease: {}", leaseName);
            } catch (ApiException e) {
                if (e.getCode() != 404) {
                    LOG.warn("Failed to delete lease {}: {}", leaseName, e.getMessage(), e);
                }
            }

            LOG.info("HA coordinator resources removed for connector: {}", connectorName);
        } catch (Exception e) {
            LOG.warn("Failed to remove HA coordinator for connector: {}", connectorName, e);
        }
    }

    /**
     * Checks if coordinator is ready for the given connector.
     */
    public boolean isCoordinatorReady(String connectorName, int timeoutSeconds) {
        LOG.info("Checking HA coordinator readiness for connector: {} (timeout: {}s)",
                 connectorName, timeoutSeconds);

        String deploymentName = getCoordinatorDeploymentName(connectorName);
        long endTime = System.currentTimeMillis() + (timeoutSeconds * 1000L);

        while (System.currentTimeMillis() < endTime) {
            try {
                V1Deployment deployment = appsV1Api.readNamespacedDeployment(
                    deploymentName, namespace, null);

                V1DeploymentStatus status = deployment.getStatus();
                if (status != null && status.getReadyReplicas() != null &&
                    status.getReadyReplicas() > 0) {
                    LOG.info("HA coordinator is ready for connector: {} ({} replicas ready)",
                            connectorName, status.getReadyReplicas());
                    return true;
                }

                // Wait before next check
                TimeUnit.SECONDS.sleep(2);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for coordinator readiness: {}", connectorName, e);
                return false;
            } catch (Exception e) {
                LOG.debug("Error checking coordinator status for {}: {}", connectorName, e.getMessage(), e);
            }
        }

        LOG.warn("HA coordinator readiness check timed out for connector: {}", connectorName);
        return false;
    }

    // ==================== Helper Methods ====================

    private int getCoordinatorReplicas(Map<String, String> config) {
        String replicas = config.get("iceberg.coordinator.replicas");
        if (replicas != null) {
            try {
                int replicaCount = Integer.parseInt(replicas);
                return Math.max(1, Math.min(replicaCount, 5)); // Limit between 1-5 replicas
            } catch (NumberFormatException e) {
                LOG.warn("Invalid coordinator replicas: {}. Using default.", replicas, e);
            }
        }
        return 3; // Default to 3 replicas for HA
    }

    private String getCoordinatorImage(Map<String, String> config) {
        // Use the same image as the current Kafka Connect cluster
        String image = config.get("iceberg.coordinator.image");
        if (image != null && !image.trim().isEmpty()) {
            return image;
        }

        // Try to detect the current Kafka Connect image from environment
        image = System.getenv("KAFKA_CONNECT_IMAGE");
        if (image != null && !image.trim().isEmpty()) {
            return image;
        }

        // Fallback to trying to detect from current pod (if running in Kubernetes)
        try {
            String currentImage = getCurrentPodImage();
            if (currentImage != null && !currentImage.trim().isEmpty()) {
                return currentImage;
            }
        } catch (Exception e) {
            LOG.debug("Could not detect current pod image: {}", e.getMessage(), e);
            throw new ConnectException("Couldn't find coordinator image for launching coordinator");
        }
        throw new ConnectException("Couldn't find coordinator image for launching coordinator");
    }

    private String getCurrentPodImage() {
        try {
            // Try to get current pod image from Kubernetes API
            String podName = System.getenv("POD_NAME");
            String namespace = System.getenv("KUBERNETES_NAMESPACE");

            if (podName != null && namespace != null) {
                V1Pod pod = coreV1Api.readNamespacedPod(podName, namespace, null);
                if (pod.getSpec() != null && pod.getSpec().getContainers() != null &&
                    !pod.getSpec().getContainers().isEmpty()) {
                    return pod.getSpec().getContainers().get(0).getImage();
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to get current pod image from Kubernetes API: {}", e.getMessage(), e);
        }
        return null;
    }

    private void createLeaderElectionLease(String connectorName) throws ApiException {
        String leaseName = getLeaderElectionLeaseName(connectorName);

        V1Lease lease = new V1Lease()
            .metadata(new V1ObjectMeta()
                .name(leaseName)
                .namespace(namespace)
                .labels(Map.of(
                    "app", "iceberg-coordinator",
                    "connector", connectorName,
                    "component", "leader-election"
                )))
            .spec(new V1LeaseSpec()
                .holderIdentity("") // Will be set by leader election process
                .leaseDurationSeconds(15)
                .renewTime(OffsetDateTime.now(java.time.ZoneOffset.UTC)));

        try {
            coordinationV1Api.createNamespacedLease(namespace, lease, null, null, null, null);
            LOG.info("Created leader election lease: {}", leaseName);
        } catch (ApiException e) {
            if (e.getCode() != 409) {
                throw e;
            }
        }
    }

    private void createCoordinatorConfigMap(String connectorName, Map<String, String> config) throws ApiException {
        String configMapName = getCoordinatorConfigMapName(connectorName);

        // Extract relevant configuration for coordinator
        Map<String, String> coordinatorConfig = new HashMap<>();
        coordinatorConfig.put("CONNECT_GROUP_ID", config.get("iceberg.connect.group-id"));
        coordinatorConfig.put("CONTROL_TOPIC", config.get("iceberg.control.topic"));
        coordinatorConfig.put("CATALOG_NAME", config.get("iceberg.catalog"));
        coordinatorConfig.put("KAFKA_BOOTSTRAP_SERVERS", config.getOrDefault("bootstrap.servers", "kafka:9092"));
        coordinatorConfig.put("COMMIT_INTERVAL_MINUTES", String.valueOf(
            Integer.parseInt(config.getOrDefault("iceberg.control.commit.interval-ms", "300000")) / 60000));
        coordinatorConfig.put("COMMIT_TIMEOUT_MINUTES", String.valueOf(
            Integer.parseInt(config.getOrDefault("iceberg.control.commit.timeout-ms", "30000")) / 60000));
        coordinatorConfig.put("COMMIT_THREADS", config.getOrDefault("iceberg.control.commit.threads",
            String.valueOf(Runtime.getRuntime().availableProcessors() * 2)));
        coordinatorConfig.put("ENABLE_TRANSACTIONS", "true");
        coordinatorConfig.put("KEEPALIVE_TIMEOUT_MS", "60000");

        // Add catalog properties with CATALOG_ prefix
        config.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith("iceberg.catalog."))
            .forEach(entry -> {
                String key = "CATALOG_" + entry.getKey().substring("iceberg.catalog.".length()).toUpperCase(java.util.Locale.ROOT).replace(".", "_");
                coordinatorConfig.put(key, entry.getValue());
            });

        V1ConfigMap configMap = new V1ConfigMap()
            .metadata(new V1ObjectMeta()
                .name(configMapName)
                .namespace(namespace)
                .labels(Map.of(
                    "app", "iceberg-coordinator",
                    "connector", connectorName,
                    "component", "config"
                )))
            .data(coordinatorConfig);

        try {
            coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null, null);
            LOG.info("Created coordinator ConfigMap: {}", configMapName);
        } catch (ApiException e) {
            if (e.getCode() == 409) {
                // Update existing configmap
                coreV1Api.replaceNamespacedConfigMap(configMapName, namespace, configMap, null, null, null, null);
            } else {
                throw e;
            }
        }
    }

    private void createCoordinatorService(String connectorName) throws ApiException {
        String serviceName = getCoordinatorServiceName(connectorName);

        V1Service service = new V1Service()
            .metadata(new V1ObjectMeta()
                .name(serviceName)
                .namespace(namespace)
                .labels(Map.of(
                    "app", "iceberg-coordinator",
                    "connector", connectorName,
                    "component", "service"
                )))
            .spec(new V1ServiceSpec()
                .selector(Map.of(
                    "app", "iceberg-coordinator",
                    "connector", connectorName
                ))
                .ports(List.of(
                    new V1ServicePort()
                        .name("http")
                        .port(8080)
                        .targetPort(new IntOrString(8080))
                        .protocol("TCP")
                )));

        try {
            coreV1Api.createNamespacedService(namespace, service, null, null, null, null);
            LOG.info("Created coordinator Service: {}", serviceName);
        } catch (ApiException e) {
            if (e.getCode() != 409) {
                throw e;
            }
        }
    }

    private void validateIcebergConfig(Map<String, String> config) {
        String groupId = config.get("iceberg.connect.group-id");
        if (groupId == null || groupId.trim().isEmpty()) {
            throw new ConfigException("iceberg.connect.group-id is required for HA coordinator deployment");
        }

        String controlTopic = config.get("iceberg.control.topic");
        if (controlTopic == null || controlTopic.trim().isEmpty()) {
            throw new ConfigException("iceberg.control.topic is required for HA coordinator deployment");
        }

        String catalogName = config.get("iceberg.catalog");
        if (catalogName == null || catalogName.trim().isEmpty()) {
            throw new ConfigException("iceberg.catalog is required for HA coordinator deployment");
        }

        boolean hasCatalogProps = config.keySet().stream()
            .anyMatch(key -> key.startsWith("iceberg.catalog."));
        if (!hasCatalogProps) {
            throw new ConfigException("Catalog properties (iceberg.catalog.*) are required for HA coordinator deployment");
        }
    }

    private void createCoordinatorDeployment(String connectorName, Map<String, String> config,
                                           int replicas, String coordinatorImage) throws ApiException {
        String deploymentName = getCoordinatorDeploymentName(connectorName);
        String configMapName = getCoordinatorConfigMapName(connectorName);
        String leaseName = getLeaderElectionLeaseName(connectorName);

        V1Deployment deployment = new V1Deployment()
            .metadata(new V1ObjectMeta()
                .name(deploymentName)
                .namespace(namespace)
                .labels(Map.of(
                    "app", "iceberg-coordinator",
                    "connector", connectorName,
                    "component", "coordinator"
                )))
            .spec(new V1DeploymentSpec()
                .replicas(replicas)
                .selector(new V1LabelSelector()
                    .matchLabels(Map.of(
                        "app", "iceberg-coordinator",
                        "connector", connectorName
                    )))
                .template(new V1PodTemplateSpec()
                    .metadata(new V1ObjectMeta()
                        .labels(Map.of(
                            "app", "iceberg-coordinator",
                            "connector", connectorName
                        )))
                    .spec(new V1PodSpec()
                        .containers(List.of(
                            new V1Container()
                                .name("coordinator")
                                .image(coordinatorImage)
                                .command(List.of("java"))
                                .args(List.of(
                                    "-cp", "/usr/share/java/kafka-connect-iceberg/*:/usr/share/java/kafka/*",
                                    "-Xmx1g", "-Xms512m",
                                    COORDINATOR_MAIN_CLASS
                                ))
                                .ports(List.of(
                                    new V1ContainerPort()
                                        .containerPort(8080)
                                        .name("http")
                                ))
                                .env(List.of(
                                    new V1EnvVar()
                                        .name("JAVA_OPTS")
                                        .value("-Xmx1g -Xms512m"),
                                    new V1EnvVar()
                                        .name("KUBERNETES_NAMESPACE")
                                        .valueFrom(new V1EnvVarSource()
                                            .fieldRef(new V1ObjectFieldSelector()
                                                .fieldPath("metadata.namespace"))),
                                    new V1EnvVar()
                                        .name("POD_NAME")
                                        .valueFrom(new V1EnvVarSource()
                                            .fieldRef(new V1ObjectFieldSelector()
                                                .fieldPath("metadata.name"))),
                                    new V1EnvVar()
                                        .name("LEADER_ELECTION_ENABLED")
                                        .value("true"),
                                    new V1EnvVar()
                                        .name("LEADER_ELECTION_LEASE_NAME")
                                        .value(leaseName),
                                    new V1EnvVar()
                                        .name("HEALTH_CHECK_PORT")
                                        .value("8080"),
                                    new V1EnvVar()
                                        .name("LEADERSHIP_TIMEOUT_SECONDS")
                                        .value("30"),
                                    new V1EnvVar()
                                        .name("JOB_ID")
                                        .value(connectorName)
                                ))
                                .envFrom(List.of(
                                    new V1EnvFromSource()
                                        .configMapRef(new V1ConfigMapEnvSource()
                                            .name(configMapName))
                                ))
                                .livenessProbe(new V1Probe()
                                    .httpGet(new V1HTTPGetAction()
                                        .path("/health")
                                        .port(new IntOrString(8080)))
                                    .initialDelaySeconds(30)
                                    .periodSeconds(10))
                                .readinessProbe(new V1Probe()
                                    .httpGet(new V1HTTPGetAction()
                                        .path("/ready")
                                        .port(new IntOrString(8080)))
                                    .initialDelaySeconds(10)
                                    .periodSeconds(5))
                                .resources(new V1ResourceRequirements()
                                    .requests(Map.of(
                                        "memory", Quantity.fromString(config.getOrDefault("iceberg.coordinator.memory.request", "512Mi")),
                                        "cpu", Quantity.fromString(config.getOrDefault("iceberg.coordinator.cpu.request", "200m"))
                                    ))
                                    .limits(Map.of(
                                        "memory", Quantity.fromString(config.getOrDefault("iceberg.coordinator.memory.limit", "1Gi")),
                                        "cpu", Quantity.fromString(config.getOrDefault("iceberg.coordinator.cpu.limit", "1000m"))
                                    )))
                        ))
                        .serviceAccountName(config.getOrDefault("iceberg.coordinator.service-account", "default"))
                        .restartPolicy("Always")
                )));

        try {
            appsV1Api.createNamespacedDeployment(namespace, deployment, null, null, null, null);
            LOG.info("Created HA coordinator Deployment: {} with {} replicas", deploymentName, replicas);
        } catch (ApiException e) {
            if (e.getCode() == 409) {
                // Update existing deployment
                appsV1Api.replaceNamespacedDeployment(deploymentName, namespace, deployment, null, null, null, null);
            } else {
                throw e;
            }
        }
    }

    // ==================== Resource Naming Methods ====================

    private String getCoordinatorDeploymentName(String connectorName) {
        return sanitizeResourceName(connectorName) + "-coordinator";
    }

    private String getCoordinatorServiceName(String connectorName) {
        return sanitizeResourceName(connectorName) + "-coordinator-svc";
    }

    private String getCoordinatorConfigMapName(String connectorName) {
        return sanitizeResourceName(connectorName) + "-coordinator-config";
    }

    private String getLeaderElectionLeaseName(String connectorName) {
        return sanitizeResourceName(connectorName) + "-coordinator-leader";
    }

    private String sanitizeResourceName(String name) {
        return name.toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z0-9-]", "-");
    }
}