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
package org.apache.iceberg.connect.coordinator;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for HA Iceberg Kafka Connect Job Manager.
 * Supports leader election for high availability deployments where multiple
 * coordinator replicas run but only one is active at a time.
 */
public class IcebergJobManagerApplication {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergJobManagerApplication.class);

  private final IcebergJobConfig jobConfig;
  private final IcebergJobManager jobManager;
  private final ScheduledExecutorService scheduler;
  private final LeaderElectionManager leaderElection;
  private final HealthCheckServer healthServer;
  private final AtomicBoolean isActive = new AtomicBoolean(false);
  private volatile boolean running;

  public IcebergJobManagerApplication(IcebergJobConfig jobConfig) {
    this.jobConfig = jobConfig;
    this.jobManager = new IcebergJobManager(jobConfig);
    this.scheduler = Executors.newScheduledThreadPool(2);

    // Initialize leader election if enabled
    if (isLeaderElectionEnabled()) {
      String identity = getPodIdentity();
      String leaseName = getLeaderElectionLeaseName();
      String namespace = getKubernetesNamespace();

      this.leaderElection = new LeaderElectionManager(
          identity,
          leaseName,
          namespace,
          this::onBecameLeader,
          this::onLostLeadership
      );
      LOG.info("Leader election enabled for identity: {} with lease: {}", identity, leaseName);
    } else {
      this.leaderElection = null;
      LOG.info("Leader election disabled, running as single coordinator");
    }

    // Initialize health check server
    int healthPort = getHealthCheckPort();
    this.healthServer = new HealthCheckServer(
        healthPort,
        this::isHealthy,          // Liveness check
        this::isReady            // Readiness check
    );
  }

  public synchronized void start() throws IOException {
    if (running) {
      LOG.warn("Job Manager application is already running");
      return;
    }

    LOG.info("Starting HA Iceberg Job Manager Application for job: {}", getJobId());

    // Start health check server first
    healthServer.start();

    if (leaderElection != null) {
      // Start leader election in background
      scheduler.submit(() -> {
        try {
          LOG.info("Starting leader election process...");
          leaderElection.start();
        } catch (Exception e) {
          LOG.error("Failed to start leader election", e);
          System.exit(1);
        }
      });

      // Wait for leadership or timeout
      int leadershipTimeout = getLeadershipTimeoutSeconds();
      LOG.info("Waiting for leadership acquisition (timeout: {}s)...", leadershipTimeout);
      boolean becameLeader = leaderElection.waitForLeadership(leadershipTimeout);

      if (!becameLeader) {
        LOG.warn("Failed to acquire leadership within timeout, but continuing as standby");
      }
    } else {
      // No leader election, start immediately
      activateCoordinator();
    }

    running = true;
    LOG.info("Job Manager application started successfully for job: {}", getJobId());
  }

  public synchronized void stop() {
    if (!running) {
      return;
    }

    LOG.info("Stopping HA Iceberg Job Manager Application for job: {}", getJobId());
    running = false;

    // Stop leader election first
    if (leaderElection != null) {
      leaderElection.stop();
    }

    // Deactivate coordinator if active
    if (isActive.get()) {
      deactivateCoordinator();
    }

    // Stop scheduler
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      scheduler.shutdownNow();
    }

    // Stop health server
    healthServer.stop();

    LOG.info("Job Manager application stopped for job: {}", getJobId());
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isActive() {
    return isActive.get();
  }

  public boolean isLeader() {
    return leaderElection != null ? leaderElection.isLeader() : true;
  }

  public String getLeaderIdentity() {
    return leaderElection != null ? leaderElection.getLeaderIdentity() : getPodIdentity();
  }

  // ==================== Leader Election Callbacks ====================

  private void onBecameLeader() {
    LOG.info("Became leader, activating coordinator for job: {}", getJobId());
    activateCoordinator();
  }

  private void onLostLeadership() {
    LOG.warn("Lost leadership, deactivating coordinator for job: {}", getJobId());
    deactivateCoordinator();
  }

  private void activateCoordinator() {
    if (isActive.compareAndSet(false, true)) {
      try {
        LOG.info("Activating coordinator for job: {}", getJobId());
        jobManager.start();

        // Schedule periodic event processing
        scheduler.scheduleWithFixedDelay(
            this::processEventsIfActive,
            0, 1, TimeUnit.SECONDS);

        LOG.info("Coordinator activated successfully for job: {}", getJobId());
      } catch (Exception e) {
        LOG.error("Failed to activate coordinator for job: {}", getJobId(), e);
        isActive.set(false);
        throw new RuntimeException("Failed to activate coordinator", e);
      }
    }
  }

  private void deactivateCoordinator() {
    if (isActive.compareAndSet(true, false)) {
      try {
        LOG.info("Deactivating coordinator for job: {}", getJobId());
        jobManager.stop();
        LOG.info("Coordinator deactivated for job: {}", getJobId());
      } catch (Exception e) {
        LOG.error("Error deactivating coordinator for job: {}", getJobId(), e);
      }
    }
  }

  private void processEventsIfActive() {
    if (isActive.get() && running) {
      try {
        jobManager.processEvents();
      } catch (Exception e) {
        LOG.error("Error processing events for job: {}", getJobId(), e);
        // Don't stop the application for processing errors
      }
    }
  }

  // ==================== Health Checks ====================

  private boolean isHealthy() {
    // Liveness check - application should be running and not in error state
    return running && (leaderElection == null || !isActive.get() || jobManager.isRunning());
  }

  private boolean isReady() {
    // Readiness check - ready to receive traffic (either leader or healthy standby)
    if (!running) {
      return false;
    }

    if (leaderElection == null) {
      // No leader election, just check if coordinator is running
      return isActive.get() && jobManager.isRunning();
    } else {
      // With leader election, always ready (standby instances are also ready)
      return true;
    }
  }

  // ==================== Configuration Helpers ====================

  private boolean isLeaderElectionEnabled() {
    return Boolean.parseBoolean(getProperty("LEADER_ELECTION_ENABLED", "false"));
  }

  private String getPodIdentity() {
    String podName = getProperty("POD_NAME", null);
    if (podName != null) {
      return podName;
    }

    // Fallback to hostname or generated ID
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "coordinator-" + System.currentTimeMillis();
    }
  }

  private String getLeaderElectionLeaseName() {
    return getProperty("LEADER_ELECTION_LEASE_NAME", "iceberg-coordinator-leader");
  }

  private String getKubernetesNamespace() {
    return getProperty("KUBERNETES_NAMESPACE", "default");
  }

  private int getHealthCheckPort() {
    return Integer.parseInt(getProperty("HEALTH_CHECK_PORT", "8080"));
  }

  private int getLeadershipTimeoutSeconds() {
    return Integer.parseInt(getProperty("LEADERSHIP_TIMEOUT_SECONDS", "30"));
  }

  private String getJobId() {
    return jobConfig != null ? jobConfig.jobId() : "unknown";
  }

  public static void main(String[] args) {
    try {
      IcebergJobConfig jobConfig = loadJobConfig();

      IcebergJobManagerApplication app = new IcebergJobManagerApplication(jobConfig);

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        LOG.info("Received shutdown signal");
        app.stop();
      }));

      app.start();

      // Keep the application running
      while (app.isRunning()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }

    } catch (Exception e) {
      LOG.error("Failed to start Job Manager application", e);
      System.exit(1);
    }
  }

  private static IcebergJobConfig loadJobConfig() {
    // Load configuration from environment variables or system properties
    CoordinatorConfig coordinatorConfig = CoordinatorConfig.builder()
        .kafkaBootstrapServers(
            getRequiredProperty("KAFKA_BOOTSTRAP_SERVERS"))
        .commitInterval(
            Duration.ofMinutes(Long.parseLong(getProperty("COMMIT_INTERVAL_MINUTES", "5"))))
        .commitTimeout(
            Duration.ofMinutes(Long.parseLong(getProperty("COMMIT_TIMEOUT_MINUTES", "30"))))
        .commitThreads(
            Integer.parseInt(getProperty("COMMIT_THREADS", "4")))
        .keepAliveTimeoutMs(
            Long.parseLong(getProperty("KEEPALIVE_TIMEOUT_MS", "60000")))
        .enableTransactions(
            Boolean.parseBoolean(getProperty("ENABLE_TRANSACTIONS", "true")))
        .transactionalId(
            getProperty("TRANSACTIONAL_ID", null))
        .transactionTimeout(
            Duration.ofMinutes(Long.parseLong(getProperty("TRANSACTION_TIMEOUT_MINUTES", "15"))))
        .build();

    Map<String, String> catalogProperties = loadCatalogProperties();

    return IcebergJobConfig.builder()
        .jobId(getProperty("JOB_ID", null))
        .connectGroupId(getRequiredProperty("CONNECT_GROUP_ID"))
        .controlTopic(getRequiredProperty("CONTROL_TOPIC"))
        .catalogName(getRequiredProperty("CATALOG_NAME"))
        .catalogProperties(catalogProperties)
        .coordinatorConfig(coordinatorConfig)
        .build();
  }

  private static Map<String, String> loadCatalogProperties() {
    Map<String, String> catalogProperties = new HashMap<>();

    // Load catalog properties from environment variables with CATALOG_ prefix
    System.getenv().entrySet().stream()
        .filter(entry -> entry.getKey().startsWith("CATALOG_"))
        .forEach(entry -> {
          String key = entry.getKey().substring(8).toLowerCase(java.util.Locale.ROOT).replace('_', '.');
          catalogProperties.put(key, entry.getValue());
        });

    return catalogProperties;
  }

  private static String getRequiredProperty(String name) {
    String value = getProperty(name, null);
    if (value == null) {
      throw new IllegalArgumentException("Required property not found: " + name);
    }
    return value;
  }

  private static String getProperty(String name, String defaultValue) {
    // Check environment variable first, then system property
    String value = System.getenv(name);
    if (value == null) {
      value = System.getProperty(name.toLowerCase(java.util.Locale.ROOT).replace('_', '.'), defaultValue);
    }
    return value;
  }
}