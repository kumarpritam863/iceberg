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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for a single Iceberg Kafka Connect Job Manager.
 * Similar to Flink's JobManager, this is a per-job coordinator that manages
 * one specific Iceberg Kafka Connect job.
 */
public class IcebergJobManagerApplication {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergJobManagerApplication.class);

  private final IcebergJobManager jobManager;
  private final ScheduledExecutorService scheduler;
  private volatile boolean running;

  public IcebergJobManagerApplication(IcebergJobConfig jobConfig) {
    this.jobManager = new IcebergJobManager(jobConfig);
    this.scheduler = Executors.newScheduledThreadPool(1);
  }

  public synchronized void start() throws IOException {
    if (running) {
      LOG.warn("Job Manager application is already running");
      return;
    }

    LOG.info("Starting Iceberg Job Manager Application for job: {}", jobManager.getJobId());

    jobManager.start();

    scheduler.scheduleWithFixedDelay(
        jobManager::processEvents,
        0, 1, TimeUnit.SECONDS);

    running = true;
    LOG.info("Job Manager application started successfully for job: {}", jobManager.getJobId());
  }

  public synchronized void stop() {
    if (!running) {
      return;
    }

    LOG.info("Stopping Iceberg Job Manager Application for job: {}", jobManager.getJobId());
    running = false;

    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      scheduler.shutdownNow();
    }

    jobManager.stop();

    LOG.info("Job Manager application stopped for job: {}", jobManager.getJobId());
  }

  public boolean isRunning() {
    return running;
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
          String key = entry.getKey().substring(8).toLowerCase().replace('_', '.');
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
      value = System.getProperty(name.toLowerCase().replace('_', '.'), defaultValue);
    }
    return value;
  }
}