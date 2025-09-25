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

import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoordinationV1Api;
import io.kubernetes.client.openapi.models.V1Lease;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader election manager for Iceberg coordinators using Kubernetes leases.
 * Ensures only one coordinator is active at a time while providing HA failover.
 */
public class LeaderElectionManager {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionManager.class);

    private final String identity;
    private final String leaseName;
    private final String namespace;
    private final Duration leaseDuration;
    private final Duration renewDeadline;
    private final Duration retryPeriod;
    private final Runnable onStartedLeading;
    private final Runnable onStoppedLeading;

    private LeaderElector leaderElector;
    private volatile boolean isLeader;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch leaderLatch = new CountDownLatch(1);

    public LeaderElectionManager(String identity, String leaseName, String namespace,
                               Runnable onStartedLeading, Runnable onStoppedLeading) {
        this.identity = identity;
        this.leaseName = leaseName;
        this.namespace = namespace;
        this.leaseDuration = Duration.ofSeconds(15);
        this.renewDeadline = Duration.ofSeconds(10);
        this.retryPeriod = Duration.ofSeconds(2);
        this.onStartedLeading = onStartedLeading;
        this.onStoppedLeading = onStoppedLeading;
        this.isLeader = false;
    }

    /**
     * Start leader election process. This method blocks until leadership is acquired
     * or the election process fails.
     */
    public void start() throws IOException {
        if (running.get()) {
            LOG.warn("Leader election is already running");
            return;
        }

        LOG.info("Starting leader election for identity: {} with lease: {}", identity, leaseName);

        try {
            ApiClient apiClient = Config.defaultClient();
            CoordinationV1Api coordinationV1Api = new CoordinationV1Api(apiClient);

            // Ensure lease exists
            ensureLeaseExists(coordinationV1Api);

            // Create lease lock
            LeaseLock leaseLock = new LeaseLock(namespace, leaseName, identity);

            // Configure leader election
            LeaderElectionConfig config = new LeaderElectionConfig(
                leaseLock,
                leaseDuration,
                renewDeadline,
                retryPeriod
            );

            leaderElector = new LeaderElector(config);

            // Set up callbacks
            leaderElector.run(
                () -> {
                    LOG.info("Acquired leadership for identity: {}", identity);
                    isLeader = true;
                    leaderLatch.countDown();
                    if (onStartedLeading != null) {
                        try {
                            onStartedLeading.run();
                        } catch (Exception e) {
                            LOG.error("Error in onStartedLeading callback", e);
                        }
                    }
                },
                () -> {
                    LOG.warn("Lost leadership for identity: {}", identity);
                    isLeader = false;
                    if (onStoppedLeading != null) {
                        try {
                            onStoppedLeading.run();
                        } catch (Exception e) {
                            LOG.error("Error in onStoppedLeading callback", e);
                        }
                    }
                },
                exception -> {
                    LOG.error("Leader election failed for identity: {}", identity, exception);
                    isLeader = false;
                    leaderLatch.countDown();
                }
            );

            running.set(true);
            LOG.info("Leader election started for identity: {}", identity);

        } catch (Exception e) {
            LOG.error("Failed to start leader election for identity: {}", identity, e);
            throw new IOException("Failed to start leader election", e);
        }
    }

    /**
     * Wait for leadership to be acquired.
     *
     * @param timeoutSeconds Maximum time to wait for leadership
     * @return true if leadership was acquired, false if timeout
     */
    public boolean waitForLeadership(int timeoutSeconds) {
        try {
            if (timeoutSeconds <= 0) {
                leaderLatch.await();
                return isLeader;
            } else {
                boolean acquired = leaderLatch.await(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS);
                return acquired && isLeader;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for leadership", e);
            return false;
        }
    }

    /**
     * Stop leader election and release leadership if held.
     */
    public void stop() {
        if (!running.get()) {
            return;
        }

        LOG.info("Stopping leader election for identity: {}", identity);
        running.set(false);

        if (leaderElector != null) {
            try {
                leaderElector.close();
            } catch (Exception e) {
                LOG.warn("Error stopping leader elector", e);
            }
        }

        isLeader = false;
        LOG.info("Leader election stopped for identity: {}", identity);
    }

    /**
     * Check if this instance is currently the leader.
     */
    public boolean isLeader() {
        return isLeader && running.get();
    }

    /**
     * Get the current leader identity.
     */
    public String getLeaderIdentity() {
        if (isLeader()) {
            return identity;
        }
        return null;
    }

    private void ensureLeaseExists(CoordinationV1Api coordinationV1Api) {
        try {
            // Try to get the lease
            coordinationV1Api.readNamespacedLease(leaseName, namespace, null);
            LOG.debug("Lease {} already exists in namespace {}", leaseName, namespace);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                // Lease doesn't exist, create it
                try {
                    V1Lease lease = new V1Lease()
                        .metadata(new V1ObjectMeta()
                            .name(leaseName)
                            .namespace(namespace));

                    coordinationV1Api.createNamespacedLease(namespace, lease, null, null, null, null);
                    LOG.info("Created lease {} in namespace {}", leaseName, namespace, e);
                } catch (ApiException createException) {
                    if (createException.getCode() != 409) { // Ignore conflict (already exists)
                        LOG.warn("Failed to create lease {}: {}", leaseName, createException.getMessage(), createException);
                    }
                }
            } else {
                LOG.warn("Error checking lease {}: {}", leaseName, e.getMessage(), e);
            }
        }
    }
}