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

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background thread responsible for Raft leader election and heartbeat management.
 *
 * <p>This thread runs independently of the Kafka Connect data flow, ensuring that:
 * <ul>
 *   <li>Leader elections happen regardless of data traffic
 *   <li>Heartbeats are sent regularly by the leader
 *   <li>Election timeouts are checked continuously
 *   <li>No blocking of the main put() data path
 * </ul>
 *
 * <p>Thread lifecycle:
 * <ol>
 *   <li>Started in RaftCommitterImpl.open()
 *   <li>Runs until RaftCommitterImpl.close()
 *   <li>Coordinates with RaftWorker via callbacks
 * </ol>
 */
public class RaftElectionThread implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RaftElectionThread.class);

  // Check interval for election timeouts and heartbeats (500ms)
  private static final long TICK_INTERVAL_MS = 500;

  // Cluster size check interval (10 seconds to avoid excessive Admin API calls)
  private static final long CLUSTER_SIZE_CHECK_INTERVAL_MS = 10_000;

  private final RaftState raftState;
  private final IcebergSinkConfig config;
  private final KafkaClientFactory clientFactory;
  private final RaftElectionCallback callback;
  private final ExecutorService executorService;
  private volatile boolean running = true;
  private volatile long lastClusterSizeCheck = 0;
  private volatile boolean wasLeader = false; // Track leadership for transition detection

  /**
   * Callback interface for Raft election events.
   *
   * <p>Implemented by RaftCommitterImpl to handle state transitions.
   */
  public interface RaftElectionCallback {
    /**
     * Called when this node becomes Raft leader.
     *
     * <p>Implementation should start the RaftCoordinator.
     */
    void onBecameLeader();

    /**
     * Called when this node loses Raft leadership.
     *
     * <p>Implementation should stop the RaftCoordinator.
     */
    void onLostLeadership();

    /**
     * Called when leader needs to send a heartbeat.
     *
     * <p>Implementation should broadcast RaftHeartbeat event.
     */
    void onSendHeartbeat();

    /**
     * Called when candidate needs to request votes.
     *
     * <p>Implementation should broadcast RaftRequestVote event.
     *
     * @param candidateId the candidate requesting votes
     * @param term the election term
     */
    void onRequestVotes(String candidateId, long term);
  }

  public RaftElectionThread(
      RaftState raftState,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      RaftElectionCallback callback) {
    this.raftState = raftState;
    this.config = config;
    this.clientFactory = clientFactory;
    this.callback = callback;
    this.executorService =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "raft-election-" + config.taskId());
              t.setDaemon(true);
              return t;
            });

    // Register callback for immediate leadership change notifications
    // This reduces transition latency from 500ms (tick interval) to near-instant
    raftState.setLeadershipChangeCallback(this::checkLeadershipTransition);

    LOG.info("RaftElectionThread created for task {}", config.taskId());
  }

  /** Starts the background election thread. */
  public void start() {
    executorService.execute(this);
    LOG.info("RaftElectionThread started for task {}", config.taskId());
  }

  /** Stops the background election thread. */
  public void stop() {
    running = false;
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
      LOG.info("RaftElectionThread stopped for task {}", config.taskId());
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
      LOG.warn("RaftElectionThread interrupted during shutdown", e);
    }
  }

  @Override
  public void run() {
    LOG.info("RaftElectionThread main loop started for task {}", config.taskId());

    while (running && !Thread.currentThread().isInterrupted()) {
      try {
        // Check for leadership transitions (also triggered immediately via callback)
        checkLeadershipTransition();

        // Leader: send heartbeats if interval elapsed
        if (raftState.isLeader() && raftState.shouldSendHeartbeat()) {
          LOG.debug("Sending heartbeat from leader {}", config.taskId());
          try {
            callback.onSendHeartbeat();
            raftState.resetHeartbeatTime();
          } catch (Exception e) {
            LOG.error("Error sending heartbeat", e);
            // Don't propagate - will retry on next tick
          }
        }

        // Follower/Candidate: check for election timeout
        if (!raftState.isLeader() && raftState.isElectionTimeoutExpired()) {
          LOG.info(
              "Election timeout expired for task {}, starting election", config.taskId());
          try {
            startElection();
          } catch (Exception e) {
            LOG.error("Error starting election", e);
            // Don't propagate - will retry on next timeout
          }
        }

        // Periodically update cluster size for quorum calculation
        updateClusterSize();

        // Sleep for tick interval
        Thread.sleep(TICK_INTERVAL_MS);

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info("RaftElectionThread interrupted, exiting", e);
        break;
      } catch (Exception e) {
        LOG.error("Unexpected error in RaftElectionThread main loop", e);
        // Continue running despite errors - this is a critical background thread
      }
    }

    LOG.info("RaftElectionThread main loop exited for task {}", config.taskId());
  }

  /** Starts a new election cycle. */
  private void startElection() {
    if (raftState.startElection()) {
      long term = raftState.getCurrentTerm();
      String candidateId = config.taskId();

      LOG.info(
          "Node {} starting election for term {} (quorum: {})",
          candidateId,
          term,
          raftState.quorum());

      // Notify callback to broadcast RequestVote messages
      callback.onRequestVotes(candidateId, term);
    }
  }

  /**
   * Checks for leadership transitions and invokes callbacks.
   * Called both periodically (500ms) and immediately via RaftState callback.
   * Thread-safe: can be called from multiple threads.
   */
  private synchronized void checkLeadershipTransition() {
    boolean isLeader = raftState.isLeader();

    // Became leader: start coordinator
    if (isLeader && !wasLeader) {
      LOG.info("Task {} became Raft leader, notifying callback", config.taskId());
      try {
        callback.onBecameLeader();
        wasLeader = true;
      } catch (Exception e) {
        LOG.error("Error in onBecameLeader callback", e);
        // Don't propagate - let election continue
      }
    }

    // Lost leadership: stop coordinator
    if (!isLeader && wasLeader) {
      LOG.info("Task {} lost Raft leadership, notifying callback", config.taskId());
      try {
        callback.onLostLeadership();
        wasLeader = false;
      } catch (Exception e) {
        LOG.error("Error in onLostLeadership callback", e);
        // Don't propagate - let election continue
      }
    }
  }

  /**
   * Updates cluster size based on Kafka consumer group membership.
   *
   * <p>Throttled to avoid excessive Admin API calls (max once per 10 seconds).
   */
  private void updateClusterSize() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastClusterSizeCheck < CLUSTER_SIZE_CHECK_INTERVAL_MS) {
      return; // Skip, too soon
    }

    try (Admin admin = clientFactory.createAdmin()) {
      ConsumerGroupDescription groupDesc =
          KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
      Collection<MemberDescription> members = groupDesc.members();

      int previousSize = raftState.getClusterSize();
      int newSize = members.size();

      if (previousSize != newSize) {
        LOG.info(
            "Cluster size changed from {} to {} (quorum: {})",
            previousSize,
            newSize,
            (newSize / 2) + 1);
      }

      raftState.updateClusterSize(newSize);
      lastClusterSizeCheck = currentTime;

    } catch (Exception e) {
      LOG.warn("Failed to update cluster size from consumer group", e);
      // Don't fail the thread, continue with last known size
    }
  }
}
