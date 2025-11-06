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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft consensus state machine for coordinator election.
 *
 * <p>Implements a simplified Raft algorithm for leader election:
 *
 * <ul>
 *   <li>**Follower**: Default state, waits for heartbeats from leader
 *   <li>**Candidate**: Initiates election when election timeout expires
 *   <li>**Leader**: Elected coordinator, sends periodic heartbeats
 * </ul>
 *
 * <p>Key properties:
 *
 * <ul>
 *   <li>**Safety**: At most one leader per term
 *   <li>**Liveness**: Eventually elects a leader
 *   <li>**No split-brain**: Requires majority votes (quorum)
 * </ul>
 */
class RaftState {

  private static final Logger LOG = LoggerFactory.getLogger(RaftState.class);

    public String getCurrentLeader() {
        return currentLeader;
    }

    public void setCurrentLeader(String currentLeader) {
        this.currentLeader = currentLeader;
    }

    /** Raft node states */
  enum State {
    FOLLOWER,
    CANDIDATE,
    LEADER
  }

  private final String nodeId;
  private final Random random = new Random();

  // Raft timing configuration (injected from IcebergSinkConfig)
  private final long baseElectionTimeoutMs;
  private final long electionTimeoutJitterMs;
  private final long heartbeatIntervalMs;

  // Persistent state (would be persisted in production)
  private final AtomicLong currentTerm = new AtomicLong(0);
  private final AtomicReference<String> votedFor = new AtomicReference<>(null);

  // Volatile state
  private volatile State state = State.FOLLOWER;
  private volatile String currentLeader = null;
  private volatile long lastHeartbeatTime = System.currentTimeMillis();
  private volatile long electionTimeout;

  // Callback for immediate leadership change notifications
  private volatile Runnable onLeadershipChangeCallback = null;

  // Election state (synchronized for thread-safety between polling and election threads)
  private final Set<String> votesReceived =
      java.util.Collections.synchronizedSet(new HashSet<>());
  private int clusterSize = 1; // Updated dynamically based on consumer group members

  RaftState(
      String nodeId,
      long baseElectionTimeoutMs,
      long electionTimeoutJitterMs,
      long heartbeatIntervalMs) {
    this.nodeId = nodeId;
    this.baseElectionTimeoutMs = baseElectionTimeoutMs;
    this.electionTimeoutJitterMs = electionTimeoutJitterMs;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.electionTimeout = randomElectionTimeout();

    // Validation and warning for potentially unstable configuration
    if (heartbeatIntervalMs * 3 >= baseElectionTimeoutMs) {
      LOG.warn(
          "Raft configuration may be unstable: heartbeat interval ({}ms) should be < "
              + "election timeout ({}ms) / 3. Consider reducing heartbeat interval or "
              + "increasing election timeout.",
          heartbeatIntervalMs,
          baseElectionTimeoutMs);
    }

    LOG.info(
        "Raft state initialized for node {} with election timeout {}ms (base: {}ms, jitter: {}ms), "
            + "heartbeat interval {}ms",
        nodeId,
        electionTimeout,
        baseElectionTimeoutMs,
        electionTimeoutJitterMs,
        heartbeatIntervalMs);
  }

  /** Returns a randomized election timeout to prevent split votes */
  private long randomElectionTimeout() {
    return baseElectionTimeoutMs + random.nextInt((int) electionTimeoutJitterMs + 1);
  }

  /**
   * Updates cluster size based on consumer group membership. Required for quorum calculation.
   */
  void updateClusterSize(int size) {
    this.clusterSize = Math.max(1, size);
    LOG.debug("Cluster size updated to {}", this.clusterSize);
  }

  /**
   * Sets a callback to be invoked immediately when leadership state changes.
   * This reduces the detection latency from the election thread's tick interval (500ms).
   */
  void setLeadershipChangeCallback(Runnable callback) {
    this.onLeadershipChangeCallback = callback;
  }

  /** Returns required number of votes for quorum (majority) */
  int quorum() {
    return (clusterSize / 2) + 1;
  }

  /** Checks if election timeout has expired */
  boolean isElectionTimeoutExpired() {
    return System.currentTimeMillis() - lastHeartbeatTime > electionTimeout;
  }

  /** Checks if it's time to send heartbeat (for leaders) */
  boolean shouldSendHeartbeat() {
    // Send heartbeats more frequently to ensure they arrive before followers timeout
    // Use 80% of configured interval to provide safety margin for network latency
    long effectiveInterval = (long) (heartbeatIntervalMs * 0.8);
    return state == State.LEADER
        && System.currentTimeMillis() - lastHeartbeatTime > effectiveInterval;
  }

  /**
   * Starts a new election. Transitions to CANDIDATE state and requests votes.
   *
   * @return true if election started successfully
   */
  boolean startElection() {
    if (state == State.LEADER) {
      return false; // Already leader
    }

    long newTerm = currentTerm.incrementAndGet();
    state = State.CANDIDATE;
    votedFor.set(nodeId); // Vote for self
    votesReceived.clear();
    votesReceived.add(nodeId); // Count own vote
    electionTimeout = randomElectionTimeout(); // Reset with new randomized timeout
    lastHeartbeatTime = System.currentTimeMillis();

    LOG.info(
        "Node {} starting election for term {} (need {} votes)",
        nodeId,
        newTerm,
        quorum());
    return true;
  }

  /**
   * Handles incoming vote request from candidate.
   *
   * @param candidateId ID of requesting candidate
   * @param term Term of requesting candidate
   * @return true if vote is granted
   */
  boolean handleVoteRequest(String candidateId, long term) {
    // Ignore vote requests from self (should be filtered by RaftWorker, but defensive check)
    if (candidateId.equals(nodeId)) {
      LOG.trace("Node {} ignoring vote request from self", nodeId);
      return false;
    }

    // If candidate's term is older, reject
    if (term < currentTerm.get()) {
      LOG.debug(
          "Node {} rejecting vote for {} (term {} < current term {})",
          nodeId,
          candidateId,
          term,
          currentTerm.get());
      return false;
    }

    // If candidate's term is newer, update our term and become follower
    if (term > currentTerm.get()) {
      LOG.info(
          "Node {} discovered newer term {} from {}, becoming follower",
          nodeId,
          term,
          candidateId);
      currentTerm.set(term);
      state = State.FOLLOWER;
      votedFor.set(null);
      currentLeader = null;
    }

    // Grant vote if haven't voted in this term, or already voted for this candidate
    String currentVote = votedFor.get();
    if (currentVote == null || currentVote.equals(candidateId)) {
      votedFor.set(candidateId);
      lastHeartbeatTime = System.currentTimeMillis(); // Reset election timeout
      LOG.info("Node {} granting vote to {} for term {}", nodeId, candidateId, term);
      return true;
    }

    LOG.debug(
        "Node {} rejecting vote for {} (already voted for {})",
        nodeId,
        candidateId,
        currentVote);
    return false;
  }

  /**
   * Handles incoming vote response.
   *
   * @param voterId ID of voter
   * @param term Term of vote
   * @param voteGranted Whether vote was granted
   */
  void handleVoteResponse(String voterId, long term, boolean voteGranted) {
    // Ignore vote responses from self (should be filtered by RaftWorker, but defensive check)
    if (voterId.equals(nodeId)) {
      LOG.trace("Node {} ignoring vote response from self", nodeId);
      return;
    }

    // Ignore votes from old terms
    if (term < currentTerm.get()) {
      return;
    }

    // If we see a newer term, step down
    if (term > currentTerm.get()) {
      LOG.info("Node {} discovered newer term {}, becoming follower", nodeId, term);
      boolean wasLeader = (state == State.LEADER);
      currentTerm.set(term);
      state = State.FOLLOWER;
      votedFor.set(null);
      currentLeader = null;

      // Notify election thread immediately if we lost leadership
      if (wasLeader && onLeadershipChangeCallback != null) {
        try {
          onLeadershipChangeCallback.run();
        } catch (Exception e) {
          LOG.warn("Error in leadership change callback", e);
        }
      }
      return;
    }

    // Only process votes if we're a candidate
    if (state != State.CANDIDATE) {
      return;
    }

    if (voteGranted) {
      votesReceived.add(voterId);
      LOG.debug(
          "Node {} received vote from {} ({}/{})",
          nodeId,
          voterId,
          votesReceived.size(),
          quorum());

      // Check if we have majority
      if (votesReceived.size() >= quorum()) {
        becomeLeader();
      }
    }

  }

  /**
   * Handles incoming heartbeat from leader.
   *
   * @param leaderId ID of leader
   * @param term Term of leader
   */
  void handleHeartbeat(String leaderId, long term) {
    // Ignore heartbeats from self (should be filtered by RaftWorker, but defensive check)
    if (leaderId.equals(nodeId)) {
      LOG.trace("Node {} ignoring heartbeat from self", nodeId);
      return;
    }

    // If heartbeat is from old term, ignore
    if (term < currentTerm.get()) {
      return;
    }

    // If heartbeat is from newer term, update and become follower
    if (term > currentTerm.get()) {
      LOG.info(
          "Node {} discovered newer term {} from leader {}, becoming follower",
          nodeId,
          term,
          leaderId);
      currentTerm.set(term);
      votedFor.set(null);
    }

    // Accept leader and reset election timeout
    if (state != State.FOLLOWER) {
      LOG.info("Node {} recognizing {} as leader for term {}", nodeId, leaderId, term);
      boolean wasLeader = (state == State.LEADER);
      state = State.FOLLOWER;

      // Notify election thread immediately if we were the leader (lost leadership)
      if (wasLeader && onLeadershipChangeCallback != null) {
        try {
          onLeadershipChangeCallback.run();
        } catch (Exception e) {
          LOG.warn("Error in leadership change callback", e);
        }
      }
    }

    currentLeader = leaderId;
    lastHeartbeatTime = System.currentTimeMillis();
  }

  /** Transitions this node to leader state */
  private void becomeLeader() {
    LOG.info(
        "Node {} elected as leader for term {} with {} votes",
        nodeId,
        currentTerm.get(),
        votesReceived.size());
    state = State.LEADER;
    currentLeader = nodeId;
    lastHeartbeatTime = System.currentTimeMillis();

    // Notify election thread immediately to reduce leadership transition latency
    if (onLeadershipChangeCallback != null) {
      try {
        onLeadershipChangeCallback.run();
      } catch (Exception e) {
        LOG.warn("Error in leadership change callback", e);
      }
    }
  }

  /** Returns current term */
  long getCurrentTerm() {
    return currentTerm.get();
  }

  /** Returns whether this node is the leader */
  boolean isLeader() {
    return state == State.LEADER;
  }

  /** Resets heartbeat time (used by leader after sending heartbeat) */
  void resetHeartbeatTime() {
    lastHeartbeatTime = System.currentTimeMillis();
  }

  /** Returns current cluster size */
  int getClusterSize() {
    return clusterSize;
  }

  /** Returns number of votes received in current election */
  int getVoteCount() {
    return votesReceived.size();
  }

}
