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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
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

  /** Raft node states */
  enum State {
    FOLLOWER,
    CANDIDATE,
    LEADER
  }

  private final String nodeId;
  private final Random random = new Random();

  // Persistent state (would be persisted in production)
  private final AtomicLong currentTerm = new AtomicLong(0);
  private final AtomicReference<String> votedFor = new AtomicReference<>(null);

  // Volatile state
  private volatile State state = State.FOLLOWER;
  private volatile String currentLeader = null;
  private volatile long lastHeartbeatTime = System.currentTimeMillis();
  private volatile long electionTimeout;

  // Election state
  private final Set<String> votesReceived = new HashSet<>();
  private int clusterSize = 1; // Updated dynamically based on consumer group members

  // Configuration
  private static final long BASE_ELECTION_TIMEOUT_MS = 5_000; // 5 seconds
  private static final long ELECTION_TIMEOUT_RANGE_MS = 5_000; // Randomized ±5 seconds
  static final long HEARTBEAT_INTERVAL_MS = 1_500; // 1.5 seconds (< election timeout)

  RaftState(String nodeId) {
    this.nodeId = nodeId;
    this.electionTimeout = randomElectionTimeout();
    LOG.info("Raft state initialized for node {} with election timeout {}ms", nodeId, electionTimeout);
  }

  /** Returns a randomized election timeout to prevent split votes */
  private long randomElectionTimeout() {
    return BASE_ELECTION_TIMEOUT_MS + random.nextInt((int) ELECTION_TIMEOUT_RANGE_MS);
  }

  /**
   * Updates cluster size based on consumer group membership. Required for quorum calculation.
   */
  void updateClusterSize(int size) {
    this.clusterSize = Math.max(1, size);
    LOG.debug("Cluster size updated to {}", this.clusterSize);
  }

  /** Returns required number of votes for quorum (majority) */
  private int quorum() {
    return (clusterSize / 2) + 1;
  }

  /** Checks if election timeout has expired */
  boolean isElectionTimeoutExpired() {
    return System.currentTimeMillis() - lastHeartbeatTime > electionTimeout;
  }

  /** Checks if it's time to send heartbeat (for leaders) */
  boolean shouldSendHeartbeat() {
    return state == State.LEADER
        && System.currentTimeMillis() - lastHeartbeatTime > HEARTBEAT_INTERVAL_MS;
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
   * @return true if this node became leader
   */
  boolean handleVoteResponse(String voterId, long term, boolean voteGranted) {
    // Ignore votes from old terms
    if (term < currentTerm.get()) {
      return false;
    }

    // If we see a newer term, step down
    if (term > currentTerm.get()) {
      LOG.info("Node {} discovered newer term {}, becoming follower", nodeId, term);
      currentTerm.set(term);
      state = State.FOLLOWER;
      votedFor.set(null);
      currentLeader = null;
      return false;
    }

    // Only process votes if we're a candidate
    if (state != State.CANDIDATE) {
      return false;
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
        return true;
      }
    }

    return false;
  }

  /**
   * Handles incoming heartbeat from leader.
   *
   * @param leaderId ID of leader
   * @param term Term of leader
   */
  void handleHeartbeat(String leaderId, long term) {
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
      state = State.FOLLOWER;
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
  }

  /** Returns current node state */
  State getState() {
    return state;
  }

  /** Returns current term */
  long getCurrentTerm() {
    return currentTerm.get();
  }

  /** Returns current leader ID (null if no known leader) */
  String getCurrentLeader() {
    return currentLeader;
  }

  /** Returns this node's ID */
  String getNodeId() {
    return nodeId;
  }

  /** Returns whether this node is the leader */
  boolean isLeader() {
    return state == State.LEADER;
  }

  /** Resets election timeout (used when receiving valid messages) */
  void resetElectionTimeout() {
    lastHeartbeatTime = System.currentTimeMillis();
  }

  @VisibleForTesting
  long getElectionTimeout() {
    return electionTimeout;
  }

  @VisibleForTesting
  void setElectionTimeout(long timeout) {
    this.electionTimeout = timeout;
  }
}
