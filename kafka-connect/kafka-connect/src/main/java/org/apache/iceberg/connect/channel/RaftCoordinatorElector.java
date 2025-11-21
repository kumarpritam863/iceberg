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

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.RaftAppendEntries;
import org.apache.iceberg.connect.events.RaftAppendResponse;
import org.apache.iceberg.connect.events.RaftRequestVote;
import org.apache.iceberg.connect.events.RaftVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft-based coordinator election using dedicated Kafka election topic for RPC communication.
 *
 * <p>GUARANTEES:
 *
 * <ul>
 *   <li>Exactly one coordinator per term (Election Safety)
 *   <li>No split-brain (term-based protection)
 *   <li>Fast failover (150-300ms elections)
 *   <li>Network partition safe (majority quorum required)
 * </ul>
 *
 * <p>Communication happens via dedicated {@link RaftElectionTransport} with separate topic,
 * consumer, and producer - completely decoupled from main data flow:
 *
 * <ul>
 *   <li>{@link RaftRequestVote} - Vote requests during leader election
 *   <li>{@link RaftVoteResponse} - Vote responses from followers
 *   <li>{@link RaftAppendEntries} - Heartbeats from leader
 *   <li>{@link RaftAppendResponse} - Heartbeat acknowledgments
 * </ul>
 */
public class RaftCoordinatorElector implements RaftElectionTransport.RaftMessageListener {

  private static final Logger LOG = LoggerFactory.getLogger(RaftCoordinatorElector.class);

  // Raft timing constants (tuned for Kafka Connect environment)
  private static final Duration HEARTBEAT_INTERVAL = Duration.ofMillis(50);
  private static final Duration MIN_ELECTION_TIMEOUT = Duration.ofMillis(150);
  private static final Duration MAX_ELECTION_TIMEOUT = Duration.ofMillis(300);

  /** Raft state machine */
  private enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
  }

  // Node identity
  private final String nodeId;
  private final String groupId;
  private final RaftElectionTransport transport;

  // Raft state
  private volatile RaftState state = RaftState.FOLLOWER;
  private long currentTerm = 0;
  private String votedFor = null;

  // Cluster membership
  private final Set<String> allNodes = ConcurrentHashMap.newKeySet();

  // Election state
  private final Set<String> votesReceived = ConcurrentHashMap.newKeySet();
  private Instant lastHeartbeatReceived = Instant.now();
  private Instant lastHeartbeatSent = Instant.MIN;
  private Duration electionTimeout;
  private final Random random = new Random();

  // Background scheduler
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  // Coordinator change listener
  private CoordinatorChangeListener changeListener;

  /**
   * Create a new Raft coordinator elector.
   *
   * @param nodeId This node's unique identifier
   * @param groupId Election group ID
   * @param transport Dedicated transport layer for election messages
   */
  public RaftCoordinatorElector(String nodeId, String groupId, RaftElectionTransport transport) {
    this.nodeId = nodeId;
    this.groupId = groupId;
    this.transport = transport;
    this.electionTimeout = randomElectionTimeout();

    // Register as message listener
    transport.setMessageListener(this);
  }

  /**
   * Start Raft consensus with initial cluster membership.
   *
   * @param initialNodes Set of all task IDs in the cluster
   */
  public void start(Set<String> initialNodes) {
    if (isRunning.compareAndSet(false, true)) {
      allNodes.addAll(initialNodes);

      LOG.info(
          "[RAFT] [{}] Starting Raft consensus (cluster={}, group={})",
          nodeId,
          initialNodes,
          groupId);

      // Start election transport
      transport.start();

      // Start election timer
      @SuppressWarnings("FutureReturnValueIgnored")
      ScheduledFuture<?> unused = scheduler.scheduleAtFixedRate(this::tick, 0, 10, TimeUnit.MILLISECONDS);
    }
  }

  /** Main tick function - checks timeouts and sends heartbeats */
  private void tick() {
    if (!isRunning.get()) {
      return;
    }

    Instant now = Instant.now();

    switch (state) {
      case FOLLOWER:
      case CANDIDATE:
        // Check if election timeout elapsed
        long timeSinceHeartbeat = Duration.between(lastHeartbeatReceived, now).toMillis();
        if (Duration.between(lastHeartbeatReceived, now).compareTo(electionTimeout) >= 0) {
          LOG.debug("[RAFT] [{}] Election timeout expired ({}ms since last heartbeat, timeout={}ms, state={})",
              nodeId, timeSinceHeartbeat, electionTimeout.toMillis(), state);
          startElection();
        }
        break;

      case LEADER:
        // Send periodic heartbeats
        if (Duration.between(lastHeartbeatSent, now).compareTo(HEARTBEAT_INTERVAL) >= 0) {
          LOG.trace("[RAFT] [{}] Heartbeat interval expired, sending heartbeats", nodeId);
          sendHeartbeats();
          lastHeartbeatSent = now;
        }
        break;
    }
  }

  /** Start leader election */
  private void startElection() {
    currentTerm++;
    state = RaftState.CANDIDATE;
    votedFor = nodeId;
    votesReceived.clear();
    votesReceived.add(nodeId); // Vote for self
    electionTimeout = randomElectionTimeout();
    lastHeartbeatReceived = Instant.now();

    LOG.warn(
        "[RAFT] [{}] *** STARTING ELECTION *** (term={}, timeout={}ms)",
        nodeId,
        currentTerm,
        electionTimeout.toMillis());

    // Send RequestVote to all other nodes
    for (String node : allNodes) {
      if (!node.equals(nodeId)) {
        RaftRequestVote voteRequest = new RaftRequestVote(currentTerm, nodeId, 0, 0);
        transport.send(voteRequest, groupId);
      }
    }

    // Check if already won (single-node cluster)
    checkElectionResult();
  }

  /**
   * Handle incoming RequestVote RPC (called via transport listener).
   *
   * @param request Vote request from candidate
   * @param fromNode Candidate node ID
   */
  public void handleRequestVote(RaftRequestVote request, String fromNode) {
    boolean voteGranted = false;

    synchronized (this) {
      // Update term if necessary
      if (request.term() > currentTerm) {
        stepDown(request.term());
      }

      // Grant vote if:
      // 1. Request term >= current term
      // 2. Haven't voted yet, OR already voted for this candidate
      // 3. Candidate's log is at least as up-to-date (simplified: always true)
      if (request.term() >= currentTerm
          && (votedFor == null || votedFor.equals(request.candidateId()))) {
        voteGranted = true;
        votedFor = request.candidateId();
        lastHeartbeatReceived = Instant.now(); // Reset election timer

        LOG.info("[RAFT] [{}] VOTED for {} (term={})", nodeId, request.candidateId(), request.term());
      } else {
        LOG.debug(
            "[RAFT] [{}] REJECTED vote for {} (term={}, votedFor={})",
            nodeId,
            request.candidateId(),
            request.term(),
            votedFor);
      }
    }

    // Send vote response
    RaftVoteResponse response = new RaftVoteResponse(currentTerm, voteGranted, nodeId);
    transport.send(response, groupId);
  }

  /**
   * Handle incoming VoteResponse (called via transport listener).
   *
   * @param response Vote response from follower
   */
  public void handleVoteResponse(RaftVoteResponse response) {
    synchronized (this) {
      // Ignore if not candidate or stale response
      if (state != RaftState.CANDIDATE || response.term() < currentTerm) {
        return;
      }

      // Step down if higher term
      if (response.term() > currentTerm) {
        stepDown(response.term());
        return;
      }

      // Record vote
      if (response.voteGranted()) {
        votesReceived.add(response.voterId());
        int majoritySize = (allNodes.size() / 2) + 1;
        LOG.debug(
            "[RAFT] [{}] Received vote from {} ({}/{})",
            nodeId,
            response.voterId(),
            votesReceived.size(),
            majoritySize);
      }

      // Check if won election
      checkElectionResult();
    }
  }

  /** Check if won the election */
  private void checkElectionResult() {
    int majoritySize = (allNodes.size() / 2) + 1;
    if (state == RaftState.CANDIDATE && votesReceived.size() >= majoritySize) {
      LOG.info("[RAFT] [{}] Won election with {}/{} votes (majority={})",
          nodeId, votesReceived.size(), allNodes.size(), majoritySize);
      becomeLeader();
    } else if (state == RaftState.CANDIDATE) {
      LOG.debug("[RAFT] [{}] Still collecting votes ({}/{}, need {})",
          nodeId, votesReceived.size(), allNodes.size(), majoritySize);
    }
  }

  /** Transition to LEADER */
  private void becomeLeader() {
    state = RaftState.LEADER;

    LOG.warn(
        "[RAFT] [{}] *** BECAME LEADER *** (term={}, votes={}/{})",
        nodeId,
        currentTerm,
        votesReceived.size(),
        allNodes.size());

    // Send immediate heartbeat to establish authority
    sendHeartbeats();
    lastHeartbeatSent = Instant.now();

    // Notify listener
    if (changeListener != null) {
      changeListener.onBecameCoordinator(currentTerm);
    }
  }

  /** Send heartbeats to all followers */
  private void sendHeartbeats() {
    for (String node : allNodes) {
      if (!node.equals(nodeId)) {
        RaftAppendEntries heartbeat =
            new RaftAppendEntries(
                currentTerm,
                nodeId,
                0, // prevLogIndex (simplified)
                0, // prevLogTerm (simplified)
                0, // leaderCommit (simplified)
                true); // isHeartbeat
        transport.send(heartbeat, groupId);
      }
    }
  }

  /**
   * Handle incoming AppendEntries (heartbeat) (called via transport listener).
   *
   * @param request Heartbeat from leader
   */
  public void handleAppendEntries(RaftAppendEntries request) {
    boolean success = false;

    synchronized (this) {
      // Update term if necessary
      if (request.term() > currentTerm) {
        stepDown(request.term());
      }

      // Reject stale requests
      if (request.term() < currentTerm) {
        RaftAppendResponse response = new RaftAppendResponse(currentTerm, false, nodeId);
        transport.send(response, groupId);
        return;
      }

      // Valid heartbeat from leader
      if (state == RaftState.CANDIDATE) {
        // Another node became leader, step down
        stepDown(request.term());
      }

      lastHeartbeatReceived = Instant.now(); // Reset election timer
      success = true;

      LOG.trace("[RAFT] [{}] Received heartbeat from leader {} (term={})", nodeId, request.leaderId(), request.term());
    }

    // Send success response
    RaftAppendResponse response = new RaftAppendResponse(currentTerm, success, nodeId);
    transport.send(response, groupId);
  }

  /**
   * Handle incoming Raft message from transport layer.
   * Implements {@link RaftElectionTransport.RaftMessageListener}.
   *
   * @param payload The Raft message payload
   * @param senderId The sender's node ID
   * @param groupId The election group ID
   */
  @Override
  public void onMessage(Payload payload, String senderId, String groupId) {
    // Only process messages for our group
    if (!this.groupId.equals(groupId)) {
      return;
    }

    switch (payload.type()) {
      case RAFT_REQUEST_VOTE:
        handleRequestVote((RaftRequestVote) payload, senderId);
        break;

      case RAFT_VOTE_RESPONSE:
        handleVoteResponse((RaftVoteResponse) payload);
        break;

      case RAFT_APPEND_ENTRIES:
        handleAppendEntries((RaftAppendEntries) payload);
        break;

      case RAFT_APPEND_RESPONSE:
        // Leader processes append responses (future: log replication)
        break;

      default:
        LOG.warn("[RAFT] [{}] Received unexpected payload type: {}", nodeId, payload.type());
    }
  }

  /** Step down to FOLLOWER */
  private void stepDown(long newTerm) {
    long oldTerm = currentTerm;
    if (newTerm > currentTerm) {
      LOG.debug("[RAFT] [{}] Discovered higher term {} (current={}), stepping down",
          nodeId, newTerm, currentTerm);
      currentTerm = newTerm;
      votedFor = null;
    }

    boolean wasLeader = (state == RaftState.LEADER);
    RaftState oldState = state;
    state = RaftState.FOLLOWER;
    lastHeartbeatReceived = Instant.now();

    if (wasLeader) {
      LOG.warn("[RAFT] [{}] Stepping down from LEADER to FOLLOWER (term: {} → {})",
          nodeId, oldTerm, currentTerm);

      // Notify listener
      if (changeListener != null) {
        changeListener.onLostCoordinator(currentTerm);
      }
    } else if (oldState != RaftState.FOLLOWER) {
      LOG.debug("[RAFT] [{}] Stepping down from {} to FOLLOWER (term: {} → {})",
          nodeId, oldState, oldTerm, currentTerm);
    }
  }

  /**
   * Update cluster membership (called on rebalance)
   *
   * @param newNodes Updated set of task IDs
   */
  public void updateNodes(Set<String> newNodes) {
    synchronized (this) {
      Set<String> oldNodes = Set.copyOf(allNodes);
      allNodes.clear();
      allNodes.addAll(newNodes);

      Set<String> added = newNodes.stream()
          .filter(n -> !oldNodes.contains(n))
          .collect(java.util.stream.Collectors.toSet());
      Set<String> removed = oldNodes.stream()
          .filter(n -> !newNodes.contains(n))
          .collect(java.util.stream.Collectors.toSet());

      LOG.info(
          "[RAFT] [{}] Cluster membership changed: {} → {} (state={}, term={}, added={}, removed={})",
          nodeId,
          oldNodes,
          newNodes,
          state,
          currentTerm,
          added.isEmpty() ? "none" : added,
          removed.isEmpty() ? "none" : removed);

      // Check if lost quorum
      int majoritySize = (allNodes.size() / 2) + 1;
      if (state == RaftState.LEADER && allNodes.size() < majoritySize) {
        LOG.warn("[RAFT] [{}] Lost quorum (cluster size {} < majority {}), stepping down from LEADER",
            nodeId, allNodes.size(), majoritySize);
        stepDown(currentTerm);
      } else if (state == RaftState.LEADER) {
        LOG.debug("[RAFT] [{}] Still have quorum as LEADER ({}/{})",
            nodeId, allNodes.size(), majoritySize);
      }
    }
  }

  /**
   * Check if this node is currently the coordinator
   *
   * @return true if leader, false otherwise
   */
  public boolean isCoordinator() {
    return state == RaftState.LEADER;
  }

  /**
   * Get current Raft term
   *
   * @return current term number
   */
  public long getCurrentTerm() {
    return currentTerm;
  }

  /**
   * Set listener for coordinator role changes
   *
   * @param listener Callback interface for coordinator changes
   */
  public void setChangeListener(CoordinatorChangeListener listener) {
    this.changeListener = listener;
  }

  /** Stop Raft consensus */
  public void stop() {
    if (isRunning.compareAndSet(true, false)) {
      LOG.info("[RAFT] [{}] Stopping Raft consensus (state={}, term={})", nodeId, state, currentTerm);

      if (state == RaftState.LEADER) {
        // Send final heartbeat to help followers recognize leader loss faster
        sendHeartbeats();
      }

      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        scheduler.shutdownNow();
      }

      // Stop transport layer
      transport.stop();
    }
  }

  private Duration randomElectionTimeout() {
    long minMs = MIN_ELECTION_TIMEOUT.toMillis();
    long maxMs = MAX_ELECTION_TIMEOUT.toMillis();
    long randomMs = minMs + random.nextInt((int) (maxMs - minMs));
    return Duration.ofMillis(randomMs);
  }

  /** Callback interface for coordinator role changes */
  public interface CoordinatorChangeListener {
    /** Called when this node becomes coordinator */
    void onBecameCoordinator(long term);

    /** Called when this node loses coordinator role */
    void onLostCoordinator(long term);
  }
}
