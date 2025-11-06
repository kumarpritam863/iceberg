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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.RaftHeartbeat;
import org.apache.iceberg.connect.events.RaftRequestVote;
import org.apache.iceberg.connect.events.RaftVoteResponse;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft-based Worker implementation with background control topic polling.
 *
 * <p>This worker implements a two-threaded architecture:
 * <ul>
 *   <li>Background polling thread: Continuously polls control topic for all messages
 *   <li>Main thread: Processes buffered START_COMMIT messages from put() method
 * </ul>
 *
 * <p>Message handling strategy:
 * <ul>
 *   <li>Raft messages (RequestVote, VoteResponse, Heartbeat): Processed immediately in background thread
 *   <li>Commit messages (START_COMMIT, COMMIT_COMPLETE): Buffered for main thread processing
 * </ul>
 *
 * <p>This ensures Raft consensus operates independently of data flow, preventing blocking
 * and ensuring timely election/heartbeat processing.
 */
class RaftWorker extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(RaftWorker.class);

  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final SinkWriter sinkWriter;
  private final RaftCommitterImpl committer;
  private final String worker;

  // Async processing components
  private final ConcurrentLinkedQueue<Envelope> commitEventQueue;
  private final ExecutorService pollingExecutor;
  private final AtomicBoolean running;
  private final Duration pollInterval;
  private final AtomicReference<Exception> errorRef = new AtomicReference<>(null);

  RaftWorker(
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkWriter sinkWriter,
      SinkTaskContext context,
      RaftCommitterImpl committer) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "raft-worker",
        config.controlGroupIdPrefix() + UUID.randomUUID(),
        config,
        clientFactory,
        context);

    this.worker = "raft-worker-" + config.connectorName() + "-" + config.taskId();
    this.config = config;
    this.context = context;
    this.sinkWriter = sinkWriter;
    this.committer = committer;

    // Initialize async processing components
    this.commitEventQueue = new ConcurrentLinkedQueue<>();
    this.running = new AtomicBoolean(false);
    this.pollInterval = Duration.ofMillis(Math.min(config.commitIntervalMs() / 10, 1000)); // Poll frequently
    this.pollingExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread thread =
                  new Thread(
                      r,
                      "raft-worker-control-poller-"
                          + config.connectorName()
                          + "-"
                          + config.taskId());
              thread.setDaemon(true);
              return thread;
            });
  }

  @Override
  void start() {
    super.start();
    running.set(true);

    try {
      // Start background polling thread
      pollingExecutor.execute(this::backgroundPoll);
      LOG.info(
          "RaftWorker {} started with background control topic polling (poll interval: {}ms)",
          worker,
          pollInterval.toMillis());
    } catch (Exception ex) {
      LOG.error("RaftWorker {} failed to start background polling thread", worker, ex);
      throw new ConnectException(
          String.format("RaftWorker %s failed to start polling thread", worker), ex);
    }
  }

  /**
   * Background polling task that continuously polls the control topic.
   *
   * <p>This thread:
   * <ul>
   *   <li>Polls control topic at high frequency (every 100-1000ms)
   *   <li>Processes Raft messages immediately
   *   <li>Buffers commit messages for main thread
   * </ul>
   */
  private void backgroundPoll() {
    LOG.info("Background control topic polling thread started on {}", worker);
    try {
      while (running.get() && !Thread.currentThread().isInterrupted()) {
        try {
          // Poll control topic continuously
          consumeAvailable(pollInterval);
        } catch (Exception e) {
          LOG.error("RaftWorker {} failed while polling control events", worker, e);
          errorRef.set(e);
          running.set(false);
          break;
        }
      }
    } finally {
      LOG.info("Background control topic polling thread stopped on {}", worker);
    }
  }

  /**
   * Process buffered commit events from the queue (called from main put() thread).
   *
   * <p>This drains all available START_COMMIT messages and processes them.
   */
  @Override
  public void process() {
    // Check for errors from background thread
    Exception ex = errorRef.getAndSet(null);
    if (ex != null) {
      stop();
      throw new ConnectException(String.format("RaftWorker %s failed", worker), ex);
    }

    Envelope envelope;
    int processed = 0;

    // Drain all available commit events from queue
    while ((envelope = commitEventQueue.poll()) != null) {
      processCommitEvent(envelope);
      processed++;
    }

    if (processed > 0) {
      LOG.debug("Processed {} buffered commit events", processed);
    }
  }

  /**
   * Receives messages from control topic (called by background polling thread).
   *
   * <p>Routing strategy:
   * <ul>
   *   <li>Raft messages: Process immediately in this thread (skip own messages)
   *   <li>Commit messages: Buffer for main thread processing
   * </ul>
   */
  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    PayloadType payloadType = event.payload().type();

    switch (payloadType) {
      case RAFT_REQUEST_VOTE:
        // Process immediately - time-sensitive (skip own vote requests)
        RaftRequestVote voteRequest = (RaftRequestVote) event.payload();
        if (!isSelfEvent(voteRequest.candidateId())) {
          handleRaftRequestVote(event);
        } else {
          LOG.trace("RaftWorker {} ignoring own RequestVote", worker);
        }
        return true;

      case RAFT_VOTE_RESPONSE:
        // Process immediately - time-sensitive (skip own vote responses)
        RaftVoteResponse voteResponse = (RaftVoteResponse) event.payload();
        if (!isSelfEvent(voteResponse.voterId())) {
          handleRaftVoteResponse(event);
        } else {
          LOG.trace("RaftWorker {} ignoring own VoteResponse", worker);
        }
        return true;

      case RAFT_HEARTBEAT:
        // Process immediately - time-sensitive (skip own heartbeats)
        RaftHeartbeat heartbeat = (RaftHeartbeat) event.payload();
        if (!isSelfEvent(heartbeat.leaderId())) {
          handleRaftHeartbeat(event);
        } else {
          LOG.trace("RaftWorker {} ignoring own Heartbeat", worker);
        }
        return true;

      case START_COMMIT:
      case COMMIT_COMPLETE:
      case COMMIT_TO_TABLE:
        // Buffer for main thread processing
        commitEventQueue.offer(envelope);
        LOG.debug("RaftWorker {} buffered commit event: {}", worker, payloadType);
        return true;

      default:
        return false;
    }
  }

  /**
   * Checks if a Raft message originated from this worker.
   *
   * @param senderId ID of the message sender (candidateId, voterId, or leaderId)
   * @return true if this worker sent the message, false otherwise
   */
  private boolean isSelfEvent(String senderId) {
    return config.taskId().equals(senderId);
  }

  /** Process a buffered commit event (called from main thread). */
  private void processCommitEvent(Envelope envelope) {
    Event event = envelope.event();
    switch (event.type()) {
      case START_COMMIT:
        handleStartCommit(event);
        break;
      case COMMIT_COMPLETE:
        // No-op for now, can add cleanup logic if needed
        break;
      case COMMIT_TO_TABLE:
        // No-op for now, can add metrics if needed
        break;
      default:
        LOG.warn(
            "RaftWorker {} received unexpected event type in commit queue: {}",
            worker,
            event.type());
    }
  }

  private void handleStartCommit(Event event) {
    SinkWriterResult results = sinkWriter.completeWrite();

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                tp -> {
                  Offset offset = results.sourceOffsets().get(tp);
                  if (offset == null) {
                    offset = Offset.NULL_OFFSET;
                  }
                  return new TopicPartitionOffset(
                      tp.topic(), tp.partition(), offset.offset(), offset.timestamp());
                })
            .collect(Collectors.toList());

    UUID commitId = ((StartCommit) event.payload()).commitId();

    List<Event> events =
        results.writerResults().stream()
            .map(
                writeResult ->
                    new Event(
                        config.connectGroupId(),
                        new DataWritten(
                            writeResult.partitionStruct(),
                            commitId,
                            TableReference.of(config.catalogName(), writeResult.tableIdentifier()),
                            writeResult.dataFiles(),
                            writeResult.deleteFiles())))
            .collect(Collectors.toList());

    Event readyEvent = new Event(config.connectGroupId(), new DataComplete(commitId, assignments));
    events.add(readyEvent);

    send(events, results.sourceOffsets());
  }

  // ========================================================================
  // Raft Message Handlers (Called from background polling thread)
  // ========================================================================

  private void handleRaftRequestVote(Event event) {
    RaftRequestVote payload = (RaftRequestVote) event.payload();
    LOG.debug(
        "RaftWorker {} processing RequestVote from candidate {} for term {}",
        worker,
        payload.candidateId(),
        payload.term());
    committer.handleRaftRequestVote(payload.candidateId(), payload.term());
  }

  private void handleRaftVoteResponse(Event event) {
    RaftVoteResponse payload = (RaftVoteResponse) event.payload();
    LOG.debug(
        "RaftWorker {} processing VoteResponse from {} for term {} (granted: {})",
        worker,
        payload.voterId(),
        payload.term(),
        payload.voteGranted());
    committer.handleRaftVoteResponse(payload.voterId(), payload.term(), payload.voteGranted());
  }

  private void handleRaftHeartbeat(Event event) {
    RaftHeartbeat payload = (RaftHeartbeat) event.payload();
    LOG.debug(
        "RaftWorker {} processing Heartbeat from leader {} for term {}",
        worker,
        payload.leaderId(),
        payload.term());
    committer.handleRaftHeartbeat(payload.leaderId(), payload.term());
  }

  // ========================================================================
  // Raft Message Senders
  // ========================================================================

  /**
   * Sends a Raft RequestVote message to all workers via the control topic.
   *
   * @param candidateId ID of the candidate requesting votes
   * @param term Election term
   */
  void sendRaftRequestVote(String candidateId, long term) {
    Event event = new Event(config.connectGroupId(), new RaftRequestVote(candidateId, term));
    send(event);
    LOG.debug("RaftWorker {} sent RequestVote for candidate {} term {}", worker, candidateId, term);
  }

  /**
   * Sends a Raft VoteResponse message to all workers via the control topic.
   *
   * @param voterId ID of the voter
   * @param term Election term
   * @param voteGranted Whether the vote was granted
   */
  void sendRaftVoteResponse(String voterId, long term, boolean voteGranted) {
    Event event =
        new Event(config.connectGroupId(), new RaftVoteResponse(voterId, term, voteGranted));
    send(event);
    LOG.debug(
        "RaftWorker {} sent VoteResponse from {} for term {} (granted: {})",
        worker,
        voterId,
        term,
        voteGranted);
  }

  /**
   * Sends a Raft Heartbeat message to all workers via the control topic.
   *
   * @param leaderId ID of the leader
   * @param term Election term
   */
  void sendRaftHeartbeat(String leaderId, long term) {
    Event event = new Event(config.connectGroupId(), new RaftHeartbeat(leaderId, term));
    send(event);
    LOG.debug("RaftWorker {} sent Heartbeat from leader {} for term {}", worker, leaderId, term);
  }

  // ========================================================================
  // Lifecycle Management
  // ========================================================================

  @Override
  void stop() {
    LOG.info("RaftWorker {} stopping, shutting down background polling", worker);
    terminateBackgroundPolling();
    sinkWriter.close();
    super.stop();
    LOG.info("RaftWorker {} stopped", worker);
  }

  private void terminateBackgroundPolling() {
    running.set(false);
    pollingExecutor.shutdownNow();
    try {
      if (!pollingExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.warn(
            "Polling thread did not terminate in time on RaftWorker {}, forcing shutdown", worker);
        throw new ConnectException(
            String.format(
                "Background polling thread of RaftWorker %s did not terminate gracefully", worker));
      }
    } catch (InterruptedException e) {
      LOG.warn("RaftWorker {} interrupted while waiting for polling thread shutdown", worker, e);
      throw new ConnectException(
          String.format("Background polling thread of RaftWorker %s interrupted while closing", worker),
          e);
    }
    commitEventQueue.clear();
    errorRef.set(null);
  }

  void save(Collection<SinkRecord> sinkRecords) {
    sinkWriter.save(sinkRecords);
  }
}
