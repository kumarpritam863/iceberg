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
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * Raft-based Worker implementation that handles Raft consensus messages.
 *
 * <p>This worker extends the base Channel class to handle:
 * <ul>
 *   <li>Regular commit cycle messages (START_COMMIT)
 *   <li>Raft consensus messages (RequestVote, VoteResponse, Heartbeat)
 * </ul>
 */
class RaftWorker extends Channel {

  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final SinkWriter sinkWriter;
  private final RaftCommitterImpl committer;

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

    this.config = config;
    this.context = context;
    this.sinkWriter = sinkWriter;
    this.committer = committer;
  }

  @Override
  public void process() {
    consumeAvailable(Duration.ZERO);
  }

  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    PayloadType payloadType = event.payload().type();

    switch (payloadType) {
      case START_COMMIT:
        return handleStartCommit(event);

      case RAFT_REQUEST_VOTE:
        return handleRaftRequestVote(event);

      case RAFT_VOTE_RESPONSE:
        return handleRaftVoteResponse(event);

      case RAFT_HEARTBEAT:
        return handleRaftHeartbeat(event);

      default:
        return false;
    }
  }

  private boolean handleStartCommit(Event event) {
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

    return true;
  }

  private boolean handleRaftRequestVote(Event event) {
    RaftRequestVote payload = (RaftRequestVote) event.payload();
    committer.handleRaftRequestVote(payload.candidateId(), payload.term());
    return true;
  }

  private boolean handleRaftVoteResponse(Event event) {
    RaftVoteResponse payload = (RaftVoteResponse) event.payload();
    committer.handleRaftVoteResponse(payload.voterId(), payload.term(), payload.voteGranted());
    return true;
  }

  private boolean handleRaftHeartbeat(Event event) {
    RaftHeartbeat payload = (RaftHeartbeat) event.payload();
    committer.handleRaftHeartbeat(payload.leaderId(), payload.term());
    return true;
  }

  /**
   * Sends a Raft RequestVote message to all workers via the control topic.
   *
   * @param candidateId ID of the candidate requesting votes
   * @param term Election term
   */
  void sendRaftRequestVote(String candidateId, long term) {
    Event event = new Event(config.connectGroupId(), new RaftRequestVote(candidateId, term));
    send(event);
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
  }

  @Override
  void stop() {
    super.stop();
    sinkWriter.close();
  }

  void save(Collection<SinkRecord> sinkRecords) {
    sinkWriter.save(sinkRecords);
  }
}
