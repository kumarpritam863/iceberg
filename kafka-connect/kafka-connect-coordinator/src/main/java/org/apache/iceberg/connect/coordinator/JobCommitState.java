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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.TableReference;

/**
 * Manages commit state for a specific job, tracking data written events
 * and coordinating commit cycles.
 */
public class JobCommitState {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final String connectGroupId;
  private final Duration commitInterval;
  private final Duration commitTimeout;

  private OffsetDateTime lastCommitTime;
  private OffsetDateTime currentCommitStartTime;
  private UUID currentCommitId;
  private final List<Envelope> dataWrittenEnvelopes;
  private final List<Envelope> dataCompleteEnvelopes;
  private boolean commitInProgress;

  public JobCommitState(String connectGroupId, Duration commitInterval) {
    this.connectGroupId = connectGroupId;
    this.commitInterval = commitInterval;
    this.commitTimeout = Duration.ofMinutes(30); // TODO: make configurable
    this.lastCommitTime = OffsetDateTime.now();
    this.dataWrittenEnvelopes = new ArrayList<>();
    this.dataCompleteEnvelopes = new ArrayList<>();
    this.commitInProgress = false;
  }

  public synchronized boolean isCommitIntervalReached() {
    if (commitInProgress) {
      return false;
    }
    return OffsetDateTime.now().isAfter(lastCommitTime.plus(commitInterval));
  }

  public synchronized boolean isCommitTimedOut() {
    if (!commitInProgress) {
      return false;
    }
    return OffsetDateTime.now().isAfter(currentCommitStartTime.plus(commitTimeout));
  }

  public synchronized void startNewCommit() {
    this.currentCommitId = UUID.randomUUID();
    this.currentCommitStartTime = OffsetDateTime.now();
    this.commitInProgress = true;
    this.dataWrittenEnvelopes.clear();
    this.dataCompleteEnvelopes.clear();
  }

  public synchronized void addDataWritten(Envelope envelope) {
    if (commitInProgress) {
      dataWrittenEnvelopes.add(envelope);
    }
  }

  public synchronized void addDataComplete(Envelope envelope) {
    if (commitInProgress) {
      dataCompleteEnvelopes.add(envelope);
    }
  }

  public synchronized boolean isCommitReady() {
    return commitInProgress && !dataCompleteEnvelopes.isEmpty();
  }

  public synchronized Map<TableReference, List<Envelope>> getTableCommitMap() {
    Map<TableReference, List<Envelope>> commitMap = new HashMap<>();

    for (Envelope envelope : dataWrittenEnvelopes) {
      if (envelope.event().payload() instanceof DataWritten) {
        DataWritten dataWritten = (DataWritten) envelope.event().payload();
        TableReference tableRef = dataWritten.tableReference();
        commitMap.computeIfAbsent(tableRef, k -> new ArrayList<>()).add(envelope);
      }
    }

    return commitMap;
  }

  public synchronized String getOffsetsJson() {
    try {
      Map<Integer, Long> offsets = new HashMap<>();
      // Extract offsets from envelopes
      for (Envelope envelope : dataWrittenEnvelopes) {
        offsets.put(envelope.partition(), envelope.offset());
      }
      return MAPPER.writeValueAsString(offsets);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize offsets", e);
    }
  }

  public synchronized OffsetDateTime getValidThroughTimestamp(boolean partialCommit) {
    if (partialCommit) {
      return currentCommitStartTime;
    }
    return OffsetDateTime.now();
  }

  public synchronized void clearCommit() {
    this.commitInProgress = false;
    this.lastCommitTime = OffsetDateTime.now();
    this.dataWrittenEnvelopes.clear();
    this.dataCompleteEnvelopes.clear();
  }

  public synchronized UUID getCurrentCommitId() {
    return currentCommitId;
  }

  public String getConnectGroupId() {
    return connectGroupId;
  }
}