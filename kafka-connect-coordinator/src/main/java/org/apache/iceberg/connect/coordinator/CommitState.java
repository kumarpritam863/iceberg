/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.iceberg.connect.coordinator;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.channel.utils.Envelope;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitState {
    private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

    private final Map<String, List<Envelope>> commitBuffer = Maps.newHashMap();
    private final Map<String, List<DataComplete>> readyBuffer = Maps.newHashMap();
    private final Map<String, AtomicBoolean> isCommitReadyMap = Maps.newHashMap();
    private long startTime;
    private UUID currentCommitId;
    private final IcebergSinkConfig config;

    public CommitState(IcebergSinkConfig config) {
        this.config = config;
    }

    public void addResponse(String connectGroupId, Envelope envelope) {
        List<Envelope> envelopeForConnectGroupId = commitBuffer.getOrDefault(connectGroupId, Lists.newArrayList());
        envelopeForConnectGroupId.add(envelope);
        commitBuffer.put(connectGroupId, envelopeForConnectGroupId);
        isCommitReadyMap.putIfAbsent(connectGroupId, new AtomicBoolean(false));
        if (!isCommitInProgress()) {
            DataWritten dataWritten = (DataWritten) envelope.event().payload();
            LOG.warn(
                    "Received commit response when no commit in progress, this can happen during recovery. Commit ID: {}",
                    dataWritten.commitId());
        }
    }

    public void addReady(String connectGroupId, Envelope envelope) {
        DataComplete dataComplete = (DataComplete) envelope.event().payload();
        List<DataComplete> dataCompleteListForConnectGroupId = readyBuffer.getOrDefault(connectGroupId, Lists.newArrayList());
        dataCompleteListForConnectGroupId.add(dataComplete);
        readyBuffer.put(connectGroupId, dataCompleteListForConnectGroupId);
        System.out.println("ready buffer after addReady op is " + readyBuffer);
        if (!isCommitInProgress()) {
            LOG.warn(
                    "Received commit ready when no commit in progress, this can happen during recovery. Commit ID: {}",
                    dataComplete.commitId());
        }
    }

    public UUID currentCommitId() {
        return currentCommitId;
    }

    boolean isCommitInProgress() {
        return currentCommitId != null;
    }

    public boolean isCommitIntervalReached() {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }

        if ((!isCommitInProgress()
                && System.currentTimeMillis() - startTime >= config.commitIntervalMs())) {
            isCommitReadyMap.forEach((cgid, isCommitReady) -> isCommitReady.set(false));
            return true;
        }
        return false;
    }

    public void startNewCommit() {
        currentCommitId = UUID.randomUUID();
        startTime = System.currentTimeMillis();
    }

    public void endCurrentCommit() {
        readyBuffer.clear();
        currentCommitId = null;
    }

    public void clearResponses() {
        commitBuffer.clear();
    }

    public boolean isCommitTimedOut() {
        if (!isCommitInProgress()) {
            return false;
        }

        if (System.currentTimeMillis() - startTime > config.commitTimeoutMs()) {
            LOG.info("Commit timeout reached. Commit ID: {}", currentCommitId);
            return true;
        }
        return false;
    }

    public boolean isCommitReady(String connectGroupId, int expectedPartitionCount) {
        System.out.println("checking if commit ready for " + connectGroupId);
        if (!isCommitInProgress()) {
            System.out.println("Commit in progress, commit not ready for " + connectGroupId);
            return false;
        }

        int receivedPartitionCount =
                readyBuffer.getOrDefault(connectGroupId, Lists.newArrayList()).stream()
                        .filter(payload -> payload.commitId().equals(currentCommitId))
                        .mapToInt(payload -> payload.assignments().size())
                        .sum();

        System.out.println("Received partition count for " + connectGroupId + " is " + receivedPartitionCount);

        if (receivedPartitionCount >= expectedPartitionCount) {
            System.out.println(
                    "Commit " + currentCommitId + " ready, received responses for all " + receivedPartitionCount);
            LOG.info(
                    "Commit {} ready, received responses for all {} partitions",
                    currentCommitId,
                    receivedPartitionCount);
            markCommitReadyFor(connectGroupId);
            return true;
        }

        System.out.println("Commit " + currentCommitId + " not ready, received responses for " + receivedPartitionCount + " of " + expectedPartitionCount + " partitions, waiting for more");
        LOG.info(
                "Commit {} not ready, received responses for {} of {} partitions, waiting for more",
                currentCommitId,
                receivedPartitionCount,
                expectedPartitionCount);
        return false;
    }

    public Map<String, Map<TableReference, List<Envelope>>> tableCommitMap(String connectGroupId, boolean partialCommit) {
        if (partialCommit) {
            return collectTimedOutCommits();
        }
        return Map.of(connectGroupId, groupCommits(commitBuffer.getOrDefault(connectGroupId, List.of())));
    }

    private Map<String, Map<TableReference, List<Envelope>>> collectTimedOutCommits() {
        Map<String, Map<TableReference, List<Envelope>>> tableCommitMap = Maps.newHashMap();
        isCommitReadyMap.forEach((cgid, isCommitReady) -> {
            if (!isCommitReady.get()) {
                tableCommitMap.put(cgid, groupCommits(commitBuffer.getOrDefault(cgid, List.of())));
            }
        });
        return tableCommitMap;
    }

    private Map<TableReference, List<Envelope>> groupCommits(List<Envelope> envelopes) {
        return envelopes.stream()
                .collect(Collectors.groupingBy(
                        e -> ((DataWritten) e.event().payload()).tableReference()
                ));
    }


    public OffsetDateTime validThroughTs(String connectGroupId, boolean partialCommit) {
        if (partialCommit) {
            return collectTimedOutValidThroughTs();
        }
        return computeValidThroughTs(connectGroupId, false);
    }

    private OffsetDateTime collectTimedOutValidThroughTs() {
        return isCommitReadyMap.entrySet().stream()
                .filter(entry -> !entry.getValue().get()) // only not ready
                .map(entry -> computeValidThroughTs(entry.getKey(), true))
                .filter(Objects::nonNull)
                .min(Comparator.naturalOrder()) // pick earliest validThrough
                .orElse(null);
    }

    private OffsetDateTime computeValidThroughTs(String connectGroupId, boolean partialCommit) {
        List<DataComplete> events = readyBuffer.getOrDefault(connectGroupId, List.of());

        boolean hasValidThroughTs = !partialCommit &&
                events.stream()
                        .flatMap(e -> e.assignments().stream())
                        .allMatch(offset -> offset.timestamp() != null);

        if (!hasValidThroughTs) {
            return null;
        }

        return events.stream()
                .flatMap(e -> e.assignments().stream())
                .map(TopicPartitionOffset::timestamp)
                .min(Comparator.naturalOrder())
                .orElse(null);
    }


    private void markCommitReadyFor(String connectGroupId) {
        isCommitReadyMap
                .computeIfAbsent(connectGroupId, k -> new AtomicBoolean(false))
                .set(true);
    }

    public Collection<String> connectGroupIds() {
        return isCommitReadyMap.keySet();
    }
}
