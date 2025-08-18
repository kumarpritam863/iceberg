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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.CatalogUtils;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.channel.utils.Envelope;
import org.apache.iceberg.connect.channel.utils.KafkaClientFactory;
import org.apache.iceberg.connect.channel.utils.KafkaUtils;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Coordinator {

    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
    private static final String VALID_THROUGH_TS_SNAPSHOT_PROP = "kafka.connect.valid-through-ts";
    private static final Duration POLL_DURATION = Duration.ofSeconds(1);

    private final Catalog catalog;
    private final IcebergSinkConfig config;
    private final ExecutorService exec;
    private final CommitState commitState;
    private final Producer<String, byte[]> controlProducer;
    private final Consumer<String, byte[]> controlConsumer;
    private final String controlTopic;
    private final String producerId;
    private final Map<Integer, Long> controlTopicOffsets = Maps.newHashMap();
    private final AtomicBoolean isCoordinatorStopping = new AtomicBoolean(false);
    private final AtomicInteger coordinatorCommitAttempt = new AtomicInteger(0);
    private final Map<String, Integer> memberMap = Maps.newHashMap();
    private final KafkaClientFactory clientFactory;

    Coordinator(IcebergSinkConfig config) {
        this.controlTopic = config.controlTopic();
        this.catalog = CatalogUtils.loadCatalog(config);
        this.config = config;
        this.clientFactory = new KafkaClientFactory(config.kafkaProps());
        this.exec =
                new ThreadPoolExecutor(
                        config.commitThreads(),
                        config.commitThreads(),
                        config.keepAliveTimeoutInMs(),
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("iceberg-committer" + "-%d")
                                .build());
        this.commitState = new CommitState(config);

        String transactionalId = config.transactionalPrefix() + "coordinator" + config.transactionalSuffix();
        this.controlProducer = clientFactory.createProducer(transactionalId);
        this.controlConsumer = clientFactory.createConsumer(config.coordinatorId() + "-coordinator");
        this.controlConsumer.subscribe(List.of(controlTopic));
        this.producerId = UUID.randomUUID().toString();
    }

    void start(long processNumber) {
        while (!isCoordinatorStopping.get()) {
            try {
                System.out.println("Starting coordinator process = "+ processNumber++);
                process();
            } catch (Exception exception) {
                System.out.println("Coordinator error while processing = " + exception.getMessage());
                isCoordinatorStopping.set(true);
            }
        }
    }

    void process() {
        if (commitState.isCommitIntervalReached()) {
            System.out.println("Commit interval reached. Starting new commit");
            // send out begin commit
            commitState.startNewCommit();
            System.out.println("Started new commit. Now preparing event");
            Event event =
                    new Event(config.coordinatorId(), new StartCommit(commitState.currentCommitId()));
            send(event);
            System.out.println("Commit " + commitState.currentCommitId() + " initiated");
        }

        consumeAvailable();

        if (commitState.isCommitTimedOut()) {
            System.out.println("commit timed out");
            commit("", true);
        }
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    protected void send(Event event) {
        synchronized (controlProducer) {
            controlProducer.beginTransaction();
            try {
                // NOTE: we shouldn't call get() on the future in a transactional context,
                // see docs for org.apache.kafka.clients.producer.KafkaProducer
                controlProducer.send(new ProducerRecord<>(controlTopic, producerId, AvroUtil.encode(event)));
                controlProducer.commitTransaction();
            } catch (Exception e) {
                try {
                    controlProducer.abortTransaction();
                } catch (Exception ex) {
                    System.out.println("Error aborting producer transaction = {" + ex.getMessage() + "}");
                }
                throw e;
            }
        }
    }

    protected void consumeAvailable() {
        System.out.println("Starting consuming for responses from the workers. Current List of connect group ids = {" + commitState.connectGroupIds() + "}");
        ConsumerRecords<String, byte[]> records = controlConsumer.poll(Coordinator.POLL_DURATION);
        System.out.println("polled { " + records.count() + "} records.");
        while (!records.isEmpty()) {
            records.forEach(
                    record -> {
                        // the consumer stores the offsets that corresponds to the next record to consume,
                        // so increment the record offset by one
                        controlTopicOffsets.put(record.partition(), record.offset() + 1);

                        Event event = AvroUtil.decode(record.value());

                        System.out.println("Got event.");

                        if (event.groupId().equals(config.coordinatorId())) {
                            String connectGroupId = new String(record.headers().lastHeader("connect_group_id").value(), StandardCharsets.UTF_8);
                            System.out.println("got event = " + event.payload().type().name() + " from " + connectGroupId);
                            memberMap.computeIfAbsent(connectGroupId, cgid -> members(clientFactory, cgid));
                            System.out.println("Received event of type: {" +  event.type().name() + "}");
                            if (receive(connectGroupId, new Envelope(event, record.partition(), record.offset()))) {
                                System.out.println("Handled event of type: " + event.type().name());
                            }
                        }
                    });
            records = controlConsumer.poll(Coordinator.POLL_DURATION);
        }
    }

    protected boolean receive(String connectGroupId, Envelope envelope) {
        switch (envelope.event().payload().type()) {
            case DATA_WRITTEN:
                commitState.addResponse(connectGroupId, envelope);
                return true;
            case DATA_COMPLETE:
                commitState.addReady(connectGroupId, envelope);
                if (commitState.isCommitReady(connectGroupId, memberMap.computeIfAbsent(connectGroupId, cgid ->members(clientFactory, cgid)))) {
                    commitState.markCommitReadyFor(connectGroupId);
                    commit(connectGroupId, false);
                }
                return true;
        }
        return false;
    }

    private void commit(String connectGroupId, boolean partialCommit) {
        try {
            doCommit(connectGroupId, partialCommit);
            coordinatorCommitAttempt.set(0);
        } catch (Exception e) {
            LOG.warn("Commit failed for attempt {}, will try again next cycle", coordinatorCommitAttempt.incrementAndGet(), e);
        } finally {
            commitState.endCurrentCommit();
        }
    }

    private void doCommit(String connectGroupId, boolean partialCommit) {
        Map<String, Map<TableReference, List<Envelope>>> commitMap = commitState.tableCommitMap(connectGroupId, partialCommit);

        String offsetsJson = offsetsJson();
        OffsetDateTime validThroughTs = commitState.validThroughTs(connectGroupId, partialCommit);

        Tasks.foreach(commitMap.entrySet())
                .executeWith(exec)
                .stopOnFailure()
                .run(entry -> {
                    String groupId = entry.getKey();
                    Map<TableReference, List<Envelope>> tables = entry.getValue();

                    tables.forEach((tableRef, envelopes) ->
                            commitToTable(groupId, tableRef, envelopes, offsetsJson, validThroughTs)
                    );
                });

        // we should only get here if all tables committed successfully...
        commitConsumerOffsets();
        commitState.clearResponses();

        Event event =
                new Event(
                        config.connectGroupId(),
                        new CommitComplete(commitState.currentCommitId(), validThroughTs));
        send(event);

        LOG.info(
                "Commit {} complete, committed to {} table(s), valid-through {}",
                commitState.currentCommitId(),
                commitMap.size(),
                validThroughTs);
    }

    protected void commitConsumerOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
        controlTopicOffsets
                .forEach(
                        (k, v) ->
                                offsetsToCommit.put(new TopicPartition(controlTopic, k), new OffsetAndMetadata(v)));
        controlConsumer.commitSync(offsetsToCommit);
    }

    private String offsetsJson() {
        try {
            return MAPPER.writeValueAsString(controlTopicOffsets);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void commitToTable(
            String connectGroupId,
            TableReference tableReference,
            List<Envelope> envelopeList,
            String offsetsJson,
            OffsetDateTime validThroughTs) {
        TableIdentifier tableIdentifier = tableReference.identifier();
        Table table;
        try {
            table = catalog.loadTable(tableIdentifier);
        } catch (NoSuchTableException e) {
            LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
            return;
        }

        String branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

        Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(connectGroupId, table, branch);

        List<DataWritten> payloads =
                envelopeList.stream()
                        .filter(
                                envelope -> {
                                    Long minOffset = committedOffsets.get(envelope.partition());
                                    return minOffset == null || envelope.offset() >= minOffset;
                                })
                        .map(envelope -> (DataWritten) envelope.event().payload())
                        .collect(Collectors.toList());

        List<DataFile> dataFiles =
                payloads.stream()
                        .filter(payload -> payload.dataFiles() != null)
                        .flatMap(payload -> payload.dataFiles().stream())
                        .filter(dataFile -> dataFile.recordCount() > 0)
                        .filter(distinctByKey(ContentFile::location))
                        .collect(Collectors.toList());

        List<DeleteFile> deleteFiles =
                payloads.stream()
                        .filter(payload -> payload.deleteFiles() != null)
                        .flatMap(payload -> payload.deleteFiles().stream())
                        .filter(deleteFile -> deleteFile.recordCount() > 0)
                        .filter(distinctByKey(ContentFile::location))
                        .collect(Collectors.toList());

        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
        } else {
            if (deleteFiles.isEmpty()) {
                AppendFiles appendOp = table.newAppend();
                if (branch != null) {
                    appendOp.toBranch(branch);
                }
                appendOp.set(String.format(
                        "kafka.connect.offsets.%s.%s", controlTopic, connectGroupId), offsetsJson);
                appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
                if (validThroughTs != null) {
                    appendOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
                }
                dataFiles.forEach(appendOp::appendFile);
                appendOp.commit();
            } else {
                RowDelta deltaOp = table.newRowDelta();
                if (branch != null) {
                    deltaOp.toBranch(branch);
                }
                deltaOp.set(String.format(
                        "kafka.connect.offsets.%s.%s", controlTopic, connectGroupId), offsetsJson);
                deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
                if (validThroughTs != null) {
                    deltaOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
                }
                dataFiles.forEach(deltaOp::addRows);
                deleteFiles.forEach(deltaOp::addDeletes);
                deltaOp.commit();
            }

            Long snapshotId = latestSnapshot(table, branch).snapshotId();
            Event event =
                    new Event(
                            config.connectGroupId(),
                            new CommitToTable(
                                    commitState.currentCommitId(), tableReference, snapshotId, validThroughTs));
            send(event);

            LOG.info(
                    "Commit complete to table {}, snapshot {}, commit ID {}, valid-through {}",
                    tableIdentifier,
                    snapshotId,
                    commitState.currentCommitId(),
                    validThroughTs);
        }
    }

    private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = Maps.newConcurrentMap();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    private Snapshot latestSnapshot(Table table, String branch) {
        if (branch == null) {
            return table.currentSnapshot();
        }
        return table.snapshot(branch);
    }

    private Map<Integer, Long> lastCommittedOffsetsForTable(String connectGroupId, Table table, String branch) {
        Snapshot snapshot = latestSnapshot(table, branch);
        while (snapshot != null) {
            Map<String, String> summary = snapshot.summary();
            String value = summary.get(String.format(
                    "kafka.connect.offsets.%s.%s", controlTopic, connectGroupId));
            if (value != null) {
                TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
                try {
                    return MAPPER.readValue(value, typeRef);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            Long parentSnapshotId = snapshot.parentId();
            snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
        }
        return ImmutableMap.of();
    }

    void terminate() {
        exec.shutdownNow();
        // wait for coordinator termination, else cause the sink task to fail
        try {
            if (!exec.awaitTermination(1, TimeUnit.MINUTES)) {
                throw new ConnectException("Timed out waiting for coordinator shutdown");
            }
        } catch (InterruptedException e) {
            throw new ConnectException("Interrupted while waiting for coordinator shutdown", e);
        }
    }

    public void stop() {
        terminate();
        controlConsumer.close();
        controlProducer.close();
        try {
            ((AutoCloseable) catalog).close();
        } catch (Exception ex) {
            System.out.println("Failed to close the catalog = " + ex.getMessage());
        }
    }

    private int members(KafkaClientFactory clientFactory, String cgid) {
        ConsumerGroupDescription groupDesc;
        try (Admin admin = clientFactory.createAdmin()) {
            groupDesc = KafkaUtils.consumerGroupDescription(cgid, admin);
            return groupDesc.members().stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
        }
    }
}

