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

package org.apache.iceberg.connect.channel;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceOffsetBackingStore implements IcebergOffsetBackingStore{

    private static final Logger log = LoggerFactory.getLogger(SourceOffsetBackingStore.class);
    public static SourceOffsetBackingStore withConnectorAndWorkerStores(
            Supplier<LoggingContext> loggingContext,
            OffsetBackingStore workerStore,
            KafkaOffsetBackingStore connectorStore,
            String connectorOffsetsTopic,
            TopicAdmin connectorStoreAdmin
    ) {
        Objects.requireNonNull(loggingContext);
        Objects.requireNonNull(workerStore);
        Objects.requireNonNull(connectorStore);
        Objects.requireNonNull(connectorOffsetsTopic);
        Objects.requireNonNull(connectorStoreAdmin);
        return new SourceOffsetBackingStore(
                Time.SYSTEM,
                loggingContext,
                connectorOffsetsTopic,
                workerStore,
                connectorStore,
                connectorStoreAdmin
        );
    }

    public static SourceOffsetBackingStore withOnlyWorkerStore(
            Supplier<LoggingContext> loggingContext,
            OffsetBackingStore workerStore,
            String workerOffsetsTopic
    ) {
        Objects.requireNonNull(loggingContext);
        Objects.requireNonNull(workerStore);
        return new SourceOffsetBackingStore(Time.SYSTEM, loggingContext, workerOffsetsTopic, workerStore, null, null);
    }

    public static SourceOffsetBackingStore withOnlyConnectorStore(
            Supplier<LoggingContext> loggingContext,
            KafkaOffsetBackingStore connectorStore,
            String connectorOffsetsTopic,
            TopicAdmin connectorStoreAdmin
    ) {
        Objects.requireNonNull(loggingContext);
        Objects.requireNonNull(connectorOffsetsTopic);
        Objects.requireNonNull(connectorStoreAdmin);
        return new SourceOffsetBackingStore(
                Time.SYSTEM,
                loggingContext,
                connectorOffsetsTopic,
                null,
                connectorStore,
                connectorStoreAdmin
        );
    }

    private final Time time;
    private final Supplier<LoggingContext> loggingContext;
    private final String primaryOffsetsTopic;
    private final Optional<OffsetBackingStore> workerStore;
    private final Optional<KafkaOffsetBackingStore> connectorStore;
    private final Optional<TopicAdmin> connectorStoreAdmin;

    SourceOffsetBackingStore(
            Time time,
            Supplier<LoggingContext> loggingContext,
            String primaryOffsetsTopic,
            OffsetBackingStore workerStore,
            KafkaOffsetBackingStore connectorStore,
            TopicAdmin connectorStoreAdmin
    ) {
        if (workerStore == null && connectorStore == null) {
            throw new IllegalArgumentException("At least one non-null offset store must be provided");
        }
        this.time = time;
        this.loggingContext = loggingContext;
        this.primaryOffsetsTopic = primaryOffsetsTopic;
        this.workerStore = Optional.ofNullable(workerStore);
        this.connectorStore = Optional.ofNullable(connectorStore);
        this.connectorStoreAdmin = Optional.ofNullable(connectorStoreAdmin);
    }

    public String primaryOffsetsTopic() {
        return primaryOffsetsTopic;
    }

    @Override
    public void start() {
        // Worker offset store should already be started
        connectorStore.ifPresent(OffsetBackingStore::start);
    }

    @Override
    public void stop() {
        // Worker offset store should not be stopped as it may be used for multiple connectors
        connectorStore.ifPresent(OffsetBackingStore::stop);
        connectorStoreAdmin.ifPresent(TopicAdmin::close);
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        Future<Map<ByteBuffer, ByteBuffer>> workerGetFuture = getFromStore(workerStore, keys);
        Future<Map<ByteBuffer, ByteBuffer>> connectorGetFuture = getFromStore(connectorStore, keys);

        return new Future<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                // Note the use of | instead of || here; this causes cancel to be invoked on both futures,
                // even if the first call to cancel returns true
                return workerGetFuture.cancel(mayInterruptIfRunning)
                        | connectorGetFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return workerGetFuture.isCancelled()
                        || connectorGetFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return workerGetFuture.isDone()
                        && connectorGetFuture.isDone();
            }

            @Override
            public Map<ByteBuffer, ByteBuffer> get() throws InterruptedException, ExecutionException {
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>(workerGetFuture.get());
                result.putAll(connectorGetFuture.get());
                return result;
            }

            @Override
            public Map<ByteBuffer, ByteBuffer> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                long timeoutMs = unit.toMillis(timeout);
                long endTime = time.milliseconds() + timeoutMs;
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>(workerGetFuture.get(timeoutMs, unit));
                timeoutMs = Math.max(1, endTime - time.milliseconds());
                result.putAll(connectorGetFuture.get(timeoutMs, TimeUnit.MILLISECONDS));
                return result;
            }
        };
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
        final OffsetBackingStore primaryStore;
        final OffsetBackingStore secondaryStore;
        if (connectorStore.isPresent()) {
            primaryStore = connectorStore.get();
            secondaryStore = workerStore.orElse(null);
        } else if (workerStore.isPresent()) {
            primaryStore = workerStore.get();
            secondaryStore = null;
        } else {
            // Should never happen since we check for this case in the constructor, but just in case, this should
            // be more informative than the NPE that would otherwise be thrown
            throw new IllegalStateException("At least one non-null offset store must be provided");
        }

        Map<ByteBuffer, ByteBuffer> regularOffsets = new HashMap<>();
        Map<ByteBuffer, ByteBuffer> tombstoneOffsets = new HashMap<>();
        values.forEach((partition, offset) -> {
            if (offset == null) {
                tombstoneOffsets.put(partition, null);
            } else {
                regularOffsets.put(partition, offset);
            }
        });

        if (secondaryStore != null && !tombstoneOffsets.isEmpty()) {
            return new ChainedOffsetWriteFuture(
                    primaryStore,
                    secondaryStore,
                    values,
                    regularOffsets,
                    tombstoneOffsets,
                    callback
            );
        } else {
            return setPrimaryThenSecondary(primaryStore, secondaryStore, values, regularOffsets, callback);
        }
    }

    private Future<Void> setPrimaryThenSecondary(
            OffsetBackingStore primaryStore,
            OffsetBackingStore secondaryStore,
            Map<ByteBuffer, ByteBuffer> completeOffsets,
            Map<ByteBuffer, ByteBuffer> nonTombstoneOffsets,
            Callback<Void> callback
    ) {
        return primaryStore.set(completeOffsets, (primaryWriteError, ignored) -> {
            if (secondaryStore != null) {
                if (primaryWriteError != null) {
                    log.trace("Skipping offsets write to secondary store because primary write has failed", primaryWriteError);
                } else {
                    try {
                        // Invoke OffsetBackingStore::set but ignore the resulting future; we don't block on writes to this
                        // backing store.
                        secondaryStore.set(nonTombstoneOffsets, (secondaryWriteError, ignored2) -> {
                            try (LoggingContext context = loggingContext()) {
                                if (secondaryWriteError != null) {
                                    log.warn("Failed to write offsets to secondary backing store", secondaryWriteError);
                                } else {
                                    log.debug("Successfully flushed offsets to secondary backing store");
                                }
                            }
                        });
                    } catch (Exception e) {
                        log.warn("Failed to write offsets to secondary backing store", e);
                    }
                }
            }
            try (LoggingContext context = loggingContext()) {
                callback.onCompletion(primaryWriteError, ignored);
            }
        });
    }

    @Override
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        Set<Map<String, Object>> partitions = new HashSet<>();
        workerStore.ifPresent(offsetBackingStore -> partitions.addAll(offsetBackingStore.connectorPartitions(connectorName)));
        connectorStore.ifPresent(offsetBackingStore -> partitions.addAll(offsetBackingStore.connectorPartitions(connectorName)));
        return partitions;
    }

    @Override
    public void configure(Map<String, Object> config) {

    }

    // For testing
    public boolean hasConnectorSpecificStore() {
        return connectorStore.isPresent();
    }

    // For testing
    public boolean hasWorkerGlobalStore() {
        return workerStore.isPresent();
    }

    private LoggingContext loggingContext() {
        LoggingContext result = loggingContext.get();
        Objects.requireNonNull(result);
        return result;
    }

    private static Future<Map<ByteBuffer, ByteBuffer>> getFromStore(Optional<? extends OffsetBackingStore> store, Collection<ByteBuffer> keys) {
        return store.map(s -> s.get(keys)).orElseGet(() -> CompletableFuture.completedFuture(Collections.emptyMap()));
    }

    private class ChainedOffsetWriteFuture implements Future<Void> {

        private final OffsetBackingStore primaryStore;
        private final OffsetBackingStore secondaryStore;
        private final Map<ByteBuffer, ByteBuffer> completeOffsets;
        private final Map<ByteBuffer, ByteBuffer> regularOffsets;
        private final Callback<Void> callback;
        private final AtomicReference<Throwable> writeError;
        private final CountDownLatch completed;

        public ChainedOffsetWriteFuture(
                OffsetBackingStore primaryStore,
                OffsetBackingStore secondaryStore,
                Map<ByteBuffer, ByteBuffer> completeOffsets,
                Map<ByteBuffer, ByteBuffer> regularOffsets,
                Map<ByteBuffer, ByteBuffer> tombstoneOffsets,
                Callback<Void> callback
        ) {
            this.primaryStore = primaryStore;
            this.secondaryStore = secondaryStore;
            this.completeOffsets = completeOffsets;
            this.regularOffsets = regularOffsets;
            this.callback = callback;
            this.writeError = new AtomicReference<>();
            this.completed = new CountDownLatch(1);

            secondaryStore.set(tombstoneOffsets, this::onFirstWrite);
        }

        private void onFirstWrite(Throwable error, Void ignored) {
            if (error != null) {
                log.trace("Skipping offsets write to primary store because secondary tombstone write has failed", error);
                try (LoggingContext context = loggingContext()) {
                    callback.onCompletion(error, ignored);
                    writeError.compareAndSet(null, error);
                    completed.countDown();
                }
                return;
            }
            setPrimaryThenSecondary(primaryStore, secondaryStore, completeOffsets, regularOffsets, this::onSecondWrite);
        }

        private void onSecondWrite(Throwable error, Void ignored) {
            callback.onCompletion(error, ignored);
            writeError.compareAndSet(null, error);
            completed.countDown();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return completed.getCount() == 0;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            completed.await();
            if (writeError.get() != null) {
                throw new ExecutionException(writeError.get());
            }
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (!completed.await(timeout, unit)) {
                throw new TimeoutException("Failed to complete offset write in time");
            }
            if (writeError.get() != null) {
                throw new ExecutionException(writeError.get());
            }
            return null;
        }
    }
}
