# Proposal: Per-TopicPartition Writer Architecture for Kafka Connect

## Abstract

This proposal replaces the current per-table writer model in `iceberg-kafka-connect` with a
per-TopicPartition writer model. The current design keys writers by table name alone, causing all
Kafka partitions assigned to a task to merge into a single writer per table. This creates unbounded
memory growth, zero fault isolation, coarse offset tracking, and expensive rebalancing. The proposed
design keys writers by `(TopicPartition, tableName)`, adds a bounded memory pool with backpressure,
LRU writer eviction, and granular per-partition flushing -- while reusing the existing `IcebergWriter`
and commit coordination protocol unchanged.

---

## 1. Motivation

### 1.1 Problem Statement

The Kafka Connect sink connector for Iceberg has a structural limitation: its writer layer is keyed
only by table name. A single `SinkWriter` per task maintains a `Map<String, RecordWriter>` where the
key is the Iceberg table name. All Kafka partitions assigned to that task share the same per-table
writer. This design has several consequences that become critical at production scale:

**Unbounded memory.** Each `IcebergWriter` wraps a `PartitionedFanoutWriter` which maintains
`Map<PartitionKey, RollingFileWriter>` (one open file per Iceberg partition). Each open Parquet file
buffers up to 128 MB in its row group. With 5 destination tables and 50 Iceberg partitions each,
a single task can hold 250 open Parquet writers consuming up to 32 GB of row group buffers. There is
no memory cap, no eviction, and no backpressure -- the task simply runs out of memory.

**Zero fault isolation.** A corrupt record, schema mismatch, or I/O error in any Kafka partition
throws a `DataException` that kills the entire task. There is no way to quarantine one partition's
problem while continuing to process others.

**Expensive rebalancing.** On partition revocation, `CommitterImpl.close()` calls `stopWorker()`,
which flushes ALL writers for ALL partitions -- including the ones that were NOT revoked. The
retained partitions lose their writer state and must recreate it (catalog access, writer
initialization) after the rebalance completes.

**Coarse offset-to-file mapping.** Offsets are tracked in a flat `Map<TopicPartition, Offset>` but
the written DataFiles have no back-reference to which Kafka partition produced them. This makes it
impossible to identify and clean up files produced by a specific partition on failure.

**Thundering flush.** On `StartCommit`, every writer across every table flushes simultaneously. After
flush, `writers.clear()` discards all writers, forcing full recreation on the next `put()` batch.
This creates I/O spikes at commit boundaries and repeated `catalog.loadTable()` calls.

### 1.2 Scope

This proposal covers changes to the data-writing layer (`SinkWriter`, `IcebergWriter`, and the
`Worker` integration). It does NOT change:

- The Coordinator/Worker control-topic protocol
- The `CommitState`, `Channel`, or `CoordinatorThread` classes
- The `IcebergSinkConnector` or `IcebergSinkTask` APIs
- The `RecordConverter` or `SchemaUtils` classes
- The event types (`StartCommit`, `DataWritten`, `DataComplete`, etc.)

The commit coordination protocol works as-is because `DataWritten` events already carry per-table
file lists, and `DataComplete` events already carry per-TopicPartition offsets. The change is
entirely in how files and offsets are produced.

---

## 2. Current Architecture

### 2.1 Writer Stack

```
CommitterImpl (1 per task)
  └─ Worker (1 per task)
       └─ SinkWriter (1 per task)
            ├─ writers: Map<String, RecordWriter>          key = tableName
            │    └─ IcebergWriter
            │         └─ TaskWriter<Record>
            │              └─ PartitionedFanoutWriter
            │                   └─ Map<PartitionKey, RollingFileWriter>   UNBOUNDED
            └─ sourceOffsets: Map<TopicPartition, Offset>  flat, latest-only
```

### 2.2 Data Flow

```
SinkTask.put(records)
 → CommitterImpl.save(records)
   → Worker.save(records)
     → SinkWriter.save(records)
       → per record:
           routeStatically/Dynamically → writerForTable(tableName)
           → writers.computeIfAbsent(tableName, factory)
           → IcebergWriter.write(record)
             → RecordConverter.convert → TaskWriter.write(row)
```

### 2.3 Flush Flow

```
Worker.receive(StartCommit)
 → SinkWriter.completeWrite()
   → for each RecordWriter: writer.complete()
     → IcebergWriter.flush() → writer.complete() → WriteResult
   → copy sourceOffsets
   → writers.clear()          ← ALL writers destroyed
   → sourceOffsets.clear()
 → build DataWritten events (one per table)
 → build DataComplete event (all TP offsets)
 → send(events, sourceOffsets)
```

### 2.4 Key Code References

| File | Key Line | Issue |
|------|----------|-------|
| `SinkWriter.java:40` | `Map<String, RecordWriter> writers` | Per-table only, no isolation |
| `SinkWriter.java:61` | `writers.clear()` | Destroys all writers after every flush |
| `PartitionedFanoutWriter.java:29` | `Map<PartitionKey, RollingFileWriter> writers` | Unbounded, no eviction |
| `IcebergWriter.java:67` | `throw new DataException(...)` | Kills entire task on any record error |
| `CommitterImpl.java:164` | `stopWorker()` | Flushes ALL data on any partition revocation |
| `Worker.java:74` | `sinkWriter.completeWrite()` | All-or-nothing flush |

---

## 3. Proposed Architecture

### 3.1 Writer Stack (Proposed)

```
CommitterImpl (1 per task, unchanged)
  └─ Worker (modified: delegates to PartitionWriterManager)
       └─ PartitionWriterManager (NEW, replaces SinkWriter)
            ├─ WriterRegistry (NEW)
            │    └─ Map<WriterKey, TopicPartitionWriter>
            │         └─ TopicPartitionWriter (NEW)
            │              ├─ IcebergWriter (EXISTING, unchanged)
            │              ├─ PartitionOffsetTracker (NEW)
            │              └─ WriterMetrics (NEW)
            ├─ MemoryPool (NEW)
            ├─ BackpressureController (NEW)
            ├─ FlushPolicy (NEW)
            └─ RecordRouter (extracted routing logic)
```

### 3.2 Design Principles

1. **Key by (TopicPartition, tableName).** Each Kafka partition gets its own writer per target
   table. Isolation is the default. No shared mutable state between partitions.

2. **Bound memory explicitly.** A `MemoryPool` with a configurable cap tracks aggregate memory
   across all writers. When the pool is exhausted, backpressure engages before OOM.

3. **Backpressure via pause/resume.** Use the Kafka Connect framework's built-in
   `SinkTaskContext.pause(TopicPartition...)` / `resume(TopicPartition...)` to throttle consumption
   when memory is under pressure. This is the mechanism Kafka Connect was designed for.

4. **Evict idle writers.** Writers that haven't received records for a configurable period are
   flushed, closed, and removed. This prevents unbounded growth with dynamic table routing.

5. **Flush granularly.** On partition revocation, flush only the revoked partitions' writers.
   Retained partitions keep their writers and state. On proactive flush, flush only the writers
   that exceed per-writer thresholds.

6. **Reuse across commits.** Writers are NOT destroyed after `completeWrite()`. They flush their
   current files and continue accumulating into new files. No repeated `catalog.loadTable()`.

7. **Preserve the commit protocol.** The `Worker` → `Coordinator` event flow is unchanged.
   `DataWritten` events still carry per-table file lists (aggregated from per-TP writers).
   `DataComplete` events still carry per-TP offsets (now with better precision).

---

## 4. Detailed Component Design

### 4.1 WriterKey

```java
/**
 * Composite key identifying a writer scoped to a single Kafka partition and
 * Iceberg table. This replaces the plain table-name key used in SinkWriter.
 */
public class WriterKey {
  private final TopicPartition topicPartition;
  private final String tableName;

  public WriterKey(TopicPartition topicPartition, String tableName) {
    this.topicPartition = topicPartition;
    this.tableName = tableName;
  }

  // equals and hashCode on both fields
  // toString: "topic-0:db.table"
}
```

**Rationale:** The current cache key is `String tableName`. Multiple Kafka partitions share the same
writer, which is the root cause of every problem listed in the motivation. Keying by
`(TopicPartition, tableName)` provides isolation at the finest useful granularity.

### 4.2 TopicPartitionWriter

```java
/**
 * Isolated writer unit for a single (TopicPartition, table) pair. Wraps an existing
 * IcebergWriter and adds offset tracking, memory accounting, and activity metrics.
 */
class TopicPartitionWriter implements Closeable {
  private final WriterKey key;
  private final IcebergWriter icebergWriter;
  private final PartitionOffsetTracker offsetTracker;
  private final MemoryPool memoryPool;

  // Metrics
  private long recordCount;
  private long estimatedBytes;
  private long lastWriteTimeMs;
  private long createdTimeMs;

  TopicPartitionWriter(
      WriterKey key,
      IcebergWriter icebergWriter,
      MemoryPool memoryPool) {
    this.key = key;
    this.icebergWriter = icebergWriter;
    this.offsetTracker = new PartitionOffsetTracker(key.topicPartition());
    this.memoryPool = memoryPool;
    this.createdTimeMs = System.currentTimeMillis();
    this.lastWriteTimeMs = this.createdTimeMs;
  }

  void write(SinkRecord record) {
    icebergWriter.write(record);
    offsetTracker.track(record);
    recordCount++;
    lastWriteTimeMs = System.currentTimeMillis();
    // Memory accounting: estimate based on record count * average row size
    // or delegate to writer.estimateMemory() if available
  }

  /**
   * Flush the writer and return results with precise offset range.
   * The writer remains open for reuse.
   */
  TopicPartitionWriterResult complete() {
    List<IcebergWriterResult> writerResults = icebergWriter.complete();
    OffsetRange offsetRange = offsetTracker.completeRange();
    long flushedBytes = estimatedBytes;
    resetMetrics();
    memoryPool.release(flushedBytes);
    return new TopicPartitionWriterResult(key, writerResults, offsetRange);
  }

  @Override
  public void close() {
    icebergWriter.close();
    memoryPool.release(estimatedBytes);
  }

  // Accessors for FlushPolicy and eviction decisions
  long recordCount()        { return recordCount; }
  long estimatedBytes()     { return estimatedBytes; }
  long lastWriteTimeMs()    { return lastWriteTimeMs; }
  long ageMs()              { return System.currentTimeMillis() - createdTimeMs; }
  WriterKey key()           { return key; }
}
```

**Key design choice: `complete()` does NOT close the `TopicPartitionWriter`.** It flushes the
current files (via `IcebergWriter.complete()`) and returns results, but the `TopicPartitionWriter`
remains in the registry for reuse. Internally, `IcebergWriter.complete()` calls
`TaskWriter.complete()`, which calls `BaseTaskWriter.close()` -- making the `TaskWriter` unusable.
Therefore, `IcebergWriter.complete()` must call `initNewWriter()` after flush to create a fresh
`TaskWriter` for the next batch. This is the same pattern already used during schema evolution
(`IcebergWriter.java:91-92`). The `Table` reference, `TableReference`, and outer `IcebergWriter`
object are reused -- only the internal `TaskWriter` and `RecordConverter` are recreated. This
eliminates the expensive `catalog.loadTable()` call that the current design incurs after every
`writers.clear()`.

### 4.3 PartitionOffsetTracker

```java
/**
 * Tracks the offset range for records written by a single TopicPartitionWriter.
 * Provides precise offset-to-file mapping for commit coordination and recovery.
 */
class PartitionOffsetTracker {
  private final TopicPartition topicPartition;
  private Long startOffset;
  private Long currentOffset;
  private OffsetDateTime latestTimestamp;

  PartitionOffsetTracker(TopicPartition topicPartition) {
    this.topicPartition = topicPartition;
  }

  void track(SinkRecord record) {
    long offset = record.originalKafkaOffset();
    if (startOffset == null) {
      startOffset = offset;
    }
    // Store offset + 1 (Kafka convention: next offset to consume)
    currentOffset = offset + 1;
    if (record.timestamp() != null) {
      latestTimestamp =
          OffsetDateTime.ofInstant(
              Instant.ofEpochMilli(record.timestamp()), ZoneOffset.UTC);
    }
  }

  /**
   * Returns the completed offset range and resets for the next batch.
   * Returns null if no records were tracked.
   */
  OffsetRange completeRange() {
    if (currentOffset == null) {
      return null;
    }
    OffsetRange range = new OffsetRange(
        topicPartition, startOffset, currentOffset, latestTimestamp);
    startOffset = null;
    currentOffset = null;
    latestTimestamp = null;
    return range;
  }

  Offset currentOffset() {
    return currentOffset == null
        ? Offset.NULL_OFFSET
        : new Offset(currentOffset, latestTimestamp);
  }
}
```

**Improvement over current design:** The current `SinkWriter.sourceOffsets` is a flat map that only
tracks the latest offset per TopicPartition across ALL tables. With `PartitionOffsetTracker`, each
writer knows exactly which offset range it covers. This enables:

- Precise identification of which files correspond to which offsets
- Better recovery: on failure, only the affected offset range needs reprocessing
- Audit trail: each `DataWritten` event could carry the offset range that produced its files

### 4.4 MemoryPool

```java
/**
 * Shared memory pool that tracks aggregate memory usage across all writers in a task.
 * Provides watermark-based signals for backpressure activation.
 *
 * Thread-safe: uses AtomicLong for lock-free accounting.
 */
public class MemoryPool {
  private final long maxBytes;
  private final double highWatermark;
  private final double lowWatermark;
  private final AtomicLong usedBytes = new AtomicLong(0);

  public MemoryPool(long maxBytes, double highWatermark, double lowWatermark) {
    Preconditions.checkArgument(maxBytes > 0, "maxBytes must be positive");
    Preconditions.checkArgument(
        highWatermark > lowWatermark,
        "highWatermark must be greater than lowWatermark");
    this.maxBytes = maxBytes;
    this.highWatermark = highWatermark;
    this.lowWatermark = lowWatermark;
  }

  /**
   * Attempt to reserve memory. Returns true if the reservation succeeded
   * (total usage remains within bounds), false otherwise.
   */
  public boolean tryReserve(long bytes) {
    long current = usedBytes.get();
    if (current + bytes > maxBytes) {
      return false;
    }
    usedBytes.addAndGet(bytes);
    return true;
  }

  /** Release previously reserved memory back to the pool. */
  public void release(long bytes) {
    usedBytes.addAndGet(-bytes);
  }

  public boolean isAboveHighWatermark() {
    return usedBytes.get() > (long) (maxBytes * highWatermark);
  }

  public boolean isBelowLowWatermark() {
    return usedBytes.get() <= (long) (maxBytes * lowWatermark);
  }

  public long usedBytes()      { return usedBytes.get(); }
  public long availableBytes() { return maxBytes - usedBytes.get(); }
  public long maxBytes()       { return maxBytes; }

  public double utilizationRatio() {
    return (double) usedBytes.get() / maxBytes;
  }
}
```

**Why this matters:** The current design has zero memory awareness. The `MemoryPool` provides:

1. A hard cap that prevents OOM regardless of partition cardinality or table count
2. Watermark signals that drive the backpressure controller
3. Utilization metrics for operational monitoring

**Memory estimation strategy:** Each `TopicPartitionWriter` estimates its memory as the Parquet row
group size (`write.parquet.row-group-size-bytes`, default 128 MB) per open Iceberg partition in its
`PartitionedFanoutWriter`. This is a conservative upper bound. A more precise estimate could query
the writer's internal buffer size, but this requires changes to Iceberg core's `BaseTaskWriter`.
The conservative approach is safe and sufficient for backpressure decisions.

### 4.5 BackpressureController

```java
/**
 * Controls Kafka partition consumption rate based on memory pool pressure.
 * Uses the Kafka Connect SinkTaskContext.pause()/resume() mechanism.
 *
 * Strategy:
 *   - When memory exceeds high watermark: pause the TopicPartitions with the
 *     largest accumulated data, then flush their writers.
 *   - When memory drops below low watermark: resume all paused partitions.
 *   - Between watermarks: no action (hysteresis prevents oscillation).
 */
class BackpressureController {
  private final SinkTaskContext context;
  private final MemoryPool memoryPool;
  private final Set<TopicPartition> pausedPartitions = new HashSet<>();

  BackpressureController(SinkTaskContext context, MemoryPool memoryPool) {
    this.context = context;
    this.memoryPool = memoryPool;
  }

  /**
   * Evaluate memory pressure and apply backpressure if needed.
   * Called after each put() batch.
   */
  void evaluate(WriterRegistry registry) {
    if (memoryPool.isAboveHighWatermark() && pausedPartitions.isEmpty()) {
      // Find the TopicPartitions consuming the most memory
      List<TopicPartition> heaviest = registry.topPartitionsByMemory(
          Math.max(1, registry.activePartitionCount() / 4));

      // Flush them to release memory
      for (TopicPartition tp : heaviest) {
        registry.completePartition(tp);
      }

      // If still above high watermark after flush, pause consumption
      if (memoryPool.isAboveHighWatermark()) {
        Set<TopicPartition> toPause = new HashSet<>(heaviest);
        context.pause(toPause.toArray(new TopicPartition[0]));
        pausedPartitions.addAll(toPause);
        LOG.info("Backpressure engaged: paused {} partitions, pool at {:.1f}%",
            toPause.size(), memoryPool.utilizationRatio() * 100);
      }
    }

    if (!pausedPartitions.isEmpty() && memoryPool.isBelowLowWatermark()) {
      context.resume(pausedPartitions.toArray(new TopicPartition[0]));
      LOG.info("Backpressure released: resumed {} partitions, pool at {:.1f}%",
          pausedPartitions.size(), memoryPool.utilizationRatio() * 100);
      pausedPartitions.clear();
    }
  }

  /** Resume all paused partitions (called on shutdown/rebalance). */
  void resumeAll() {
    if (!pausedPartitions.isEmpty()) {
      context.resume(pausedPartitions.toArray(new TopicPartition[0]));
      pausedPartitions.clear();
    }
  }

  Set<TopicPartition> pausedPartitions() {
    return Collections.unmodifiableSet(pausedPartitions);
  }
}
```

**Hysteresis:** The high/low watermark gap prevents oscillation. The controller only pauses when
crossing the high watermark (e.g., 80%) and only resumes when dropping below the low watermark
(e.g., 50%). This is the same pattern used by TCP flow control and Netty's channel backpressure.

### 4.6 FlushPolicy

```java
/**
 * Determines when individual writers should be flushed, independent of the
 * coordinator's global commit cycle.
 *
 * Proactive per-writer flushing provides two benefits:
 * 1. Spreads I/O across time instead of concentrating it at commit boundaries
 * 2. Releases memory back to the pool, reducing backpressure events
 */
class FlushPolicy {
  private final long maxAgeMs;        // max time a writer can accumulate
  private final long maxRecords;      // max records per writer before flush
  private final long maxBytes;        // max estimated bytes per writer before flush

  FlushPolicy(IcebergSinkConfig config) {
    this.maxAgeMs = config.writerFlushMaxAgeMs();
    this.maxRecords = config.writerFlushMaxRecords();
    this.maxBytes = config.writerFlushMaxBytes();
  }

  boolean shouldFlush(TopicPartitionWriter writer) {
    if (maxAgeMs > 0 && writer.ageMs() >= maxAgeMs) {
      return true;
    }
    if (maxRecords > 0 && writer.recordCount() >= maxRecords) {
      return true;
    }
    if (maxBytes > 0 && writer.estimatedBytes() >= maxBytes) {
      return true;
    }
    return false;
  }

  /**
   * Select writers that should be proactively flushed.
   * Called after each put() batch.
   */
  List<WriterKey> selectForFlush(WriterRegistry registry) {
    List<WriterKey> toFlush = new ArrayList<>();
    for (TopicPartitionWriter writer : registry.allWriters()) {
      if (shouldFlush(writer)) {
        toFlush.add(writer.key());
      }
    }
    return toFlush;
  }
}
```

**Relationship to commit interval:** The coordinator's `commitIntervalMs` (default 5 min) controls
when files are committed to Iceberg tables. The `FlushPolicy` controls when individual writers close
their current files and start new ones. A writer flushed by the policy produces completed files that
are held until the next `StartCommit`, then reported in `DataWritten` events.

This means a writer may produce multiple files per commit interval -- which is fine. It keeps memory
bounded and spreads I/O.

### 4.7 WriterRegistry

```java
/**
 * Manages the lifecycle of all TopicPartitionWriters in a task. Provides
 * bounded caching with LRU eviction, idle cleanup, and selective flushing.
 */
class WriterRegistry {
  private final Map<WriterKey, TopicPartitionWriter> writers = new LinkedHashMap<>(
      16, 0.75f, true);  // access-ordered for LRU
  private final IcebergWriterFactory writerFactory;
  private final MemoryPool memoryPool;
  private final int maxWriters;
  private final long idleEvictionMs;

  WriterRegistry(
      IcebergWriterFactory writerFactory,
      MemoryPool memoryPool,
      int maxWriters,
      long idleEvictionMs) {
    this.writerFactory = writerFactory;
    this.memoryPool = memoryPool;
    this.maxWriters = maxWriters;
    this.idleEvictionMs = idleEvictionMs;
  }

  /**
   * Get an existing writer or create a new one for the given key.
   * If the registry is at capacity, the least-recently-used writer is evicted.
   */
  TopicPartitionWriter getOrCreate(WriterKey key, SinkRecord sample, boolean ignoreMissingTable) {
    TopicPartitionWriter writer = writers.get(key);
    if (writer != null) {
      return writer;
    }

    // Evict LRU if at capacity
    if (writers.size() >= maxWriters) {
      evictLRU(1);
    }

    RecordWriter recordWriter = writerFactory.createWriter(
        key.tableName(), sample, ignoreMissingTable);
    writer = new TopicPartitionWriter(key, recordWriter, memoryPool);
    writers.put(key, writer);
    return writer;
  }

  /**
   * Flush ALL writers and return aggregated results.
   * Writers remain in the registry for reuse.
   */
  List<TopicPartitionWriterResult> completeAll() {
    List<TopicPartitionWriterResult> results = new ArrayList<>();
    for (TopicPartitionWriter writer : writers.values()) {
      TopicPartitionWriterResult result = writer.complete();
      if (result.hasData()) {
        results.add(result);
      }
    }
    return results;
  }

  /**
   * Flush only writers belonging to the given TopicPartition.
   * Used during rebalance to flush only revoked partitions.
   */
  List<TopicPartitionWriterResult> completePartition(TopicPartition tp) {
    List<TopicPartitionWriterResult> results = new ArrayList<>();
    List<WriterKey> toRemove = new ArrayList<>();

    for (Map.Entry<WriterKey, TopicPartitionWriter> entry : writers.entrySet()) {
      if (entry.getKey().topicPartition().equals(tp)) {
        TopicPartitionWriterResult result = entry.getValue().complete();
        if (result.hasData()) {
          results.add(result);
        }
        entry.getValue().close();
        toRemove.add(entry.getKey());
      }
    }

    toRemove.forEach(writers::remove);
    return results;
  }

  /**
   * Flush a specific writer by key (for proactive FlushPolicy).
   * Writer remains in registry; only its current files are finalized.
   */
  TopicPartitionWriterResult completeWriter(WriterKey key) {
    TopicPartitionWriter writer = writers.get(key);
    if (writer == null) {
      return null;
    }
    return writer.complete();
  }

  /** Evict writers that have been idle longer than the configured threshold. */
  void evictIdle() {
    long now = System.currentTimeMillis();
    List<WriterKey> idle = new ArrayList<>();
    for (Map.Entry<WriterKey, TopicPartitionWriter> entry : writers.entrySet()) {
      if (now - entry.getValue().lastWriteTimeMs() > idleEvictionMs) {
        idle.add(entry.getKey());
      }
    }
    for (WriterKey key : idle) {
      TopicPartitionWriter writer = writers.remove(key);
      if (writer != null) {
        writer.complete();  // flush any remaining data
        writer.close();
      }
    }
    if (!idle.isEmpty()) {
      LOG.debug("Evicted {} idle writers", idle.size());
    }
  }

  /** Evict the least-recently-used writers to make room. */
  private void evictLRU(int count) {
    Iterator<Map.Entry<WriterKey, TopicPartitionWriter>> it = writers.entrySet().iterator();
    int evicted = 0;
    while (it.hasNext() && evicted < count) {
      Map.Entry<WriterKey, TopicPartitionWriter> entry = it.next();
      entry.getValue().complete();
      entry.getValue().close();
      it.remove();
      evicted++;
    }
    LOG.debug("LRU-evicted {} writers (registry at capacity {})", evicted, maxWriters);
  }

  int size()                       { return writers.size(); }
  int activePartitionCount()       { return /* distinct TPs */ ... ; }
  Collection<TopicPartitionWriter> allWriters() { return writers.values(); }

  void close() {
    for (TopicPartitionWriter writer : writers.values()) {
      writer.close();
    }
    writers.clear();
  }
}
```

**LinkedHashMap with access-order** (constructor parameter `true`) gives us LRU semantics for free.
The least-recently-accessed entries are at the head of the iteration order, so `evictLRU()` simply
iterates from the head.

### 4.8 PartitionWriterManager (replaces SinkWriter)

```java
/**
 * Top-level orchestrator for per-TopicPartition writers. Replaces SinkWriter.
 *
 * Responsibilities:
 * - Route records to the correct (TP, table) writer
 * - Enforce memory bounds and backpressure
 * - Apply proactive flush policy
 * - Support granular per-partition flushing for rebalance
 */
public class PartitionWriterManager {
  private final IcebergSinkConfig config;
  private final WriterRegistry registry;
  private final MemoryPool memoryPool;
  private final BackpressureController backpressure;
  private final FlushPolicy flushPolicy;

  public PartitionWriterManager(
      Catalog catalog,
      IcebergSinkConfig config,
      SinkTaskContext context) {
    this.config = config;
    this.memoryPool = new MemoryPool(
        config.writerMemoryPoolBytes(),
        config.writerMemoryHighWatermark(),
        config.writerMemoryLowWatermark());
    this.registry = new WriterRegistry(
        new IcebergWriterFactory(catalog, config),
        memoryPool,
        config.writerMaxWriters(),
        config.writerIdleEvictionMs());
    this.backpressure = new BackpressureController(context, memoryPool);
    this.flushPolicy = new FlushPolicy(config);
  }

  /**
   * Save a batch of records. Called by Worker on each put().
   */
  public void save(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      save(record);
    }

    // Proactive flush: check if any writers exceed thresholds
    List<WriterKey> toFlush = flushPolicy.selectForFlush(registry);
    for (WriterKey key : toFlush) {
      registry.completeWriter(key);
    }

    // Idle eviction
    registry.evictIdle();

    // Backpressure evaluation
    backpressure.evaluate(registry);
  }

  private void save(SinkRecord record) {
    TopicPartition tp = new TopicPartition(
        record.originalTopic(), record.originalKafkaPartition());

    List<RouteTarget> targets = route(record);

    for (RouteTarget target : targets) {
      WriterKey key = new WriterKey(tp, target.tableName());
      TopicPartitionWriter writer = registry.getOrCreate(
          key, record, target.ignoreMissingTable());
      writer.write(record);
    }
  }

  /**
   * Complete all writers and return aggregated results.
   * Called by Worker on StartCommit. Writers remain open for reuse.
   */
  public PartitionWriterManagerResult completeWrite() {
    List<TopicPartitionWriterResult> tpResults = registry.completeAll();
    return aggregateResults(tpResults);
  }

  /**
   * Complete only writers belonging to the given partitions.
   * Called on rebalance for revoked partitions only.
   * Retained partitions' writers are untouched.
   */
  public PartitionWriterManagerResult completePartitions(
      Collection<TopicPartition> partitions) {
    List<TopicPartitionWriterResult> tpResults = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      tpResults.addAll(registry.completePartition(tp));
    }
    backpressure.resumeAll();  // release any paused partitions being revoked
    return aggregateResults(tpResults);
  }

  /**
   * Aggregate per-(TP, table) results into per-table results (for DataWritten events)
   * and per-TP offsets (for DataComplete events).
   *
   * This is the bridge between the per-TP internal model and the existing
   * per-table commit protocol.
   */
  private PartitionWriterManagerResult aggregateResults(
      List<TopicPartitionWriterResult> tpResults) {

    // Group IcebergWriterResults by table (for DataWritten events)
    Map<TableReference, List<IcebergWriterResult>> byTable = new HashMap<>();
    for (TopicPartitionWriterResult tpResult : tpResults) {
      for (IcebergWriterResult wr : tpResult.writerResults()) {
        byTable.computeIfAbsent(wr.tableReference(), k -> new ArrayList<>()).add(wr);
      }
    }
    // Flatten per-table results
    List<IcebergWriterResult> mergedWriterResults = new ArrayList<>();
    for (Map.Entry<TableReference, List<IcebergWriterResult>> entry : byTable.entrySet()) {
      // Merge DataFile/DeleteFile lists from multiple TPs into one result per table
      List<DataFile> dataFiles = new ArrayList<>();
      List<DeleteFile> deleteFiles = new ArrayList<>();
      StructType partitionStruct = null;
      for (IcebergWriterResult wr : entry.getValue()) {
        dataFiles.addAll(wr.dataFiles());
        deleteFiles.addAll(wr.deleteFiles());
        partitionStruct = wr.partitionStruct();
      }
      mergedWriterResults.add(new IcebergWriterResult(
          entry.getKey(), dataFiles, deleteFiles, partitionStruct));
    }

    // Collect per-TP offsets (max offset per TP across all tables)
    Map<TopicPartition, Offset> sourceOffsets = new HashMap<>();
    for (TopicPartitionWriterResult tpResult : tpResults) {
      if (tpResult.offsetRange() != null) {
        TopicPartition tp = tpResult.key().topicPartition();
        Offset newOffset = tpResult.offsetRange().toOffset();
        sourceOffsets.merge(tp, newOffset, Offset::max);
      }
    }

    return new PartitionWriterManagerResult(mergedWriterResults, sourceOffsets);
  }

  public void close() {
    backpressure.resumeAll();
    registry.close();
  }
}
```

**Critical aggregation step:** The `aggregateResults()` method bridges the per-TP internal model to
the existing commit protocol. `DataWritten` events need per-table file lists (not per-TP), and
`DataComplete` needs per-TP offsets. The aggregation:

1. Groups `IcebergWriterResult` objects by `TableReference`, merging their `DataFile`/`DeleteFile`
   lists -- this is what the Coordinator expects in `DataWritten` events.
2. Computes per-TP offsets by taking the max offset across all tables for each TopicPartition --
   this is what the Coordinator expects in `DataComplete` events.

**No changes to `Worker.receive()` are needed** beyond replacing `SinkWriter` calls with
`PartitionWriterManager` calls.

### 4.9 Worker Changes (Minimal)

The changes to `Worker` are minimal:

```java
class Worker extends Channel {

  // CHANGED: SinkWriter → PartitionWriterManager
  private final PartitionWriterManager writerManager;

  Worker(
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      PartitionWriterManager writerManager,  // CHANGED
      SinkTaskContext context) {
    super("worker", ...);
    this.writerManager = writerManager;
    ...
  }

  @Override
  protected boolean receive(Envelope envelope) {
    ...
    // CHANGED: one method call
    PartitionWriterManagerResult results = writerManager.completeWrite();

    // REST IS IDENTICAL: build DataWritten events from results.writerResults(),
    // build DataComplete from results.sourceOffsets()
    List<TopicPartitionOffset> assignments = ...;  // same logic
    List<Event> events = ...;  // same logic
    send(events, results.sourceOffsets());  // same call
    return true;
  }

  void save(Collection<SinkRecord> sinkRecords) {
    writerManager.save(sinkRecords);  // CHANGED
  }

  void stop() {
    super.stop();
    writerManager.close();  // CHANGED
  }
}
```

### 4.10 CommitterImpl Changes (Granular Rebalance)

The key improvement is in `close(Collection<TopicPartition> closedPartitions)`:

```java
@Override
public void close(Collection<TopicPartition> closedPartitions) {
  if (!isInitialized.get()) {
    LOG.warn("Close unexpectedly called without partition assignment");
    return;
  }

  if (closedPartitions.isEmpty()) {
    // Task stopping: full shutdown
    stopWorker();
    stopCoordinator();
    return;
  }

  // CHANGED: flush only revoked partitions instead of stopping the entire worker
  if (worker != null) {
    worker.completePartitions(closedPartitions);  // NEW: granular flush
  }

  if (hasLeaderPartition(closedPartitions)) {
    stopCoordinator();
  }

  KafkaUtils.seekToLastCommittedOffsets(context);
}
```

**Current behavior:** `stopWorker()` flushes ALL writers, closes the worker, nulls it out.
After rebalance, a new worker must be created from scratch.

**Proposed behavior:** Only the revoked partitions' writers are flushed and removed. The worker
and its retained-partition writers continue operating. This dramatically reduces rebalance cost.

---

## 5. Guarantee Preservation Analysis

This section traces every correctness guarantee the connector currently provides and proves each
is preserved (or strengthened) by the proposed design.

### 5.1 Guarantee Map

The connector's correctness guarantees are implemented in the Channel/Coordinator/CommitState
layer -- NOT in the writer layer. The proposed changes are entirely below the `Worker.receive()`
boundary. This is the fundamental reason all guarantees are preserved.

```
┌──────────────────────────────────────────────────────┐
│  UNCHANGED: Coordinator, Channel, CommitState,       │
│  CoordinatorThread, Event protocol                   │
│  (ALL guarantees enforced here)                      │
├──────────────────────────────────────────────────────┤
│  Worker.receive(StartCommit)                         │
│    │                                                 │
│    │  calls completeWrite()     ← INTERFACE BOUNDARY │
│    │  gets (writerResults, sourceOffsets)             │
│    │  builds DataWritten + DataComplete              │
│    │  calls Channel.send(events, sourceOffsets)       │
├──────────────────────────────────────────────────────┤
│  CHANGED: PartitionWriterManager replaces SinkWriter │
│  (writer lifecycle, routing, memory management)      │
└──────────────────────────────────────────────────────┘
```

The contract between the Worker and the writer layer is:

- **Input**: `save(Collection<SinkRecord>)` -- buffer records
- **Output**: `completeWrite()` returns `(List<IcebergWriterResult>, Map<TopicPartition, Offset>)`

Both the current `SinkWriter` and the proposed `PartitionWriterManager` implement exactly this
contract. The Worker, Channel, Coordinator, and CommitState see no difference.

### 5.2 Per-Guarantee Trace

#### G1: Exactly-Once Semantics

| Mechanism | Code Location | Affected by Change? |
|-----------|---------------|---------------------|
| Transactional producer (begin/commit/abort) | `Channel.java:95-114` | No. Channel is unchanged. |
| `sendOffsetsToTransaction` atomicity | `Channel.java:102-103` | No. Worker still calls `send(events, sourceOffsets)` with same format. |
| Offset validation before table append | `Coordinator.java:275,289` via `offsetValidator()` | No. Coordinator receives same `DataWritten` events. |
| Deduplication by file location | `Coordinator.java:253,261` via `distinctByKey(ContentFile::location)` | No. File locations are unique per `OutputFileFactory` UUID. Per-TP writers use different UUIDs, making locations even MORE unique. Deduplication still works. |
| Commit-ID filtering | `CommitState.java:119` checks `commitId` match | No. Worker still sends `commitId` in `DataWritten`. |

**Verdict: Preserved. No mechanism depends on writer granularity.**

#### G2: Offset Ordering (Monotonic Advance)

| Mechanism | Code Location | Affected by Change? |
|-----------|---------------|---------------------|
| Source offsets: `offset + 1` convention | Currently `SinkWriter.java:83` | Moved to `PartitionOffsetTracker.track()`. Same `+1` convention. Same `Map<TopicPartition, Offset>` output. |
| Offset merge: `Long::max` per partition | `Coordinator.java:232-235` | No. Coordinator is unchanged. |
| Offset filtering: `envelope.offset() >= minOffset` | `Coordinator.java:241-243` | No. Coordinator is unchanged. |
| Offset persistence in snapshot | `Coordinator.java:279,293` | No. Same `snapshotOffsetsProp` written. |

**Important detail**: In the proposed design, multiple `TopicPartitionWriter` instances for the
same `TopicPartition` (but different tables) each track offsets independently. The
`PartitionWriterManager.aggregateResults()` merges them with `Offset::max`, producing the same
final `Map<TopicPartition, Offset>` as the current flat map.

**Verdict: Preserved. Same offset semantics, same output format.**

#### G3: Commit Completeness (All Partitions Required)

| Mechanism | Code Location | Affected by Change? |
|-----------|---------------|---------------------|
| `totalPartitionCount` computed from consumer group | `Coordinator.java:97-98` | No. Coordinator is unchanged. |
| Worker includes ALL assigned TPs in `DataComplete` | `Worker.java:79-90` | No. Worker still iterates `context.assignment()`. |
| `NULL_OFFSET` for TPs with no data | `Worker.java:84-85` | Preserved. `PartitionWriterManager.completeWrite()` returns offsets only for TPs that had data. Worker still maps missing TPs to `NULL_OFFSET` using `context.assignment()`. |
| `isCommitReady(totalPartitionCount)` check | `CommitState.java:112-123` | No. CommitState is unchanged. |

**Verdict: Preserved. The Worker's assignment iteration is unchanged.**

#### G4: Table UUID Validation

| Mechanism | Code Location | Affected by Change? |
|-----------|---------------|---------------------|
| `TableReference` created with `table.uuid()` | `IcebergWriterFactory.java:72-78` | Same factory, same code path. Each per-TP writer creates a `TableReference` with the same table UUID because they load the same table. |
| UUID comparison at commit | `Coordinator.java:217-224` | No. Coordinator compares `tableReference.uuid()` from `DataWritten` against current `table.uuid()`. Per-TP writers for the same table produce the same `TableReference`. |

**Verdict: Preserved.**

#### G5: Commit Idempotency (Offset Validation)

| Mechanism | Code Location | Affected by Change? |
|-----------|---------------|---------------------|
| `SnapshotAncestryValidator` in `offsetValidator()` | `Coordinator.java:322-342` | No. Coordinator is unchanged. |
| `lastCommittedOffsetsForTable()` reads snapshot history | `Coordinator.java:356-376` | No. Same snapshot traversal. |
| Offset persistence enables re-validation | `Coordinator.java:279,293` | No. Same properties written. |

**Verdict: Preserved.**

#### G6: Event Ordering on Control Topic

| Mechanism | Code Location | Affected by Change? |
|-----------|---------------|---------------------|
| All events keyed by `producerId` | `Channel.java:91` | No. Channel is unchanged. |
| `synchronized(producer)` prevents interleaving | `Channel.java:95` | No. Channel is unchanged. |

**Verdict: Preserved.**

#### G7: Valid-Through Timestamp

| Mechanism | Code Location | Affected by Change? |
|-----------|---------------|---------------------|
| Timestamp tracked per offset | Currently `SinkWriter.java:74-77` | Moved to `PartitionOffsetTracker`. Same conversion logic. |
| Min-timestamp computation | `CommitState.java:147-166` | No. CommitState is unchanged. |
| Partial commits null out timestamp | `CommitState.java:149` | No. CommitState is unchanged. |

**Verdict: Preserved.**

#### G8: Offset Recovery on Restart

| Mechanism | Code Location | Affected by Change? |
|-----------|---------------|---------------------|
| `seekToLastCommittedOffsets()` | `KafkaUtils.java:60-85` | No. Called from `CommitterImpl.close()`, which is unchanged in the seek path. |
| Transactional offset commits to consumer group | `Channel.java:102-103` | No. Channel is unchanged. |

**Verdict: Preserved.**

### 5.3 What Could Go Wrong (and Why It Doesn't)

**Concern: Multiple writers for the same table produce different `TableReference` objects.**
Each `TopicPartitionWriter` for the same table calls `IcebergWriterFactory.createWriter()` which
calls `catalog.loadTable()` and captures `table.uuid()`. All writers for the same table get the
same UUID (it's a property of the table, not the writer). The `aggregateResults()` step groups by
`TableReference`, which uses equals/hashCode on `(catalogName, identifier, uuid)`. Writers for the
same table produce the same `TableReference` and are correctly grouped.

**Concern: Per-TP writers produce more `DataFile` objects in `DataWritten` events.**
Yes, but the Coordinator already handles arbitrary numbers of files per `DataWritten` event. The
deduplication (`distinctByKey(ContentFile::location)`) works on file location, which is globally
unique via `OutputFileFactory`'s UUID-based naming. More files is fine -- the Coordinator
iterates them and appends/adds each one.

**Concern: `sendOffsetsToTransaction` receives different offset values.**
The `sourceOffsets` passed to `Channel.send()` is the same `Map<TopicPartition, Offset>` format.
`PartitionWriterManager.aggregateResults()` computes the max offset per TP across all tables,
exactly as the current `SinkWriter` does (it just overwrites with the latest per-record offset,
which is the max since offsets are monotonically increasing within a partition).

### 5.4 Summary

**All 8 guarantees are preserved.** The change is entirely below the Worker/Channel interface
boundary. The Coordinator, CommitState, Channel, and event protocol see no difference between
the current and proposed designs. The only difference is the internal organization of how files
and offsets are produced -- the output contract is identical.

---

## 6. Throughput Analysis

### 6.1 Write Path Comparison

The hot path for record writing is:

```
SinkRecord → RecordConverter.convert() → TaskWriter.write(row)
```

This path is **identical** in both designs. `RecordConverter` and `TaskWriter` are unchanged. The
`IcebergWriter.write()` method is unchanged. The only difference is which `IcebergWriter` instance
a record is routed to.

**Per-record overhead difference**: One additional `HashMap.get()` on `WriterKey` (two-field
composite key) instead of `String` key. This is ~10ns per record -- negligible compared to
the ~1-10μs cost of Parquet serialization.

### 6.2 Commit-Time I/O Analysis

**Current design**: On `StartCommit`, every writer flushes simultaneously. For N tables × P
Iceberg partitions, this is N×P file close operations in a burst. After flush, `writers.clear()`
destroys all writers. On the next `put()`, all writers are recreated.

**Proposed design (without proactive flushing)**: Same burst behavior on `StartCommit`. But writers
are reused across commits (only the internal `TaskWriter` is recreated, not the `IcebergWriter`
itself or the `RecordConverter`). This saves `catalog.loadTable()` calls.

**Proposed design (with proactive flushing)**: Writers that exceed age/size/count thresholds are
flushed before `StartCommit`. This spreads I/O across time, reducing the commit-time burst. The
`StartCommit` flush only handles writers that haven't been proactively flushed.

### 6.3 Writer Reuse: What It Actually Saves

`BaseTaskWriter.complete()` calls `close()`, which closes the underlying `PartitionedFanoutWriter`
and all its per-partition `RollingFileWriter` instances. **After `complete()`, the `TaskWriter` is
not reusable.** A new `TaskWriter` must be created.

Therefore, "writer reuse" means:

| Component | Current (recreated every commit) | Proposed (reused) |
|-----------|----------------------------------|-------------------|
| `IcebergWriter` object | Recreated | Reused |
| `RecordConverter` | Recreated | Reused |
| `Table` reference | Re-fetched via `catalog.loadTable()` | Already held |
| `TableReference` | Re-captured | Already held |
| `TaskWriter` (PartitionedFanoutWriter) | Recreated | Recreated (via `initNewWriter()`) |
| `OutputFileFactory` | Recreated | Recreated (new operation ID) |
| `GenericFileWriterFactory` | Recreated | Recreated |

The biggest savings are:
1. **No `catalog.loadTable()` per table per commit.** This is an RPC to the catalog (REST, Hive
   Metastore, Glue, etc.) that can take 10-100ms. With 5 tables and 5-minute commits, this saves
   5 RPCs per commit cycle, or ~500ms of catalog latency.
2. **No `RecordConverter` re-creation.** Schema introspection and name-mapping are re-done on each
   creation. Reuse eliminates this.

### 6.4 Throughput Impact: File Size vs. Record Count

Raw write throughput (records/sec) is determined by Parquet serialization speed, not writer
granularity. Whether 1 writer or 10 writers serialize 1 million records, the aggregate serialization
work is the same.

The concern is **file size**, not throughput. Smaller files have:
- Higher metadata-to-data ratio (each file has headers, footers, statistics)
- More file handles during reads (scan planning must open more files)
- Higher manifest size (more `DataFile` entries)

These affect **read** performance, not **write** throughput. Write throughput is unaffected by the
number of writers.

### 6.5 Throughput Under Stress

The critical throughput difference is in **sustained operation under stress**:

| Scenario | Current | Proposed |
|----------|---------|----------|
| Memory pressure | OOM crash → task restart (loses 5+ min) | Backpressure → throttle → sustained throughput |
| Schema evolution | Flush entire per-table writer → I/O burst | Flush only affected per-TP writer → smaller burst |
| Rebalance | Flush ALL writers → full restart | Flush only revoked TPs → partial restart |
| Catalog outage during writer recreation | Task fails on first `put()` after commit | Writers reuse held `Table` ref → no catalog call needed |

For workloads that never hit these stress cases, throughput is identical. For workloads that do,
the proposed design maintains throughput while the current design crashes and restarts.

---

## 7. Small Files: Problem Analysis and Solution

### 7.1 Root Cause

When `TaskWriter.complete()` is called, `BaseTaskWriter.close()` closes ALL open files in the
`PartitionedFanoutWriter`. Each `RollingFileWriter` closes its current Parquet file regardless of
how much data it contains. Files with 0 records are deleted (`BaseTaskWriter.java:437-448`), but
files with 1+ records are committed -- no matter how small.

**Current design**: 1 writer per table → all records for that table merge into one file per Iceberg
partition → files are as large as the commit interval allows.

**Proposed design (naive)**: N writers per table (one per Kafka partition) → records for the same
Iceberg partition are split across N files → each file is ~1/N the size.

**Concrete numbers** (5-minute commit interval, 10 Kafka partitions, 1 table, 100K records/sec):
- Current: 50 Iceberg partitions × 1 file each = 50 files, ~600K records per file
- Proposed: 50 Iceberg partitions × 10 TPs = up to 500 files, ~60K records per file

In practice, not all Kafka partitions hit all Iceberg partitions per commit window. With typical
partition affinity, expect 2-3× more files, not 10×.

### 7.2 Solution: Adaptive Writer Consolidation

Rather than accepting more small files and depending on external compaction, we solve this at the
writer level with a throughput-adaptive strategy:

**Principle**: Use per-TP writers only when the throughput justifies it. Low-throughput
TopicPartitions share a per-table writer (current behavior). High-throughput TopicPartitions get
their own isolated writer (proposed behavior).

```java
class WriterRegistry {

  // Two-tier writer structure
  private final Map<String, TopicPartitionWriter> sharedWriters;     // key = tableName
  private final Map<WriterKey, TopicPartitionWriter> isolatedWriters; // key = (TP, tableName)

  // Throughput tracking per (TP, table)
  private final Map<WriterKey, RateTracker> throughputTrackers;

  // Threshold: records per commit interval to qualify for isolation
  private final long isolationThreshold;  // default: 1000 records

  TopicPartitionWriter getOrCreate(WriterKey key, SinkRecord sample, boolean ignoreMissing) {
    RateTracker tracker = throughputTrackers.computeIfAbsent(key, k -> new RateTracker());
    tracker.record();

    if (tracker.recordsSinceLastFlush() >= isolationThreshold) {
      // High throughput: use isolated per-TP writer
      return isolatedWriters.computeIfAbsent(key, k -> createWriter(key, sample, ignoreMissing));
    } else {
      // Low throughput: share per-table writer (current behavior)
      return sharedWriters.computeIfAbsent(
          key.tableName(), k -> createWriter(key, sample, ignoreMissing));
    }
  }
}
```

**How this works**:
1. A new (TP, table) pair starts writing to the **shared** per-table writer (same as current)
2. Once its record count crosses the `isolationThreshold` (default 1000), it is promoted to its
   own **isolated** per-TP writer
3. The shared writer handles all low-throughput TPs, producing fewer, larger files
4. Isolated writers handle high-throughput TPs, providing fault isolation and memory bounding

**File count impact**:
- Low-throughput TPs: same file count as current (1 file per Iceberg partition per table)
- High-throughput TPs: more files, but each is large enough to not be a "small file"
- The threshold ensures that any TP producing its own files has enough data to produce
  reasonably-sized files

**Configuration**:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `iceberg.connect.writer.isolation-threshold` | Long | `1000` | Minimum records per commit interval for a TP to get its own writer. Below this, TPs share a per-table writer. Set to `0` to always isolate (full per-TP). Set to `Long.MAX_VALUE` to never isolate (current behavior). |

### 7.3 Solution: Post-Flush File Coalescing (Optional Enhancement)

As a complementary strategy, the `PartitionWriterManager.aggregateResults()` step can merge
small files targeting the same Iceberg partition:

```java
private List<IcebergWriterResult> coalesceSmallFiles(
    List<IcebergWriterResult> results, long minFileSize) {
  // Group DataFiles by (tableReference, partitionKey)
  // If multiple small files (< minFileSize) target the same partition:
  //   - Read them back
  //   - Write a single merged file
  //   - Delete the originals
  //   - Return the merged file in the result
  // Files above minFileSize are passed through unchanged
}
```

This is an opt-in, more aggressive optimization. It trades CPU (re-reading and re-writing) for
fewer output files. The adaptive writer consolidation (Section 7.2) should make this unnecessary
for most workloads, but it's available for extreme cases.

**Configuration**:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `iceberg.connect.writer.coalesce-min-file-size` | Long | `0` (disabled) | Files smaller than this are candidates for post-flush coalescing. Set to e.g., `8388608` (8 MB) to merge files under 8 MB. |

### 7.4 Comparison Under Realistic Workload

**Scenario**: 10 Kafka partitions, 1 table, 50 Iceberg partitions, 100K records/sec total,
5-minute commit interval, `isolationThreshold = 1000`.

Assume 8 of 10 Kafka partitions each produce >1000 records (high throughput) and 2 produce <1000
(low throughput). Each high-throughput TP touches ~15 Iceberg partitions per window; each
low-throughput TP touches ~5.

| Design | Writers | Files per Commit | Avg Records per File |
|--------|---------|------------------|----------------------|
| Current (per-table) | 1 | 50 | 600,000 |
| Proposed (all per-TP) | 10 | ~130 | ~230,000 |
| **Proposed (adaptive)** | **8 isolated + 1 shared** | **~125** | **~240,000** |

The adaptive design produces ~2.5× more files than the current design, but each file contains
~240K records -- well above the "small file" threshold. The 2 low-throughput TPs share a single
writer, keeping their file output identical to the current design.

---

## 8. Configuration

### 8.1 New Configuration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `iceberg.connect.writer.memory-pool-bytes` | Long | `536870912` (512 MB) | Maximum aggregate memory for all open writers in a task. Prevents OOM by capping total row group buffers. |
| `iceberg.connect.writer.memory-high-watermark` | Double | `0.8` | When pool utilization exceeds this fraction, backpressure engages (pause partitions). |
| `iceberg.connect.writer.memory-low-watermark` | Double | `0.5` | When pool utilization drops below this fraction, backpressure releases (resume partitions). |
| `iceberg.connect.writer.max-writers` | Int | `200` | Maximum number of concurrent `TopicPartitionWriter` instances. Prevents unbounded growth with dynamic routing. LRU eviction when exceeded. |
| `iceberg.connect.writer.idle-eviction-ms` | Long | `300000` (5 min) | Writers with no activity for this duration are flushed and closed. Set to `0` to disable. |
| `iceberg.connect.writer.flush.max-age-ms` | Long | `0` (disabled) | Per-writer age threshold. Writer is flushed when it has been accumulating longer than this. |
| `iceberg.connect.writer.flush.max-records` | Long | `0` (disabled) | Per-writer record count threshold. Writer is flushed after this many records. |
| `iceberg.connect.writer.flush.max-bytes` | Long | `0` (disabled) | Per-writer byte estimate threshold. Writer is flushed when estimated memory exceeds this. |
| `iceberg.connect.writer.isolation-threshold` | Long | `1000` | Minimum records per commit interval for a TopicPartition to get its own isolated writer. Below this, TPs share a per-table writer. `0` = always isolate. `Long.MAX_VALUE` = never isolate (current behavior). |
| `iceberg.connect.writer.coalesce-min-file-size` | Long | `0` (disabled) | Files smaller than this (bytes) are candidates for post-flush coalescing within the same Iceberg partition. `0` = disabled. |

### 8.2 Backward Compatibility

All new properties have defaults that produce behavior similar to the current design:

- `memory-pool-bytes = 512MB` is generous for most workloads
- `max-writers = 200` accommodates 20 Kafka partitions x 10 tables without eviction
- `idle-eviction-ms = 5min` matches the default commit interval
- All flush thresholds default to `0` (disabled), so proactive flushing is opt-in
- `isolation-threshold = 1000` means low-throughput TPs share writers (current file behavior)

Users who do not set any new properties will see improved behavior (bounded memory, backpressure,
writer reuse) without configuration changes. The connector remains a drop-in upgrade.

### 8.3 Existing Properties (Unchanged)

| Property | Default | Interaction |
|----------|---------|-------------|
| `iceberg.control.commit.interval-ms` | `300000` | Still controls coordinator commit frequency. Per-writer flush is independent. |
| `iceberg.control.commit.timeout-ms` | `30000` | Unchanged. |
| `iceberg.tables.write-props.*` | (pass-through) | Still forwarded to Iceberg `TableProperties`. Row group size affects per-writer memory. |

---

## 9. Trade-Off Analysis

### 9.1 File Count and Small Files

The primary trade-off of per-TopicPartition writers is increased file count. Where the current design
produces one set of files per table per commit, the proposed design produces one set per
(TopicPartition, table) per commit.

**Worst case:** 10 Kafka partitions x 5 tables = 50 writer instances vs. 5 writer instances.
Each partitioned table writer may produce files per Iceberg partition it touches. If all 10 Kafka
partitions touch all 50 Iceberg partitions in every commit window:
- Current: 5 tables x 50 files = 250 files
- Proposed: 50 writers x 50 files = 2,500 files

**Realistic case:** In practice, Kafka partitions typically have affinity with a subset of Iceberg
partitions (time-based partitioning, key-based partitioning). Empirically:
- Current: 5 x 50 = 250 files
- Proposed: 50 x ~10 = 500 files (each TP touches ~10 of 50 Iceberg partitions)

**Mitigations:**

1. **Iceberg compaction.** The `RewriteDataFilesAction` is designed for exactly this. Many
   production Iceberg deployments already run periodic compaction. The small files produced by
   per-TP writers are efficiently merged by the compactor.

2. **Longer commit intervals.** Increasing `commit.interval-ms` from 5 to 15 minutes produces 3x
   larger files per writer, offsetting the file multiplication.

3. **Writer reuse across commits.** Because writers are NOT destroyed after `completeWrite()`,
   the `PartitionedFanoutWriter` inside each `IcebergWriter` continues writing into the same
   set of Iceberg partition files across multiple proactive flushes. Only the completed files
   are reported; the writer keeps its open file handles.

4. **Future: post-flush local compaction (optional enhancement).** Before reporting files to the
   coordinator, small files for the same Iceberg partition could be locally merged. This is out
   of scope for this proposal but enabled by the architecture.

### 9.2 Per-Table vs Per-TopicPartition Decision Matrix

| Scenario | Current (per-table) | Proposed (per-TP) | Recommendation |
|----------|--------------------|--------------------|----------------|
| Few Kafka partitions (1-5), few tables (1-3) | Works fine | Equivalent, slight overhead | Either |
| Many Kafka partitions (50+), few tables | OOM risk from Iceberg partition fan-out | Bounded by memory pool | Proposed |
| Dynamic routing with high cardinality | Unbounded writer growth | Capped by max-writers + eviction | Proposed |
| High-cardinality Iceberg partitions (1000+) | OOM guaranteed | Backpressure prevents OOM | Proposed |
| Frequent rebalances | Entire worker destroyed/rebuilt | Only revoked partitions flushed | Proposed |
| Read-heavy workload (file count matters) | Fewer, larger files | More, smaller files | Current |

### 9.3 Throughput Analysis

Per-TP writers receive fewer records than shared per-table writers, so individual Parquet files
may be smaller. However:

- Raw write throughput is identical (same `TaskWriter.write()` path)
- Commit-time I/O is spread more evenly (proactive flushing)
- Writer reuse eliminates repeated `catalog.loadTable()` calls (est. 10-50ms each)
- Backpressure prevents OOM-induced task restarts (each restart loses 5+ minutes)

For sustained throughput over hours, the proposed design is superior because it avoids the
catastrophic failure modes that interrupt the current design.

---

## 10. Compatibility

### 10.1 Wire Protocol Compatibility

The Coordinator receives `DataWritten` events containing `List<DataFile>` and `List<DeleteFile>`,
and `DataComplete` events containing `List<TopicPartitionOffset>`. Both are unchanged:

- `DataWritten` still carries per-table file lists (aggregated from per-TP writers)
- `DataComplete` still carries per-TP offsets

A task running the proposed design can communicate with a coordinator running the current design,
and vice versa. **Rolling upgrades are safe.**

### 10.2 Snapshot Compatibility

The files written to Iceberg tables are standard `DataFile` and `DeleteFile` objects. There is no
change to the file format, metadata, or commit properties. Snapshot properties (`commit-id`,
`task-id`, `valid-through-ts`, `offsets`) are unchanged.

### 10.3 Configuration Compatibility

All existing configuration properties are preserved. New properties have defaults that do not change
observable behavior for users who do not set them (beyond the improvements in memory safety).

---

## 11. Testing Plan

### 11.1 Unit Tests

| Test Class | Coverage |
|------------|----------|
| `TestWriterKey` | equals/hashCode, toString |
| `TestPartitionOffsetTracker` | track, completeRange, reset, null-offset |
| `TestMemoryPool` | reserve, release, watermarks, utilization, edge cases |
| `TestBackpressureController` | engage, release, hysteresis, resumeAll |
| `TestFlushPolicy` | age-based, count-based, bytes-based, disabled thresholds |
| `TestWriterRegistry` | getOrCreate, LRU eviction, idle eviction, completeAll, completePartition, close |
| `TestPartitionWriterManager` | save routing, completeWrite aggregation, completePartitions, backpressure integration |
| `TestTopicPartitionWriter` | write, complete, close, metrics |

### 11.2 Integration Tests

| Test | Scenario |
|------|----------|
| `testPerTPWriterPartitionedTable` | Multiple Kafka partitions writing to day-partitioned table. Verify correct files per partition. |
| `testPerTPWriterRebalance` | Revoke subset of partitions mid-commit. Verify retained partitions continue; revoked partitions' files are committed. |
| `testPerTPWriterBackpressure` | Configure small memory pool. Produce records that exceed pool. Verify partitions are paused and resumed. |
| `testPerTPWriterIdleEviction` | Write to dynamic tables, stop writing to some. Verify idle writers are evicted. |
| `testPerTPWriterSchemaEvolution` | Schema change in one Kafka partition. Verify only that partition's writer is affected. |
| `testPerTPWriterMultiTable` | Records routed to multiple tables. Verify correct aggregation in DataWritten events. |

### 11.3 Compatibility Tests

| Test | Scenario |
|------|----------|
| `testMixedVersionRollingUpgrade` | One task running proposed design, coordinator running current design. Verify commits succeed. |
| `testDefaultConfigMatchesCurrent` | No new config set. Verify file output matches current design (same tables, same schema, same data). |

---

## 12. Implementation Plan

### Phase 1: Core Components (non-breaking)

1. Add `WriterKey`, `PartitionOffsetTracker`, `OffsetRange` value classes
2. Add `MemoryPool` with unit tests
3. Add `FlushPolicy` with unit tests
4. Add `TopicPartitionWriter` wrapping existing `IcebergWriter`
5. Add `WriterRegistry` with LRU eviction and idle eviction

### Phase 2: Integration (swap-in)

6. Add `PartitionWriterManager` with result aggregation
7. Add `BackpressureController`
8. Add new configuration properties to `IcebergSinkConfig`
9. Modify `Worker` to use `PartitionWriterManager` instead of `SinkWriter`
10. Modify `CommitterImpl.close()` for granular partition flushing

### Phase 3: Testing & Validation

11. Unit tests for all new components
12. Integration tests for rebalance, backpressure, eviction scenarios
13. Compatibility tests (rolling upgrade, default config)
14. Performance benchmarking: throughput, memory, file count

### Phase 4: Cleanup

15. Mark `SinkWriter` as `@Deprecated` (keep for one release cycle)
16. Update documentation

---

## 13. Alternatives Considered

### 13.1 Keep Per-Table Writers, Add Memory Limits Only

Add a memory pool and backpressure to the existing per-table writer model without changing the
cache key.

**Rejected because:** This does not solve fault isolation (one partition's error kills the task),
rebalance cost (entire worker destroyed), or offset-to-file traceability. Memory bounding is
necessary but not sufficient.

### 13.2 Per-TopicPartition with ClusteredWriter (Sorted Input)

Use Iceberg's `ClusteredWriter` instead of `PartitionedFanoutWriter` within each
`TopicPartitionWriter`. `ClusteredWriter` keeps only one file open at a time but requires input
pre-sorted by Iceberg partition.

**Rejected because:** Kafka records arrive in offset order, not partition-sorted order. Buffering
and sorting adds complexity and memory (defeating the purpose). The `PartitionedFanoutWriter` is
the correct choice for unsorted input, and the memory pool bounds its aggregate cost.

### 13.3 Shared Writer Pool with Per-TP Routing Metadata

Keep shared per-table writers but tag each record with its source TopicPartition. On flush,
separate the files by source TP using metadata.

**Rejected because:** Iceberg's `TaskWriter` does not support per-record metadata tagging. Files
from different TPs would be interleaved within the same Parquet file, making per-TP recovery
impossible. This would require changes to Iceberg core's writer stack.

### 13.4 Per-TopicPartition with File Coalescing

Per-TP writers, but before reporting to the coordinator, merge small files from different TPs
that target the same Iceberg partition into larger files.

**Deferred (not rejected):** This is a valid optimization that could be added in a follow-up.
The proposed architecture supports it cleanly -- the `PartitionWriterManager.aggregateResults()`
method is the natural place to add a coalescing step. However, it adds complexity and is not
needed for the initial implementation. Iceberg's built-in compaction handles the small files
problem adequately for most workloads.

---

## 14. Summary

| Dimension | Current | Proposed |
|-----------|---------|----------|
| **Memory safety** | Unbounded; OOM on high cardinality | Bounded pool with configurable cap |
| **Backpressure** | None; crash on overload | pause/resume via Kafka Connect API |
| **Fault isolation** | Task-level (one error kills all) | Partition-level (isolated failures) |
| **Rebalance cost** | Flush all, destroy worker | Flush only revoked partitions |
| **Writer reuse** | Destroyed after every commit | IcebergWriter reused; only TaskWriter recreated |
| **Offset precision** | Latest offset per TP (flat map) | Offset range per (TP, table) writer |
| **File count** | Fewer, larger files | Adaptive: shared writers for low-throughput TPs, isolated for high-throughput |
| **Config complexity** | 2 properties | 10 new properties (all with safe defaults) |
| **Exactly-once guarantee** | Preserved | Preserved (all 8 guarantees traced; Section 5) |
| **Protocol changes** | N/A | None (wire-compatible) |
| **Rolling upgrade** | N/A | Safe (same DataWritten/DataComplete format) |
| **Raw write throughput** | Baseline | Identical hot path; saves catalog RPCs on reuse (Section 6) |
| **Small files risk** | Low (shared writers) | Mitigated by adaptive consolidation (Section 7) |
