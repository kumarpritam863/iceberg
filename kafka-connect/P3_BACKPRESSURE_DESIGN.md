# P3: Rate Limiting & Backpressure — Design Plan

## Context

The iceberg-kafka-connect module has **zero backpressure**. Records flow from `put()` → `SinkWriter` → `IcebergWriter` → `TaskWriter` with no bounds on the number of writers.

### Memory Model (verified code trace)

Records are NOT held fully in memory until commit. The actual write path is:

```
IcebergWriter.write(record)
  → TaskWriter.write(row)
    → PartitionedFanoutWriter.write(row)     — routes to per-partition writer
      → RollingFileWriter.write(row)          — delegates to Parquet
        → ParquetWriter.add(value)            — buffers in writeStore (in-memory column buffers)
          → checkSize()                       — when buffered ≈ 128 MB (row group default)
            → flushRowGroup()                 — WRITES TO DISK incrementally
```

**Per-partition memory:** up to **128 MB** (one Parquet row group buffer, `PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT`).
**The 512 MB `targetFileSize`** only controls when `RollingFileWriter` rolls to a NEW file — data is already streaming to disk in 128 MB chunks.

### What IS unbounded

The real risk is the **number of concurrent writers**, not per-writer memory:

| Unbounded Growth | What Controls It | Memory Per Entry |
|------------------|-----------------|------------------|
| `SinkWriter.writers` map | Distinct table names (dynamic routing) | ~128 MB per partition per table |
| `PartitionedFanoutWriter.writers` map | Distinct partition keys per table | ~128 MB each |

**Realistic worst case:** 10 tables x 50 active partitions each = 500 partition writers x 128 MB = **~64 GB**. This is the actual OOM risk — not records accumulating, but writer fan-out with high-cardinality partitions.

**Secondary risk:** Commit flush IO spike. All writers close simultaneously at commit time, creating a burst of file finalizations and metadata operations.

The Kafka Connect framework provides `SinkTaskContext.pause(TopicPartition...)` and `.resume(TopicPartition...)` — purpose-built for this, but the connector doesn't use them.

---

## Design: Two-Threshold Watermark with Pause/Resume

The design uses a **high/low watermark** pattern — the same pattern used by TCP flow control, Netty's channel backpressure, and Kafka's own `max.poll.records`:

1. When buffered records exceed the **high watermark** → `context.pause()` all assigned partitions
2. When `completeWrite()` flushes and buffered records drop below the **low watermark** → `context.resume()` all partitions
3. Between watermarks → normal operation, no action

This is simple, proven, and requires no changes to the commit coordination protocol.

### Why Records, Not Bytes

Tracking exact bytes in `TaskWriter` is impractical — the writer is an Iceberg internal (`RollingFileWriter`) and doesn't expose its buffer size. But we CAN count records written since last flush, which correlates directly with memory usage.

---

## New Configuration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `iceberg.backpressure.enabled` | Boolean | `false` | Enable backpressure via pause/resume |
| `iceberg.backpressure.max-buffered-records` | Long | `1000000` | High watermark: pause when this many records are buffered |
| `iceberg.backpressure.resume-buffered-records` | Long | `500000` | Low watermark: resume when records drop below this after flush |

**Why off by default?** Pause/resume changes the connector's throughput characteristics. Existing users who have tuned `commit.interval-ms` and memory settings should opt in deliberately.

**Why 1M / 500K defaults?** At ~1KB average record size, 1M records ≈ 1GB. This is a safe default for most deployments with 4GB+ heap.

---

## Architecture

```
IcebergSinkTask.put(records)
  → CommitterImpl.save(records)
    → Worker.save(records)
      → SinkWriter.save(records)
        → for each record:
          → writer.write(record)
          → bufferedRecordCount++        ← NEW: count
          → if (count > highWatermark)   ← NEW: check
            → context.pause(all)         ← NEW: backpressure
            → paused = true

Worker.receive(START_COMMIT)
  → sinkWriter.completeWrite()
    → all writers flush
    → bufferedRecordCount = 0            ← NEW: reset
    → if (paused && count < lowWatermark)← NEW: check
      → context.resume(all)              ← NEW: release
      → paused = false
```

---

## Files to Create (1 new file)

### 1. `BackpressureController.java`
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/BackpressureController.java`

```java
class BackpressureController {
    private final SinkTaskContext context;
    private final boolean enabled;
    private final long highWatermark;
    private final long lowWatermark;
    private final AtomicLong bufferedRecords = new AtomicLong(0);
    private volatile boolean paused = false;

    /** No-op instance for tests and when backpressure is disabled. */
    static final BackpressureController NOOP =
        new BackpressureController(null, false, Long.MAX_VALUE, Long.MAX_VALUE);

    BackpressureController(SinkTaskContext context, IcebergSinkConfig config) {
        this.context = context;
        this.enabled = config.backpressureEnabled();
        this.highWatermark = config.backpressureMaxBufferedRecords();
        this.lowWatermark = config.backpressureResumeBufferedRecords();
    }

    private BackpressureController(
        SinkTaskContext context, boolean enabled, long highWatermark, long lowWatermark) {
        this.context = context;
        this.enabled = enabled;
        this.highWatermark = highWatermark;
        this.lowWatermark = lowWatermark;
    }

    /** Called after each record is written to a table writer. */
    void recordBuffered() {
        long count = bufferedRecords.incrementAndGet();
        if (enabled && !paused && count >= highWatermark) {
            pause();
        }
    }

    /** Called after completeWrite() flushes all writers. */
    void recordsFlushed() {
        bufferedRecords.set(0);
        if (enabled && paused) {
            resume();
        }
    }

    private void pause() {
        Collection<TopicPartition> partitions = context.assignment();
        if (!partitions.isEmpty()) {
            context.pause(partitions.toArray(new TopicPartition[0]));
            paused = true;
            LOG.warn("Backpressure activated: {} records buffered (high watermark: {})",
                     bufferedRecords.get(), highWatermark);
        }
    }

    private void resume() {
        Collection<TopicPartition> partitions = context.assignment();
        if (!partitions.isEmpty()) {
            context.resume(partitions.toArray(new TopicPartition[0]));
            paused = false;
            LOG.info("Backpressure released: records flushed");
        }
    }

    long bufferedRecords() { return bufferedRecords.get(); }
    boolean isPaused() { return paused; }
}
```

**Thread safety:** `bufferedRecords` is `AtomicLong`. `paused` is `volatile`. `pause()` and `resume()` are idempotent (Kafka Connect handles duplicate pause/resume calls gracefully). No locking needed.

**When disabled:** `recordBuffered()` still increments the counter (cheap) but never calls `pause()`. The `NOOP` instance has `enabled=false` and `highWatermark=Long.MAX_VALUE`.

---

## Files to Modify (3 files)

### 2. `IcebergSinkConfig.java`
Add three config properties:
```java
private static final String BACKPRESSURE_ENABLED_PROP = "iceberg.backpressure.enabled";
private static final String BACKPRESSURE_MAX_RECORDS_PROP = "iceberg.backpressure.max-buffered-records";
private static final String BACKPRESSURE_RESUME_RECORDS_PROP = "iceberg.backpressure.resume-buffered-records";
```

Add ConfigDef entries and accessors:
```java
public boolean backpressureEnabled();
public long backpressureMaxBufferedRecords();   // default 1_000_000
public long backpressureResumeBufferedRecords(); // default 500_000
```

Add validation: `resume < max` (otherwise the system would never resume).

### 3. `SinkWriter.java`
Accept `BackpressureController` in constructor, call `recordBuffered()` after each write, `recordsFlushed()` in `completeWrite()`:

```java
public SinkWriter(Catalog catalog, IcebergSinkConfig config,
                  BackpressureController backpressure) {
    this.config = config;
    this.writerFactory = new IcebergWriterFactory(catalog, config);
    this.backpressure = backpressure;
    this.writers = Maps.newHashMap();
    this.sourceOffsets = Maps.newHashMap();
}

private void save(SinkRecord record) {
    // ... existing offset tracking ...
    if (config.dynamicTablesEnabled()) {
      routeRecordDynamically(record);
    } else {
      routeRecordStatically(record);
    }
    backpressure.recordBuffered();
}

public SinkWriterResult completeWrite() {
    List<IcebergWriterResult> writerResults = ...;
    // ... existing flush logic ...
    backpressure.recordsFlushed();
    return new SinkWriterResult(writerResults, offsets);
}
```

### 4. `CommitterImpl.java`
Create `BackpressureController` in `startWorker()`, pass to `SinkWriter`:

```java
private void startWorker() {
    if (null == this.worker) {
        LOG.info("Starting commit worker {}-{}", config.connectorName(), config.taskId());
        BackpressureController backpressure = new BackpressureController(context, config);
        SinkWriter sinkWriter = new SinkWriter(catalog, config, backpressure);
        worker = new Worker(config, clientFactory, sinkWriter, context);
        worker.start();
    }
}
```

---

## Test Files (1 new, 1 modified)

### 5. New: `TestBackpressureController.java`

Tests:
- `testNoopNeverPauses` — NOOP instance, high volume, no pause called
- `testDisabledNeverPauses` — enabled=false via config, no pause called
- `testPauseAtHighWatermark` — records exceed threshold → context.pause() called
- `testResumeAfterFlush` — recordsFlushed() → context.resume() called
- `testNoPauseBeforeHighWatermark` — below threshold → no pause
- `testIdempotentPause` — multiple records above threshold → pause called only once
- `testBufferedRecordCount` — counter tracks correctly
- `testResetOnFlush` — counter resets to 0 after recordsFlushed()
- `testPauseResumeWithEmptyAssignment` — empty assignment doesn't throw

### 6. Modified: `TestSinkWriter.java`
Update constructor calls to pass `BackpressureController.NOOP`.

---

## Implementation Order

1. `IcebergSinkConfig` — add backpressure properties
2. `BackpressureController` — core logic with NOOP singleton
3. `SinkWriter` — accept controller, add calls
4. `CommitterImpl` — create controller, pass to SinkWriter
5. All tests

---

## How It Works End-to-End

### Normal Operation (backpressure off, default)
```
put() → save() → write() → [no limit] → completeWrite() every 5 min
```
Identical to current behavior.

### Backpressure Enabled, Steady State
```
put(1000 records)  → buffered: 1000    → below 1M → continue
put(1000 records)  → buffered: 2000    → below 1M → continue
... (many batches) ...
put(1000 records)  → buffered: 999,500 → below 1M → continue
put(1000 records)  → buffered: 1,000,500 → ABOVE 1M → context.pause()!
                                        → Kafka Connect stops calling put()
... (commit triggered) ...
completeWrite()    → buffered: 0       → below 500K → context.resume()
                                        → Kafka Connect resumes put()
```

### Backpressure + Slow Catalog
```
put(batch) → buffered: 1M → PAUSE
completeWrite() → flush starts → catalog slow → takes 60s
  → but NO new records arriving (paused)
  → memory stable
  → catalog completes → buffered: 0 → RESUME
```

Without backpressure, those 60s would add another 6M records (at 100K/sec) worth of new partition writers, each with up to 128 MB row group buffers.

---

## Verification

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home

./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:spotlessApply
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:build -Pquick=true
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test \
  --tests org.apache.iceberg.connect.data.TestBackpressureController
```

## Backward Compatibility

- `backpressure.enabled` defaults to `false` — zero behavioral change for existing users
- `BackpressureController.NOOP` used in tests — no constructor changes leak into unrelated tests
- No changes to commit coordination, event protocol, or control topic
- No changes to Coordinator — backpressure only affects the ingestion side
- No dependency on ConnectorMetrics — fully standalone feature

---

## Why NOT Other Approaches

| Alternative | Problem |
|-------------|---------|
| Throw exception in `put()` | Kafka Connect retries the same batch forever. No progress. |
| Track bytes instead of records | `TaskWriter` doesn't expose buffer size. Estimating is fragile. |
| Per-table pause | `context.pause()` only works on `TopicPartition`, not tables. Would need complex topic→table→partition mapping. |
| Adaptive commit interval | Would require changes to Coordinator + CommitState, touches the coordination protocol. Good enhancement but much higher risk. |
| `Thread.sleep()` in `put()` | Blocks the Kafka Connect worker thread, can trigger rebalance timeout. |
