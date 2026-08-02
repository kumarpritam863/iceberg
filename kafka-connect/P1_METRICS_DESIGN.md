# P1: Metrics/Observability — Design Plan

## Context

The iceberg-kafka-connect module has **zero metrics** — only SLF4J logging. Operators can't monitor records/sec, commit latency, error rates, or active writers without parsing logs. This makes production operation effectively blind.

### KIP-KAFKA-15995 (PluginMetrics API)

The KIP introduces `SinkTaskContext.pluginMetrics()` returning a `PluginMetrics` instance for registering connector-specific metrics under `kafka.connect:type=plugins,connector=<name>,task=<id>`. These metrics inherit the runtime's JMX reporters automatically.

**Availability:** Kafka 4.0+ (the iceberg module currently depends on Kafka 3.9.2)

**Design strategy:** Build an internal `ConnectorMetrics` abstraction that:
1. **Attempts** to use `PluginMetrics` from the KIP (via reflection or try-catch on `NoSuchMethodError`)
2. **Falls back** to a no-op implementation on Kafka < 4.0
3. When the Kafka dependency is bumped to 4.0+, the reflection/try-catch path becomes the happy path with zero code changes

This is the exact pattern the KIP's own javadoc recommends:
```java
PluginMetrics pluginMetrics;
try {
    pluginMetrics = context.pluginMetrics();
} catch (NoSuchMethodError | NoClassDefFoundError e) {
    pluginMetrics = null;
}
```

---

## Metric Categories and Specific Metrics

### Tier 1: Operational Essentials (must-have for production)

| Metric Name | Type | Description | Tags | Instrumentation Point |
|-------------|------|-------------|------|-----------------------|
| `records-received-total` | CumulativeCount | Total records received in `put()` | — | `IcebergSinkTask.put()` |
| `records-received-rate` | Rate | Records received per second | — | `IcebergSinkTask.put()` |
| `records-written-total` | CumulativeCount | Records successfully written to Iceberg | `table` | `IcebergWriter.write()` line 64 |
| `records-dropped-total` | CumulativeCount | Records dropped (routing returned empty, or tombstones) | — | `SinkWriter.save()` line 90 |
| `record-conversion-errors-total` | CumulativeCount | Record conversion failures | `table` | `IcebergWriter.write()` catch block line 66 |
| `commit-total` | CumulativeCount | Total Iceberg commits attempted | — | `Coordinator.commit()` line 149 |
| `commit-success-total` | CumulativeCount | Successful commits | — | `Coordinator.doCommit()` after line 185 |
| `commit-failure-total` | CumulativeCount | Failed commits | — | `Coordinator.commit()` catch line 151 |
| `commit-duration-ms` | Avg/Max | Time to execute a full commit cycle | — | `Coordinator.commit()` lines 148-155 |

### Tier 2: Write Path Visibility

| Metric Name | Type | Description | Tags | Instrumentation Point |
|-------------|------|-------------|------|-----------------------|
| `data-files-written-total` | CumulativeCount | Data files produced | `table` | `IcebergWriter.flush()` line 111 |
| `delete-files-written-total` | CumulativeCount | Delete files produced | `table` | `IcebergWriter.flush()` line 112 |
| `data-files-committed-total` | CumulativeCount | Data files committed to table | `table` | `Coordinator.commitToTable()` lines 272-278 |
| `flush-duration-ms` | Avg/Max | Writer flush latency | `table` | `IcebergWriter.flush()` lines 100-113 |
| `active-writers` | Gauge | Currently active RecordWriters | — | `SinkWriter.writers.size()` |
| `commit-table-duration-ms` | Avg/Max | Per-table commit latency | `table` | `Coordinator.commitToTable()` |

### Tier 3: Schema and Table Management

| Metric Name | Type | Description | Tags | Instrumentation Point |
|-------------|------|-------------|------|-----------------------|
| `schema-evolutions-total` | CumulativeCount | Schema evolution events applied | `table` | `IcebergWriter.convertToRow()` line 86 |
| `tables-auto-created-total` | CumulativeCount | Tables auto-created | `table` | `IcebergWriterFactory.autoCreateTable()` line 121 |
| `commit-timeout-total` | CumulativeCount | Commits that timed out waiting for workers | — | `CommitState.isCommitTimedOut()` line 107 |

---

## Architecture

### Component Design

```
SinkTaskContext
    ↓ .pluginMetrics() (Kafka 4.0+, null on 3.x)
    ↓
ConnectorMetrics  ← thin wrapper, handles null PluginMetrics
    ↓
    ├── Sensor("record-ingestion")
    │     ├── records-received-rate
    │     └── records-received-total
    ├── Sensor("record-write") 
    │     ├── records-written-total (per table)
    │     └── records-dropped-total
    ├── Sensor("record-errors")
    │     └── record-conversion-errors-total (per table)
    ├── Sensor("commit")
    │     ├── commit-total
    │     ├── commit-success-total
    │     ├── commit-failure-total
    │     └── commit-duration-ms
    ├── Sensor("write-files") 
    │     ├── data-files-written-total (per table)
    │     ├── delete-files-written-total (per table)
    │     └── flush-duration-ms (per table)
    └── Gauge: active-writers
```

### ConnectorMetrics class

```java
// Package-private, NOT public API
class ConnectorMetrics implements Closeable {

    private final PluginMetrics pluginMetrics;  // nullable on Kafka < 4.0

    // Pre-created sensors (null-safe — all methods no-op when pluginMetrics is null)
    private final Sensor recordIngestion;
    private final Sensor recordErrors;
    private final Sensor commit;
    // ... etc

    // Table-scoped sensors (created lazily per table)
    private final Map<String, Sensor> tableWriteSensors;
    private final Map<String, Sensor> tableFileSensors;

    static ConnectorMetrics create(SinkTaskContext context) {
        PluginMetrics pm = null;
        try {
            pm = context.pluginMetrics();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            LOG.info("PluginMetrics not available (Kafka < 4.0), metrics disabled");
        }
        return new ConnectorMetrics(pm);
    }

    // Convenience methods called from instrumentation points:
    void recordsReceived(int count);
    void recordWritten(String tableName);
    void recordDropped();
    void recordConversionError(String tableName);
    void commitStarted();
    void commitSucceeded(long durationMs);
    void commitFailed(long durationMs);
    void filesWritten(String tableName, int dataFiles, int deleteFiles);
    void flushCompleted(String tableName, long durationMs);
    void schemaEvolved(String tableName);
    void tableAutoCreated(String tableName);
    void commitTimedOut();

    // Gauge registration
    void registerActiveWriters(Supplier<Integer> supplier);
}
```

**Why a wrapper instead of using PluginMetrics directly?**
1. **Null-safety**: When `PluginMetrics` is null (Kafka < 4.0), every method is a no-op. No null checks at call sites.
2. **Convenience API**: `recordWritten("orders")` is much simpler than constructing MetricName + looking up Sensor + recording at each call site
3. **Table-scoped sensors**: Created lazily — the first write to a table creates its sensors with the `table` tag
4. **Single integration point**: When Kafka 4.0 becomes the minimum version, only this class changes

### Wiring — How ConnectorMetrics Flows Through the System

```
IcebergSinkTask.open()
  → CommitterImpl.open(catalog, config, context, partitions)
    → CommitterImpl stores context
      → startWorker()
        → ConnectorMetrics.create(context)  ← creation point
        → SinkWriter(catalog, config, metrics)
          → IcebergWriterFactory(catalog, config, metrics)
            → IcebergWriter(table, ref, config, metrics)
        → Worker(config, clientFactory, sinkWriter, context)

      → startCoordinator()
        → Coordinator(catalog, config, members, clientFactory, context, metrics)
```

`ConnectorMetrics` is created once per task in `CommitterImpl.startWorker()` and passed down the component chain.

---

## Files to Create (2 new files)

### 1. `ConnectorMetrics.java`
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/ConnectorMetrics.java`

The metrics facade. Contains:
- `create(SinkTaskContext)` static factory with PluginMetrics try-catch
- All sensor definitions and registration
- Table-scoped lazy sensor creation
- Convenience recording methods
- `Closeable` implementation

### 2. `TestConnectorMetrics.java`
**Path:** `kafka-connect/kafka-connect/src/test/java/org/apache/iceberg/connect/data/TestConnectorMetrics.java`

Tests:
- `testCreateWithNullPluginMetrics` — verifies no-op behavior on Kafka < 4.0
- `testRecordsReceivedCounterIncrement` — basic counter verification
- `testTableScopedSensorCreation` — lazy sensor per table
- `testDuplicateTableSensorReuse` — same table doesn't create duplicate sensors
- `testConvenienceMethodsNoOpWhenDisabled` — no NPE when metrics disabled
- `testCloseCleanup` — sensors removed on close

## Files to Modify (6 files)

### 3. `CommitterImpl.java`
- Create `ConnectorMetrics` in `startWorker()` from `context`
- Pass to `SinkWriter` and `Coordinator`
- Store as field for lifecycle management

### 4. `SinkWriter.java`
- Accept `ConnectorMetrics` in constructor
- Call `metrics.recordsReceived()` in `save(Collection)`
- Call `metrics.recordDropped()` when router returns empty
- Register `metrics.registerActiveWriters(() -> writers.size())`
- Pass to `IcebergWriterFactory`

### 5. `IcebergWriterFactory.java`
- Accept `ConnectorMetrics` in constructor
- Pass to `IcebergWriter`
- Call `metrics.tableAutoCreated()` in `autoCreateTable()`

### 6. `IcebergWriter.java`
- Accept `ConnectorMetrics` in constructor
- Call `metrics.recordWritten(tableName)` on successful write (line 64)
- Call `metrics.recordConversionError(tableName)` in catch block (line 66)
- Call `metrics.filesWritten(tableName, dataCount, deleteCount)` in `flush()` (line 111-112)
- Call `metrics.flushCompleted(tableName, durationMs)` in `flush()` 
- Call `metrics.schemaEvolved(tableName)` when schema updates detected (line 86)

### 7. `Coordinator.java`
- Accept `ConnectorMetrics` in constructor
- Call `metrics.commitStarted()` in `commit()` (line 149)
- Call `metrics.commitSucceeded(durationMs)` after successful `doCommit()` 
- Call `metrics.commitFailed(durationMs)` in catch block (line 151)
- Call `metrics.commitTimedOut()` in `isCommitTimedOut()` path (line 128)

### 8. `IcebergSinkConfig.java`
No changes needed — metrics don't require connector-level config. The KIP's `PluginMetrics` handles all JMX registration/reporter wiring automatically.

---

## Metric Naming Convention

Following the KIP's naming rules:
- **Group:** `plugins` (set by `PluginMetrics` automatically)
- **Tags (auto-added by KIP):** `connector=<name>`, `task=<id>`
- **Custom tags:** `table=<fully-qualified-table-name>` where applicable
- **Names:** kebab-case, suffixed with measurement type (`-total`, `-rate`, `-ms`)

Example JMX MBean names (Kafka 4.0+):
```
kafka.connect:type=plugins,connector=iceberg-sink,task=0,name=records-received-total
kafka.connect:type=plugins,connector=iceberg-sink,task=0,name=records-received-rate
kafka.connect:type=plugins,connector=iceberg-sink,task=0,table=db.orders,name=records-written-total
kafka.connect:type=plugins,connector=iceberg-sink,task=0,name=commit-duration-ms
```

---

## Implementation Order

1. `ConnectorMetrics` (the facade — no dependencies on modified files)
2. `IcebergSinkConfig` — no changes needed
3. `IcebergWriter` (add metrics parameter + recording calls)
4. `IcebergWriterFactory` (pass metrics through)
5. `SinkWriter` (add metrics parameter + recording calls)
6. `Coordinator` (add metrics parameter + recording calls)
7. `CommitterImpl` (create metrics, pass to SinkWriter + Coordinator)
8. Tests — `TestConnectorMetrics` + update existing tests to pass mock/no-op metrics

---

## Backward Compatibility

- **Kafka < 4.0 (current 3.9.2):** `ConnectorMetrics.create()` catches `NoSuchMethodError`, returns wrapper with null `PluginMetrics`. All convenience methods become no-ops. Zero overhead.
- **Kafka 4.0+:** Metrics are automatically registered under `kafka.connect:type=plugins` and inherit all configured metric reporters (JMX, Prometheus, etc.)
- **No new connector config:** The KIP handles all metric infrastructure. No `iceberg.metrics.*` properties needed.
- **Existing tests:** Updated constructors pass `ConnectorMetrics.NOOP` (a static no-op singleton) to avoid test complexity.

---

## Why This Design Over Alternatives

| Alternative | Problem |
|-------------|---------|
| Create own `Metrics` instance | Doesn't inherit JMX reporters, creates duplicate registries, KIP explicitly warns against this |
| Use Micrometer/Dropwizard directly | Third-party dependency, doesn't integrate with Kafka Connect's metric reporters |
| JMX MBeans directly | Duplicates what the KIP provides, no auto-cleanup, manual reporter wiring |
| Wait for Kafka 4.0 hard requirement | Users on 3.x get nothing; the no-op fallback costs nothing |

---

## Verification

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home

# Build
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:build -Pquick=true

# Test
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test

# Spotless
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:spotlessApply
```

---

## Future Extensions

- **Per-table commit latency:** Add `commit-table-duration-ms` sensor with `table` tag in `commitToTable()`
- **Bytes written:** Sum `DataFile.fileSizeInBytes()` in flush for `bytes-written-total`
- **Routing-specific metrics:** Records routed by each router type
- **Histogram percentiles:** When Kafka adds histogram support to PluginMetrics, add p50/p95/p99 for latencies
- **Health check gauge:** Binary 0/1 for coordinator alive status
