# Iceberg Kafka Connect — Deep Analysis & Feature Roadmap

## Current Routing Architecture

The routing lives entirely in `SinkWriter.java` (141 lines) and operates in **two mutually exclusive modes**:

### Mode 1 — Static Routing (`routeRecordStatically`, line 89)
- No route field → broadcast to ALL configured tables
- With route field → extract value, regex-match per table (`iceberg.table.<name>.route-regex`)
- Tables must be pre-declared in `iceberg.tables`

### Mode 2 — Dynamic Routing (`routeRecordDynamically`, line 117)
- Extracts a single field value → lowercased → used as table identifier
- One field, one dimension, one table per record
- Supports auto-create

**Critical limitation**: These modes are **mutually exclusive** (validated at line 278 of `IcebergSinkConfig.java`). You cannot combine them.

---

## The Routing Gap: What's Missing

The current routing is fundamentally **single-dimensional** — one field determines the destination.

### 1. Multi-Field Composite Routing
Currently you can only route on ONE field. Real-world use cases need:
```
route by: region + event_type + schema_version
→ db.us_east.clicks_v2, db.eu_west.impressions_v1
```

### 2. Expression-Based Routing
No support for computed routes like:
```
IF record.amount > 10000 THEN "high_value_transactions"
ELSE IF record.currency IN ("USD","EUR") THEN "major_currency"
ELSE "other"
```

### 3. Topic-Aware Routing
The current router ignores `record.topic()` entirely. Many users want topic→table mapping (e.g., `orders-v2` topic → `analytics.orders` table).

### 4. Header-Based Routing
Kafka record headers are completely ignored. Headers are a standard mechanism for routing metadata in event-driven architectures.

### 5. Key-Based Routing
`record.key()` is never used for routing decisions.

### 6. Fallback/Default Table
In static mode with regex, if no regex matches, the record is **silently dropped** (line 103-113). There's no fallback/catch-all table.

### 7. One-to-Many Dynamic Routing
Dynamic mode routes each record to exactly one table. No way to fan out a single record to multiple dynamically-determined tables.

---

## Proposed Solution: Pluggable `RecordRouter` Interface

The most flexible and backward-compatible design — a pluggable `RecordRouter` interface with built-in implementations.

### Interface Design

```java
public interface RecordRouter extends Configurable, Closeable {
  List<RouteTarget> route(SinkRecord record);
}

public class RouteTarget {
  String tableName;
  boolean ignoreMissingTable;
}
```

### Built-in Implementations

| Router | Description |
|--------|-------------|
| `StaticRouter` | Current static behavior (backward compat) |
| `DynamicFieldRouter` | Current dynamic behavior (backward compat) |
| `TopicNameRouter` | Maps topic name → table name with prefix/suffix/regex transforms |
| `CompositeFieldRouter` | Multi-field routing with configurable template |
| `HeaderRouter` | Routes based on Kafka header values |
| `ExpressionRouter` | CEL/SpEL/simple expression for conditional routing |
| `ChainedRouter` | Composes multiple routers: first match wins, or union |

### Injection Point

Replace the `if/else` in `SinkWriter.save()` (line 82-86) with:
```java
List<RouteTarget> destinations = router.route(record);
for (RouteTarget dest : destinations) {
    writerForTable(dest.tableName(), record, dest.ignoreMissingTable()).write(record);
}
```

---

## Other High-Value Missing Features

### 1. Dead Letter Queue (DLQ) — HIGH VALUE
**Current state:** Conversion errors in `IcebergWriter.java:66-75` throw `DataException` which crashes the task.

**What's needed:**
- Configurable error tolerance (`errors.tolerance=all`)
- DLQ topic for failed records with error context in headers
- Error count metrics and thresholds
- Per-record error handling instead of task-level failure

### 2. Metrics/Observability — HIGH VALUE
**Current state:** Only SLF4J logging. Zero JMX/Micrometer metrics.

**What's needed:**
- Records received/written/failed per table
- Commit latency, commit size (files, bytes)
- Schema evolution event counts
- Write throughput (records/sec, bytes/sec)
- Coordinator election events
- File count and sizes per commit

### 3. Record Filtering/Predicate — MEDIUM-HIGH VALUE
**Current state:** All non-null records are written. The only "filter" is the route-regex tied to routing.

**What's needed:**
- Per-table record predicates
- Global filter (skip records before routing)
- Useful for: filtering test data, dropping PII fields, sampling

### 4. Rate Limiting / Backpressure — MEDIUM VALUE
**Current state:** No rate limiting. If Iceberg writes slow down, records pile up unbounded.

**What's needed:**
- Max in-flight bytes/records per writer
- Configurable flush thresholds
- Backpressure signal via `context.pause()`/`context.resume()`

### 5. Schema Registry Integration — MEDIUM VALUE
**Current state:** Schema inference from Kafka Connect's deserialized objects. No native Schema Registry awareness.

### 6. Table Maintenance Operations — MEDIUM VALUE
**Current state:** Only appends data. No maintenance.

**What's needed:**
- Periodic compaction trigger
- Snapshot expiration
- Orphan file cleanup

### 7. Column Projection/Selection — MEDIUM VALUE
**Current state:** All fields from the record are written.

**What's needed:**
- Include/exclude field lists per table
- Field renaming at write time
- Computed/derived columns

### 8. Watermark-Based Commit Triggers — MEDIUM VALUE
**Current state:** Commits are purely time-interval based.

**What's needed:**
- Commit on record count threshold
- Commit on bytes-written threshold
- Commit on event-time watermark advancement

---

## Priority Ranking

| Priority | Feature | Effort | Impact |
|----------|---------|--------|--------|
| **P0** | Pluggable RecordRouter + TopicRouter | Medium | Unlocks all routing + most common need |
| **P1** | Dead letter queue | Medium | Production-critical |
| **P1** | Metrics/observability | Medium | Production-critical |
| **P1** | Composite field router | Low | Multi-dimensional routing |
| **P2** | Header-based router | Low | Event-driven architecture support |
| **P2** | Record filtering | Medium | Data quality |
| **P2** | Watermark-based commits | Medium | Write optimization |
| **P3** | Rate limiting | Medium | Stability under load |
| **P3** | Table maintenance | Medium | Operational overhead reduction |
| **P3** | Column projection | Low | Flexibility |

---

## Key Files Reference

| Component | Path |
|-----------|------|
| Routing | `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/SinkWriter.java` |
| Config | `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/IcebergSinkConfig.java` |
| Per-table Config | `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/TableSinkConfig.java` |
| Writer Factory | `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/IcebergWriterFactory.java` |
| Record Converter | `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/RecordConverter.java` |
| Iceberg Writer | `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/IcebergWriter.java` |
| Coordinator | `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java` |
| Worker | `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Worker.java` |
| Committer | `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitterImpl.java` |
| Transforms | `kafka-connect/kafka-connect-transforms/src/main/java/org/apache/iceberg/connect/transforms/` |
