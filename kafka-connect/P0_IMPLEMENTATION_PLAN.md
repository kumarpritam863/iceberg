# P0: Pluggable RecordRouter + TopicNameRouter — Implementation Plan

## Context

The iceberg-kafka-connect module has routing logic hardcoded in `SinkWriter.java` with two mutually exclusive modes (static regex vs dynamic field). This blocks users from topic-based routing, multi-field routing, header-based routing, and any custom routing. We introduce a `RecordRouter` interface that abstracts routing, ship backward-compatible wrappers for existing behavior, and add `TopicNameRouter` as the first new router — all in one cohesive design.

---

## Files to Create (7 new files)

### 1. `RouteTarget.java` — routing result value object
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/RouteTarget.java`

Immutable value class:
- `String tableName` — fully-qualified Iceberg table identifier
- `boolean ignoreMissingTable` — `false` for static routing (throw on missing), `true` for dynamic/topic routing (NoOpWriter)
- Static factories: `RouteTarget.of(tableName)` (ignoreMissing=false), `RouteTarget.of(tableName, ignoreMissing)`
- `equals`/`hashCode`/`toString`

### 2. `RecordRouter.java` — pluggable interface
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/RecordRouter.java`

```java
public interface RecordRouter extends Closeable {
  void configure(Map<String, String> props);
  List<RouteTarget> route(SinkRecord record);
  @Override default void close() {}
}
```

Mirrors the Kafka Connect `Transformation.configure(Map)` pattern. Returns `List` to support fan-out (broadcast to multiple tables).

### 3. `StaticRouter.java` — wraps existing static routing
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/StaticRouter.java`

Extracts logic from `SinkWriter.routeRecordStatically()` (lines 89-114):
- No route field → broadcast `RouteTarget.of(tableName)` for every table
- With route field → extract via `RecordUtils.extractFromRecordValue()`, regex-match per table
- Package-private `configure(IcebergSinkConfig)` overload to avoid re-parsing config

### 4. `DynamicRouter.java` — wraps existing dynamic routing
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/DynamicRouter.java`

Extracts logic from `SinkWriter.routeRecordDynamically()` (lines 117-126):
- Extract route field → lowercase → `RouteTarget.of(tableName, true)`
- Package-private `configure(IcebergSinkConfig)` overload

### 5. `TopicNameRouter.java` — new topic-to-table mapping
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/TopicNameRouter.java`

Config properties (prefix `iceberg.tables.route.topic-name.`):

| Property | Default | Description |
|----------|---------|-------------|
| `table-namespace` | null | Namespace prepended to derived name (`db` → `db.orders`) |
| `table-map.<topic>` | — | Explicit topic→table overrides (bypass all transforms) |
| `regex` | null | Regex applied to topic name (must match whole topic) |
| `regex-replacement` | `$1` | Replacement string for regex (supports capture groups) |
| `lowercase` | true | Lowercase the derived table name |
| `ignore-missing-table` | true | NoOpWriter vs throw on missing table |

**Routing priority:**
1. Explicit `table-map` entry → use as-is (fully qualified, skip namespace/lowercase)
2. `regex` set → apply `replaceAll`, drop if no match
3. Otherwise → raw topic name
4. Apply lowercase if enabled
5. Prepend `table-namespace` if set
6. Return `RouteTarget.of(result, ignoreMissingTable)`

**Example configurations:**

Simple namespace prefix (`orders` topic → `analytics.orders` table):
```properties
iceberg.tables.router-class=org.apache.iceberg.connect.data.TopicNameRouter
iceberg.tables.route.topic-name.table-namespace=analytics
```

Regex strip prefix (`prod.events.clicks` → `warehouse.clicks`):
```properties
iceberg.tables.router-class=org.apache.iceberg.connect.data.TopicNameRouter
iceberg.tables.route.topic-name.regex=.*\.(.+)
iceberg.tables.route.topic-name.regex-replacement=$1
iceberg.tables.route.topic-name.table-namespace=warehouse
```

Explicit overrides for specific topics:
```properties
iceberg.tables.router-class=org.apache.iceberg.connect.data.TopicNameRouter
iceberg.tables.route.topic-name.table-namespace=db
iceberg.tables.route.topic-name.table-map.orders-v2=analytics.orders
iceberg.tables.route.topic-name.table-map.users-legacy=analytics.users
```

### 6. `RecordRouterFactory.java` — factory with backward-compatible fallback
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/RecordRouterFactory.java`

Package-private. `create(IcebergSinkConfig config)`:
1. If `config.tablesRouterClass()` is set → reflective load via `DynClasses`/`DynConstructors` (same pattern as `CatalogUtils:50-63`), call `configure(config.originalProps())`
2. Else if `config.dynamicTablesEnabled()` → `new DynamicRouter()`, call `configure(config)` (package-private overload)
3. Else → `new StaticRouter()`, call `configure(config)` (package-private overload)

### 7. Test files (4 new)
- `TestTopicNameRouter.java` — 11 tests covering namespace, explicit map, regex, lowercase, ignoreMissing
- `TestStaticRouter.java` — 4 tests mirroring existing static routing scenarios
- `TestDynamicRouter.java` — 2 tests mirroring existing dynamic routing scenarios
- `TestRecordRouterFactory.java` — 4 tests (default→Static, dynamic→Dynamic, custom class, invalid class)

---

## Files to Modify (3 files)

### 8. `IcebergSinkConfig.java`
- Add constant: `TABLES_ROUTER_CLASS_PROP = "iceberg.tables.router-class"`
- Add ConfigDef entry (Type.STRING, null, MEDIUM importance)
- Add accessor: `tablesRouterClass()` → `getString(TABLES_ROUTER_CLASS_PROP)`
- Expose `originalProps()` for custom routers
- Update `validate()`: if `tablesRouterClass() != null`, skip table/dynamic validation (early return)

### 9. `SinkWriter.java`
- Add `RecordRouter router` field
- Constructor: `this.router = RecordRouterFactory.create(config)`
- Add package-private constructor `SinkWriter(Catalog, IcebergSinkConfig, RecordRouter)` for tests
- Replace routing if/else (lines 82-86) with:
  ```java
  List<RouteTarget> targets = router.route(record);
  for (RouteTarget target : targets) {
    writerForTable(target.tableName(), record, target.ignoreMissingTable()).write(record);
  }
  ```
- Delete `routeRecordStatically()`, `routeRecordDynamically()`, `extractRouteValue()`
- Update `close()` to call `router.close()`

### 10. `TestSinkWriter.java`
- Use new package-private constructor with mock `RecordRouter`
- Tests remain equivalent but inject routing via router mock instead of config mocks

---

## Implementation Order

1. `RouteTarget` → `RecordRouter` (no dependencies)
2. `StaticRouter` + `DynamicRouter` (depend on interface + RecordUtils)
3. `TopicNameRouter` (depends on interface only)
4. `IcebergSinkConfig` changes (additive, no breakage)
5. `RecordRouterFactory` (depends on all routers + config)
6. `SinkWriter` refactor (depends on factory)
7. All tests

---

## Verification

```bash
# Build kafka-connect module
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:build -Pquick=true

# Run all kafka-connect tests
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test

# Run specific new test classes
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test --tests org.apache.iceberg.connect.data.TestTopicNameRouter
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test --tests org.apache.iceberg.connect.data.TestStaticRouter
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test --tests org.apache.iceberg.connect.data.TestDynamicRouter
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test --tests org.apache.iceberg.connect.data.TestRecordRouterFactory
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test --tests org.apache.iceberg.connect.data.TestSinkWriter

# Spotless formatting
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:spotlessApply
```

---

## Backward Compatibility

- No existing config changes behavior — `RecordRouterFactory` falls back to `StaticRouter`/`DynamicRouter` when `router-class` is unset
- Existing `iceberg.tables`, `iceberg.tables.dynamic-enabled`, `iceberg.tables.route-field`, `iceberg.table.<name>.route-regex` all work identically
- New `iceberg.tables.router-class` is optional with null default
