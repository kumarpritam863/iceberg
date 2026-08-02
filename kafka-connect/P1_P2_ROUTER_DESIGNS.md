# P1/P2: Composite Field Router, Header Router, Record Filter — Design

## Context

These three features all build on the `RecordRouter` interface we implemented in P0. Each is a new `RecordRouter` implementation — no changes to the routing framework, SinkWriter, or config infrastructure.

---

## 1. Composite Field Router (P1)

### Problem
Current routing extracts ONE field and uses it as-is (DynamicRouter) or regex-matches it (StaticRouter). Real-world use cases need multi-field routing:
```
route by: region + event_type → "warehouse.us_east.clicks"
route by: env + service + version → "staging.payments_v2"
```

### Design

**Class:** `CompositeFieldRouter implements RecordRouter`
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/CompositeFieldRouter.java`

**Config properties** (prefix `iceberg.tables.route.composite.`):

| Property | Default | Description |
|----------|---------|-------------|
| `fields` | *required* | Comma-separated list of field paths (supports dot notation for nesting) |
| `table-template` | `${0}.${1}` | Template for building table name. `${N}` references fields by index. Literal text preserved. |
| `table-namespace` | null | Namespace prepended to the final table name |
| `lowercase` | true | Lowercase the derived table name |
| `ignore-missing-table` | true | NoOpWriter vs throw on missing table |
| `null-handling` | `drop` | What to do when a field value is null: `drop` (skip record), `literal` (use string "null"), `default:<value>` (use given default) |

**Template engine:**
- `${0}`, `${1}`, `${2}` ... reference extracted field values by index in `fields` list
- Literal characters are preserved: `${0}_${1}` → `us_east_clicks`
- Nested separators: `${0}.${1}` → `us_east.clicks`
- Simple string replacement, no expression evaluation

**Routing logic:**
```
1. For each field in `fields`:
   - Extract via RecordUtils.extractFromRecordValue(record.value(), field)
   - If null → apply null-handling policy
   - Convert to string via .toString()
2. Apply template substitution: replace ${N} with field values
3. Apply lowercase if enabled
4. Prepend table-namespace if set
5. Return RouteTarget.of(result, ignoreMissingTable)
```

**Example configurations:**

Multi-dimensional routing (region + event_type):
```properties
iceberg.tables.router-class=org.apache.iceberg.connect.data.CompositeFieldRouter
iceberg.tables.route.composite.fields=region,event_type
iceberg.tables.route.composite.table-template=${0}_${1}
iceberg.tables.route.composite.table-namespace=warehouse
# Record {region: "us_east", event_type: "clicks"} → warehouse.us_east_clicks
```

Hierarchical routing with nested fields:
```properties
iceberg.tables.route.composite.fields=metadata.env,service.name,schema_version
iceberg.tables.route.composite.table-template=${0}.${1}_v${2}
# Record {metadata: {env: "prod"}, service: {name: "payments"}, schema_version: "2"} → prod.payments_v2
```

**Edge cases:**
- Field not found in record → null → null-handling decides
- All fields null with `drop` → empty list (record silently dropped)
- Template references non-existent field index → IllegalArgumentException at configure() time (validate template against fields count)
- Empty `fields` → ConfigException at configure() time

---

## 2. Header-Based Router (P2)

### Problem
Kafka record headers are a standard mechanism for routing metadata in event-driven architectures. Producers attach headers like `X-Target-Table`, `X-Routing-Key`, or `iceberg.table` to control downstream routing. The current connector ignores headers entirely.

### Design

**Class:** `HeaderRouter implements RecordRouter`
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/HeaderRouter.java`

**Config properties** (prefix `iceberg.tables.route.header.`):

| Property | Default | Description |
|----------|---------|-------------|
| `name` | *required* | Header name to extract (e.g., `iceberg.table`, `X-Target-Table`) |
| `table-namespace` | null | Namespace prepended to derived table name |
| `table-map.<header-value>` | — | Explicit header-value→table overrides |
| `regex` | null | Regex applied to the header value |
| `regex-replacement` | `$1` | Replacement string for regex captures |
| `lowercase` | true | Lowercase the derived table name |
| `ignore-missing-table` | true | NoOpWriter vs throw on missing |
| `on-missing-header` | `drop` | What to do when header is absent: `drop` (skip record), `fail` (throw), `default:<table>` (use fallback table) |
| `multi-value` | `first` | When multiple headers with same name: `first` (use first), `all` (route to all), `last` (use last) |

**Routing logic:**
```
1. Get header(s) by name from record.headers()
2. If no header found → apply on-missing-header policy
3. Decode header value(s) as UTF-8 string
4. For each header value (depends on multi-value policy):
   a. Check explicit table-map → use if found (bypass transforms)
   b. Apply regex if set → drop if no match
   c. Apply lowercase if enabled
   d. Prepend table-namespace if set
   e. Add RouteTarget to result list
5. Return list of RouteTargets
```

**Header value decoding:**
- Headers in Kafka are `byte[]`. We decode as UTF-8 (the overwhelming standard for header values).
- If value is null (header present but no value), treated as missing.

**Example configurations:**

Simple header-as-table:
```properties
iceberg.tables.router-class=org.apache.iceberg.connect.data.HeaderRouter
iceberg.tables.route.header.name=iceberg.table
# Record with header "iceberg.table: db.orders" → routes to db.orders
```

Header with namespace:
```properties
iceberg.tables.route.header.name=X-Event-Type
iceberg.tables.route.header.table-namespace=events
# Record with header "X-Event-Type: user_signup" → events.user_signup
```

Header with explicit mapping:
```properties
iceberg.tables.route.header.name=X-Routing-Key
iceberg.tables.route.header.table-map.user=analytics.users
iceberg.tables.route.header.table-map.order=analytics.orders
iceberg.tables.route.header.on-missing-header=default:analytics.unknown
```

Multi-value fan-out (rare but powerful):
```properties
iceberg.tables.route.header.name=X-Target-Tables
iceberg.tables.route.header.multi-value=all
# Record with headers "X-Target-Tables: audit" and "X-Target-Tables: analytics"
# → routes to both audit and analytics tables
```

**Edge cases:**
- Header absent → `on-missing-header` policy
- Header present but value is null → treated as absent
- Header value is empty string → valid, routed normally
- Multiple headers with same name → `multi-value` policy
- Non-UTF-8 bytes → decoded as UTF-8 with replacement characters (best-effort)

---

## 3. Record Filter (P2)

### Problem
Users need to drop records before they reach the writer based on field values, metadata, or presence of specific fields. Currently, the only "filter" is the route-regex in StaticRouter, which is tightly coupled to routing.

### Design Approach

A filter is NOT a router — it's a **decorator/wrapper** around a router. It evaluates a predicate BEFORE routing. If the predicate rejects the record, it returns an empty list (record dropped). If accepted, it delegates to the wrapped router.

**Class:** `FilterRouter implements RecordRouter`
**Path:** `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/FilterRouter.java`

**Config properties** (prefix `iceberg.tables.route.filter.`):

| Property | Default | Description |
|----------|---------|-------------|
| `delegate-class` | *required* | Fully-qualified class name of the actual router to delegate to after filtering |
| `mode` | `include` | `include` = only matching records pass. `exclude` = matching records are dropped. |
| `field` | null | Record value field to evaluate (dot notation for nesting) |
| `header` | null | Header name to evaluate (alternative to field) |
| `topic-regex` | null | Regex that topic name must match |
| `field-regex` | null | Regex that extracted field value must match |
| `header-regex` | null | Regex that extracted header value must match |
| `field-exists` | null | Field path that must exist (non-null) in the record |

**Predicate evaluation:**
- All configured predicates are AND-ed together (must all match for `include`, any match for `exclude`)
- At least one predicate must be configured

**Evaluation logic:**
```
1. Evaluate all predicates:
   a. topic-regex: regex.matcher(record.topic()).matches()
   b. field + field-regex: extract field, regex match on its string value
   c. header + header-regex: extract header, regex match on its string value
   d. field-exists: extract field, check != null
2. Combine predicates (all must be true for include mode)
3. If mode=include AND predicates pass → delegate.route(record)
4. If mode=include AND predicates fail → ImmutableList.of() (drop)
5. If mode=exclude AND predicates pass → ImmutableList.of() (drop)
6. If mode=exclude AND predicates fail → delegate.route(record)
```

**Example configurations:**

Filter by topic (only process events.* topics), delegate to TopicNameRouter:
```properties
iceberg.tables.router-class=org.apache.iceberg.connect.data.FilterRouter
iceberg.tables.route.filter.delegate-class=org.apache.iceberg.connect.data.TopicNameRouter
iceberg.tables.route.filter.topic-regex=events\..*
iceberg.tables.route.filter.mode=include
# TopicNameRouter config follows:
iceberg.tables.route.topic-name.table-namespace=warehouse
```

Exclude test data:
```properties
iceberg.tables.router-class=org.apache.iceberg.connect.data.FilterRouter
iceberg.tables.route.filter.delegate-class=org.apache.iceberg.connect.data.DynamicRouter
iceberg.tables.route.filter.field=env
iceberg.tables.route.filter.field-regex=test|staging|dev
iceberg.tables.route.filter.mode=exclude
# DynamicRouter config:
iceberg.tables.dynamic-enabled=true
iceberg.tables.route-field=table_name
```

Drop records missing a required field:
```properties
iceberg.tables.router-class=org.apache.iceberg.connect.data.FilterRouter
iceberg.tables.route.filter.delegate-class=org.apache.iceberg.connect.data.CompositeFieldRouter
iceberg.tables.route.filter.field-exists=customer.id
iceberg.tables.route.filter.mode=include
# CompositeFieldRouter config follows...
```

Filter by header presence:
```properties
iceberg.tables.route.filter.header=X-Processable
iceberg.tables.route.filter.header-regex=true
iceberg.tables.route.filter.mode=include
```

**Edge cases:**
- No predicates configured → ConfigException at configure() time
- `field` set but `field-regex` not set → checks field exists (non-null)
- `header` set but `header-regex` not set → checks header exists
- Null record value with field predicate → predicate fails (field not extractable)
- Delegate router throws → exception propagates (filter doesn't catch)

---

## Implementation Order

All three routers are independent — they can be implemented in any order or in parallel.

### Per-router file list:

**CompositeFieldRouter:**
1. `CompositeFieldRouter.java` (new)
2. `TestCompositeFieldRouter.java` (new, ~12 tests)

**HeaderRouter:**
1. `HeaderRouter.java` (new)
2. `TestHeaderRouter.java` (new, ~14 tests)

**FilterRouter:**
1. `FilterRouter.java` (new)
2. `TestFilterRouter.java` (new, ~10 tests)

**No existing files need modification.** All three are new `RecordRouter` implementations that plug into the existing framework.

---

## Test Coverage

### CompositeFieldRouter tests:
- `testTwoFieldTemplate` — basic multi-field routing
- `testThreeFieldTemplate` — three fields with literal separators
- `testNestedFieldExtraction` — dot notation for nested fields
- `testNamespacePrepended` — namespace + template
- `testLowercaseDefault` — mixed case normalized
- `testLowercaseDisabled` — case preserved
- `testNullFieldDrop` — null field with drop policy → empty list
- `testNullFieldLiteral` — null field with literal policy → "null" in output
- `testNullFieldDefault` — null field with default value
- `testAllFieldsNull` — all null with drop → empty list
- `testTemplateValidation` — invalid template index → ConfigException
- `testEmptyFieldsConfig` — no fields configured → ConfigException

### HeaderRouter tests:
- `testSimpleHeaderAsTable` — header value used as table name
- `testHeaderWithNamespace` — namespace prepended
- `testHeaderExplicitMap` — explicit value→table override
- `testHeaderExplicitMapBypassesTransforms` — explicit map skips regex/namespace
- `testHeaderRegex` — regex transform on header value
- `testHeaderRegexNoMatch` — regex miss → empty list
- `testMissingHeaderDrop` — absent header with drop → empty list
- `testMissingHeaderFail` — absent header with fail → exception
- `testMissingHeaderDefault` — absent header with default → uses fallback
- `testNullHeaderValue` — header exists but value null → treated as missing
- `testMultiValueFirst` — multiple headers, first used
- `testMultiValueAll` — multiple headers, all routed
- `testMultiValueLast` — multiple headers, last used
- `testLowercaseHeader` — case normalization on header value

### FilterRouter tests:
- `testIncludeTopicRegex` — matching topic passes through
- `testIncludeTopicRegexNoMatch` — non-matching topic dropped
- `testExcludeTopicRegex` — matching topic dropped
- `testExcludeTopicRegexNoMatch` — non-matching topic passes
- `testIncludeFieldRegex` — matching field value passes
- `testExcludeFieldRegex` — matching field value dropped
- `testFieldExistsInclude` — non-null field passes
- `testFieldExistsExcludeNullField` — null field passes in exclude mode
- `testHeaderRegexFilter` — header-based filtering
- `testCombinedPredicates` — topic + field predicates AND-ed
- `testDelegateCalled` — verify delegate router receives record on pass

---

## Verification

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home

# Build
$JAVA_HOME/../.. ./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:build -Pquick=true

# Test individual routers
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test --tests org.apache.iceberg.connect.data.TestCompositeFieldRouter
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test --tests org.apache.iceberg.connect.data.TestHeaderRouter
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test --tests org.apache.iceberg.connect.data.TestFilterRouter

# Spotless
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:spotlessApply
```

## Backward Compatibility

All three are additive — new classes, no modifications to existing code. They're opt-in via `iceberg.tables.router-class` configuration.
