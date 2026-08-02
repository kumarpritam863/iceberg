# Zero-Copy Avro Passthrough: Kafka bytes → Iceberg data file

**Status:** Design. Every load-bearing claim below was verified against Avro 1.12.1 bytecode, this
Iceberg tree (`22714f023a`), and the PIE converter at
`work/official/kafka-connect-pie-converter`. Claims that survived an adversarial refutation pass are
marked ✅; qualified ones are marked ⚠️ with the exact qualification.

---

## 1. Verdict

The idea is sound and the mechanism already exists. **You can memcpy the Kafka Avro payload straight
into the Iceberg Avro data file** — no `GenericRecord`, no Connect `Struct`, no Iceberg `Record`, no
re-encode.

Today, one record crosses four full object-tree traversals and allocates three intermediate trees:

```
Kafka bytes ──decode──▶ GenericRecord ──AvroData──▶ Connect Struct ──RecordConverter──▶ Iceberg Record ──encode──▶ Avro bytes
              (1)                       (2)                          (3)                                 (4)
```

All four are pure waste when the source and destination are both Avro. The proposed path:

```
Kafka bytes ──ByteBuffer.slice()──▶ appendEncoded() ──▶ Avro bytes
                (0 copies)            (1 memcpy, or 0 with a custom OCF writer)
```

The catch is not the write path — that part is easy and safe. The catch is that byte-level
passthrough imposes a **structural contract** between the Kafka writer schema and the Iceberg table
schema, and Avro will not tell you when you break it. Section 6 is the important part of this
document; §7.5 is the sharpest edge.

Partitioning works normally, on ordinary payload fields, through the existing Iceberg code paths —
see §7.

### Phase 0 result — measured ✅

`ZeroCopyAvroBenchmark` in `kafka-connect/kafka-connect/src/jmh/java/.../connect/data/`. JMH, JDK 17,
Apple Silicon, 1 fork, 3 warmup + 5 measurement iterations, 41-column schema, 383 B average payload,
1024 distinct pooled records. Stages ③ and ④ use the **real** `RecordConverter` and the **real**
Iceberg `FileAppender`; both paths write to a discarding sink with compression off, so the comparison
is apples-to-apples.

| | ns/record |
|---|---|
| ① decode → `GenericRecord` | 658 ± 269 |
| ② `GenericRecord` → Connect `Struct` (*stand-in, lower bound*) | 1591 ± 52 |
| ③ `Struct` → Iceberg `Record` — **real `RecordConverter`** | 1428 ± 56 |
| ④ append Iceberg `Record` — **real Iceberg `FileAppender`** | 954 ± 114 |
| **today, four stages chained** | **6233 ± 496** |
| **proposed, `appendEncoded`** | **22.2 ± 2.1** |
| bare memcpy floor | 9.9 ± 1.1 |

**281× cheaper** for an unpartitioned table. Adding payload-field partition extraction (§7.3, measured
separately at 367–442 ns, or ~58 ns with root-plan truncation) still leaves the proposed path
**14–16×** cheaper — or ~78× with truncation.

Three things Phase 0 changed:

- **Stage ③ is 1428 ns, not the ~134 ns an earlier stand-in suggested — 10.7× higher.** Pinning this
  with the real `RecordConverter` was the main point of Phase 0, and it roughly doubles the measured
  cost of today's path.
- **Chained cost exceeds the sum of isolated stages** (6233 vs 4631). Measured per-stage, each gets a
  warm cache; chained, the pipeline allocates three object trees per record and pays the GC for it.
- **Stage ② at 1591 ns is a floor, not an estimate.** It is a hand-written per-field copy specialised
  to the benchmark schema. Confluent's general-purpose `AvroData` measured ~2304 ns on a comparable
  record, and Apple's `AvroData`/`FastAvroData` is more general still — so the real today-number is
  *above* 6233 ns. Worth re-running stage ② against the actual PIE converter in Phase 2.

Caveats: absolute numbers exclude compression and real I/O — both paths would pay those, and output
volume is similar since raw mode writes the same payload bytes. Stage ① is allocation-heavy and noisy
(±41%). Larger payloads raise the memcpy floor linearly (~212 ns at 10 KB) but raise the decode and
re-encode stages too.

---

## 2. The three load-bearing facts

**① `DataFileWriter.appendEncoded(ByteBuffer)` is exactly the primitive you want.** ✅

Verified from the 1.12.1 jar (`javap -c`) — the entire method body is four statements:

```java
public void appendEncoded(ByteBuffer datum) throws IOException {
  assertOpen();
  bufOut.writeFixed(datum);   // one System.arraycopy into the block staging buffer
  blockCount++;
  writeIfBlockFull();
}
```

No decode, no validation, no `DatumWriter` involvement. It honours `setCodec` (compression happens
at block flush, independent of how bytes entered the buffer), honours `syncInterval`, increments the
block object count correctly, and does not consume the buffer (position unchanged, so slices work).
It predates 1.12 by many releases (present in 1.7.4+), so no version gate is needed.

**② An Avro record datum has no framing.** ✅

A record's encoding is the bare concatenation of its fields' encodings in declaration order — no
length prefix, no tag, no count. The block payload is likewise the bare concatenation of datum
encodings. This is why a memcpy is sufficient, and it is also why you can append extra trailing
fields as suffix bytes (§7.4).

**③ Iceberg data files do not need to carry the table schema.** ✅

This is the insight that makes the whole thing practical. `ValueReaders.buildReadPlan`
(`core/.../avro/ValueReaders.java:224-260`) walks **the file's own field order**, resolves each
field **by field-id**, skips file fields the table doesn't have (`projectionPos == null` → reader
retained for skipping only), and null-fills table fields the file lacks
(`addMissingFileReadersToPlan`). Union branches are read positionally in **file-schema order**
(`AvroWithPartnerVisitor.visitUnion:196` → `ValueReaders.UnionReader:770`), so `["T","null"]` reads
correctly, not just `["null","T"]`. ⚠️

So each data file may carry its own Kafka writer schema, verbatim, annotated with field IDs. You do
not normalise anything at write time. Iceberg reconciles per file, at read time, for free.

---

## 3. Architecture

Five new pieces. Nothing in `iceberg-core` needs to change.

```
┌─ PieAvroConverter (raw mode) ────────────────────────────────────────────┐
│  toConnectData → SchemaAndValue(RAW_AVRO_BYTES_SCHEMA, RawAvroPayload)   │
│    RawAvroPayload = { ByteBuffer slice, String schemaName, int version } │
│    · no GenericRecord, no AvroData, no Struct                            │
└──────────────────────────────────────────────────────────────────────────┘
                 │  Connect runtime: value passes through untouched ✅
                 ▼
┌─ IcebergWriter (raw branch) ─────────────────────────────────────────────┐
│  if (value instanceof RawAvroPayload) → rawWriter.write(payload, record) │
│  else → existing RecordConverter path (unchanged)                        │
└──────────────────────────────────────────────────────────────────────────┘
                 ▼
┌─ EncodedRowWriterFactory : FileWriterFactory<EncodedRow> ────────────────┐
│  newDataWriter → new DataWriter<>(EncodedAvroFileAppender, AVRO, ...)    │
│  keyed by (table, partition, writerSchemaVersion)                        │
└──────────────────────────────────────────────────────────────────────────┘
                 ▼
┌─ EncodedAvroFileAppender : FileAppender<EncodedRow> ─────────────────────┐
│  DataFileWriter.create(annotatedWriterSchema, positionOutputStream)      │
│  add(row) → appendEncoded(row.payload) [+ appendEncoded(row.suffix)]     │
│  metrics() → new Metrics(numRecords, null, null, null, null)             │
└──────────────────────────────────────────────────────────────────────────┘
```

`EncodedRow` is a two-field value object: the payload slice, plus an optional pre-encoded suffix
buffer for Kafka-metadata columns (§7.4). For partitioned tables the same object is also the backing
for the `StructLike` façade that yields partition values (§7.2).

**As built (Phase 1):** the row type is `RawAvroPayload` — payload `ByteBuffer`, resolved
`org.apache.avro.Schema`, and the `(schemaName, schemaVersion)` registry coordinates. The suffix
buffer is not there yet; it arrives with §7.4 if wanted. Two decisions worth noting against the
sketch above:

- **The per-record guard is on `(schemaName, schemaVersion)`, not schema equality.** The file's header
  schema is the *annotated* form — field IDs injected, record names rewritten — so it is never equal
  to the producer schema the payload was encoded against. The registry coordinates are the real
  discriminator, and comparing them is O(1) rather than a deep structural walk per record. (The first
  draft compared schemas and rejected every legitimate record; the round-trip test caught it.)
- **`RawAvroPayload` is public and carries a `assertLoadedOnce` guard.** Kafka Connect can load a
  converter plugin and a connector plugin in separate classloaders, in which case the converter's
  `RawAvroPayload` is a different `Class` and every record fails `instanceof`. The guard turns that
  into one explanatory startup failure instead of a per-record `ClassCastException`.

### Why this composes with no core changes

The Iceberg writer stack is **fully generic in the row type**. Verified end to end:

| Class | Row-type coupling |
|---|---|
| `FileAppender<D>` (`api/.../io/FileAppender.java`) | none — `add(D)`, `metrics()`, `length()`, `splitOffsets()` |
| `DataWriter<T>` (`core/.../io/DataWriter.java:69`) | none — `write(T)` is literally `appender.add(row)` |
| `FileWriterFactory<T>` | none — 3 generic methods; only `newDataWriter` needs a body |
| `RollingFileWriter<T,W,R>` | none — rolls on row *count* and `length()`, never row content |
| `UnpartitionedWriter<T>` | none |
| `PartitionedFanoutWriter<T>` | one method: `protected abstract PartitionKey partition(T row)` |

Every concrete `Record` assumption lives *outside* core: `GenericFileWriterFactory` (hardcodes
`Record.class`) and kafka-connect's own `RecordUtils`/`IcebergWriter`/`PartitionedAppendWriter`.
Those are ours to parameterise.

`AvroFileAppender` being package-private is irrelevant — we are not extending it. There is also no
`module-info.java` and no sealed-jar config anywhere in the repo, so a split-package class in
`org.apache.iceberg.avro` would work too; we just don't need one.

---

## 4. Metrics and split offsets: nothing is lost

`AvroMetrics.fromWriter` (`core/.../avro/AvroMetrics.java:30-37`) is, in its entirety:

```java
// TODO will populate in following PRs if datum writer is a MetricsAwareDatumWriter
return new Metrics(numRecords, null, null, null, null);
```

**Every Avro data file Iceberg writes today already has zero column statistics**, regardless of
`write.metadata.metrics.default`. `AvroFileAppender` also never overrides `splitOffsets()`, so Avro
files carry none. A raw-bytes appender that returns `Metrics(recordCount, null, null, null, null)`
and `splitOffsets() == null` is therefore **metrics-identical to the built-in Avro path** — this
costs nothing relative to the status quo.

Null metrics are legal and safe: `DataFiles.Builder` requires only `file_path`, `file_format`,
`file_size_in_bytes`, `record_count` (+ partition); `ManifestWriter.addEntry` touches only
`recordCount()` and `partition()`; both metrics evaluators null-guard and fail *open*
(`ROWS_MIGHT_MATCH`), so results stay correct. The kafka-connect `DataWritten` Avro event encodes
the nulls as union-null branches with no NPE.

⚠️ **But empty metrics are not purely a performance matter for downstream consumers.** The
adversarial pass found two hard failures, both *outside* the ingest path:
`DeleteFiles.deleteFromRowFilter` / `OverwriteFiles.validateAddedFilesMatchOverwriteFilter` throw
`ValidationException` (strict evaluator can't prove `ROWS_MUST_MATCH` without bounds), and Flink's
`ColumnStatsWatermarkExtractor` throws `IllegalArgumentException`. Again — this is already true of
Avro-format Iceberg tables today. It is a reason to prefer Parquet for tables that need metadata
deletes, not a regression introduced by this design.

---

## 5. Schema handling

### 5.1 Annotate, don't convert

For each distinct `(schemaName, version)` seen, once:

1. Fetch the writer schema `Sv` from the PIE schema store.
2. Run the **eligibility gate** (§6). Reject → fall back to the decode path for that version.
3. Inject `field-id` on every record field, and `element-id` / `key-id` / `value-id` on
   arrays/maps, by matching field names to the Iceberg table schema.
4. Cache the annotated schema `Sv'` keyed by `(schemaName, version, tableSchemaId)`.

`Sv'` goes into the OCF header verbatim. Do **not** run it through `AvroSchemaUtil.convert` — the
whole point is that it never becomes an Iceberg schema.

Six rules govern the annotation, all confirmed by refutation testing: ⚠️

- **Annotate all fields or none.** Partial annotation is *worse* than none: a single ID anywhere
  makes `HasIds` true, and `NameMappingDatumReader:46-50` then skips the name-mapping fallback for
  the entire file — unannotated columns read as silent nulls, or hard-fail if the table column is
  required.
- **Match against `AvroSchemaUtil.makeCompatibleName(icebergName)`**, not the raw Iceberg name, or
  columns with non-Avro-legal characters never match.
- **IDs must be globally unique**, which means an Avro named type reused at two points in the tree
  cannot be annotated. Reject such schemas at the gate.
- **Rewrite the record `fullName`** to something non-loadable (e.g. `r2`). If the registry-generated
  name resolves to a `SpecificRecord` on the worker classpath, `GenericAvroReader:123-138` will
  instantiate that class and break the moment the table gains a column. Names never affect the wire.
- **New table columns must be optional** (or carry an `initial-default`), which is already the
  Iceberg evolution rule.
- **Nothing validates that the Avro physical type matches the Iceberg column type.** A mismatch is
  silently wrong data, never an error. The gate is your only defence.

### 5.2 One open file per writer-schema version

An OCF file has exactly one `avro.schema` in its header, and every datum in it must be encoded under
that schema. Kafka delivers versions interleaved (v5, v7, v5, v6…). So the writer key becomes:

```
(table, partitionKey, writerSchemaVersion)  →  one open EncodedAvroFileAppender
```

This is the one genuinely new operational cost. Mitigations: schema versions in a topic are
few and long-lived in practice; cap concurrent open writers and evict LRU by closing (each close
just emits a smaller data file); emit a metric on the open-writer count. Note this interacts with
partition fanout multiplicatively — worth bounding explicitly.

Pleasantly, **schema evolution gets simpler, not harder.** Today evolution is driven by the Connect
`valueSchema` of a materialised Struct. Here, a new version's annotated schema is computed anyway,
so the "does the table need new columns?" question is answered from `Sv` alone, with no data
touched — `SchemaUtils.toIcebergType` already walks a schema tree without values. Hook it into
`IcebergWriter.convertToRow`, which already has the flush → apply-updates → re-init dance.

---

## 6. The eligibility gate

This is the safety-critical component. It runs once per schema version and decides raw vs. decode.
It must inspect the **entire** writer schema, not just the mapped columns — unprojected file fields
still need a working skip-reader, and e.g. `PlannedDataReader` throws on an enum field even when
nobody selects it.

The matrix below was produced by actually encoding values and reading them back, not from docs:

**Zero-copy eligible** — byte-identical to Iceberg's own encoding:

| Avro construct | Iceberg type |
|---|---|
| `boolean`, `int`, `long`, `float`, `double`, `bytes`, `string`, `null` | 1:1 |
| `int` + `date` | `date` |
| `long` + `time-micros` | `time` |
| `long` + `timestamp-micros` / `timestamp-nanos`, ± `adjust-to-utc` | `timestamp[_ns]` / `timestamptz` |
| `bytes` or `fixed(N)` + `decimal` | `decimal` (any N — reader dispatches on actual type) |
| `fixed(16)` + `uuid` | `uuid` |
| `fixed(N)` plain | `fixed[N]` (or `binary`) |
| `array<T>` | `list` — element nullability may differ from the column, both directions |
| `map<string,V>` | `map` — Avro map framing is byte-identical to Iceberg's array-of-kv |
| `["null",T]` **and** `[T,"null"]` | optional column — both branch orders |
| nested record | `struct` — zero framing bytes |
| field with a `default` | no-op; defaults never appear on the wire |

**Must reject:**

| Construct | Failure |
|---|---|
| `enum` | Zigzag ordinal vs length-prefixed UTF-8. `EOFException` / stream desync. |
| `string` + `uuid` | Reader blindly does `readFixed(16)` on a 37-byte string. **Silent corruption + desync.** |
| unions with 3+ branches, or 2 branches without null | `Preconditions` failure in `visitUnion` before any byte is read. |
| recursive / self-referential records | `IllegalStateException: Cannot process recursive Avro record`, all paths. |
| `int` + `time-millis`; all `local-timestamp-*` | `IllegalArgumentException: Unknown logical type` — a crash in every reader, not a graceful skip. |
| `long` + `timestamp-millis` | Layout matches, but readers multiply by 1000. **Silently 1000× wrong.** No Iceberg millis type exists. |
| `decimal` whose precision/scale ≠ the column's | Reader uses the *file's* scale. Silently off by powers of ten. |
| a named type referenced more than once | Can't assign unique field IDs. |

Two of these — `string+uuid` and `timestamp-millis` — produce **silently wrong data with no error at
write time and no error at read time**. They are the reason the gate must be a hard allowlist that
walks the full schema and rejects on anything unrecognised, rather than a denylist.

`appendEncoded` will not save you: its javadoc says outright *"No validation is performed… Appending
non-conforming data may result in an unreadable file."* Confirmed by refutation — a length mismatch
corrupts the rest of the block (up to `syncInterval` bytes of otherwise-good records), and a
well-formed-but-different encoding produces silent wrong values. ⚠️

### Fail-fast measures

- Gate at schema-registration time, not per record — the cost amortises to zero.
- Pin the writer-schema fingerprint: `SchemaNormalization.parsingFingerprint64(Sv)` is stored in the
  appender and compared against the per-record header's resolved version. Cheap, catches a registry
  serving different bytes for the same version.
- **Exactly one datum per `appendEncoded` call.** Two datums in one buffer desyncs `blockCount` →
  silent data loss plus a block read error.
- Shadow-validate a sampled fraction (say 1-in-N, configurable, default off in steady state): decode
  the buffer with a `BinaryDecoder` and assert the decoder sits exactly at end-of-input. This catches
  all three failure modes at a controllable cost.

---

## 7. Partitioning: normal Iceberg partitioning on payload fields

**An earlier draft of this document claimed payload-field partitioning was impractical and proposed
partitioning only on Kafka-derived columns appended as suffix bytes. That was wrong on both counts.**
Normal Iceberg partitioning works, on ordinary payload fields, through the existing code paths. The
suffix trick is demoted to an optional feature (§7.4).

The correction rests on two observations: the Avro writer schema is a complete substitute for a
Connect schema on the DDL side, and Iceberg's partitioning path only ever touches a row through a
narrow `StructLike` interface on the value side. So both halves are adapter problems, not redesigns.

### 7.1 DDL side — derive the Iceberg schema from the Avro schema

`IcebergWriterFactory.autoCreateTable` derives a `StructType` from the Connect `valueSchema`, wraps
it in an `org.apache.iceberg.Schema`, and hands that to `SchemaUtils.createPartitionSpec` and
`catalog.createTable`. Only the *first* step is Connect-specific. Replace it:

```java
// today — dies on an opaque BYTES valueSchema:
structType = SchemaUtils.toIcebergType(sample.valueSchema(), config).asStructType();

// raw mode — the Avro writer schema is already richer than the Connect schema:
org.apache.iceberg.Schema schema = AvroSchemaUtil.toIceberg(avroWriterSchema);
```

`AvroSchemaUtil.toIceberg(Schema)` is public (`AvroSchemaUtil.java:88`) and returns an
`org.apache.iceberg.Schema` directly. Everything downstream is untouched:

- `SchemaUtils.createPartitionSpec(schema, partitionBy)` (`SchemaUtils.java:154`) is **purely
  name-based** — it parses `day(ts)` / `bucket(id, 16)` strings and calls `specBuilder.day("ts")`.
  It never inspects a Connect schema. ✅
- `TableMetadata.newTableMetadata` re-assigns field IDs anyway
  (`TypeUtil.assignFreshIds`, `TableMetadata.java:122`) and re-binds the spec **by source name**
  (`TableMetadata.java:132`). ✅

Two consequences worth stating explicitly:

- **The IDs you pass to `createTable` are provisional.** Iceberg normalises them. So the §5.1
  annotator must read the field IDs back from the *created/loaded* table, never assume the ones
  `toIceberg` invented. (§5.1 already says annotate by name-matching the table schema — this is why.)
- **This is strictly better than the status quo**, not merely equivalent. The Avro schema carries real
  nullability, decimal precision/scale, and date/time logical types; Connect-schema inference is
  lossier — and `inferIcebergType` on a schemaless value is lossier still. Auto-created tables get
  *better* types in raw mode.

Your framing was "a proxy Iceberg schema that internally stores an Avro schema." Worth a refinement:
on the **schema** side a proxy buys nothing — `toIceberg` is a cheap pure function and `Schema`'s
operations (`findField`, `accessorForField`) are already index-backed, so just convert once and cache
per `(schemaName, version)`. The place a proxy is genuinely needed is the **row**, next.

### 7.2 Value side — a `StructLike` proxy over the raw bytes

This is the part that made the earlier draft reach for suffix columns, and it dissolves once you look
at what partitioning actually asks of a row. `PartitionKey.partition(row)` delegates to
`StructTransform.wrap` (`api/.../StructTransform.java:78`), which is:

```java
public void wrap(StructLike row) {
  for (int i = 0; i < transformedTuple.length; i += 1) {
    transformedTuple[i] = transforms[i].apply(accessors[i].get(row));
  }
}
```

It touches **only the partition source fields**, via pre-built accessors and pre-bound transforms.
The accessors bottom out in exactly two calls (`api/.../Accessors.java:70, 168`):

```java
row.get(position, javaClass)          // PositionAccessor — a primitive leaf
row.get(position, StructLike.class)   // WrappedPositionAccessor — descend one level
```

So a `StructLike` façade over the raw payload bytes makes the **entire existing partitioning path
work unchanged** — `PartitionedAppendWriter`, `PartitionKey`, nested accessors, bound transforms,
`toPath()`, the fanout writer's map keying. Nothing needs new partitioning support.

This is an idiomatic Iceberg pattern, not a novel one: `InternalRecordWrapper implements StructLike`
(`data/.../InternalRecordWrapper.java:33`) does precisely this — wraps a row and converts to internal
representations on `get()`, returning nested wrappers for nested structs — and it is what
`PartitionedAppendWriter` already uses today (`PartitionedAppendWriter.java:47`).

**In practice you do not have to write the façade at all.** Empirical validation (§7.3) showed
`InternalReader` already returns a `StructLike` carrying internal representations, so
`partitionKey.partition(projectedRow)` works directly. Keep the reasoning above — it explains *why* a
projected read suffices, since `wrap()` never asks the row for anything but the partition sources —
but the concrete work reduces to the five lines in §7.3. The façade is only needed if you pick
`GenericAvroReader`, whose rows are not `StructLike`.

### 7.3 Which reader backs the extractor — validated

**This section was rewritten after empirical validation reversed its original recommendation.** A
harness built the extractor for real and ran **89 assertions, 0 failures**, across 3 partition specs
(identity on field 1/40, `days()` on a timestamp at field 20, `bucket(16)` on field 40, and `days()`
on the nested `payload.event_ts`), 3 file-schema flavours (IDs from `AvroSchemaUtil.convert`, IDs via
`applyNameMapping` on a raw Kafka schema, and logical types stripped), null/non-null unions,
empty/non-empty and block-framed arrays and maps, and 5 unknown producer fields inserted
**mid-record**. `toPath()`, `equals()` and `hashCode()` all matched the full-decode reference, and
`decoder.isEnd()` was true afterwards — proving the skip walk is byte-exact. ✅

**The validated recipe, all public API:**

```java
Schema projected = TypeUtil.project(tableSchema, partitionSourceIds);   // preserves nesting
InternalReader<StructLike> reader = InternalReader.create(projected);
reader.setSchema(annotatedFileAvroSchema);
StructLike row = reader.read(reuse, DecoderFactory.get().binaryDecoder(bytes, off, len, null));
partitionKey.partition(row);                                            // zero glue
```

`TypeUtil.project(schema, {1, 104})` yields `struct<1: event_id: string, 27: payload: struct<104:
event_ts: timestamptz>>` — nesting preserved, siblings dropped. (`TypeUtil.select` is equivalent for
leaf-only ID sets but differs when a struct ID is included, so prefer `project`.)

| | cheap `skip()`? | representation | `StructLike`? | plain `fixed(N)` |
|---|---|---|---|---|
| **`InternalReader`** ✅ **use this** | ✅ | `String` / `Long` — exactly what transforms want | ✅ **yes** | ❌ desyncs |
| `GenericAvroReader` | ✅ | `Utf8` / `Long` — transforms accept both | ❌ `GenericData.Record` | ✅ correct |
| `PlannedDataReader` | ❌ zero overrides | `OffsetDateTime` → `ClassCastException` | ✅ | — |

- **`InternalReader` wins**, contrary to my earlier reasoning. It is the only reader giving **both** a
  `StructLike` **and** the internal Java types `Transform.bind().apply()` wants, so
  `partitionKey.partition(row)` works with **zero glue** — the §7.2 façade is not needed at all.
- **`GenericAvroReader` loses on one point only:** it returns `org.apache.avro.generic.GenericData
  .Record`, which is *not* a `StructLike`, so there is no accessor path. Its values are fine
  (`Utf8` is a `CharSequence`, timestamps are `Long`) but you would have to read positions manually
  and call `PartitionKey.set()` — the façade I proposed. Real, but strictly more code.
- **`PlannedDataReader` is doubly wrong**, as originally stated: `GenericReaders` has *zero* `skip()`
  overrides (so skips fall through to `default skip(d) { read(d, null); }`, `ValueReader.java:26` —
  decode and allocate), and `days.bind(...).apply(OffsetDateTime)` throws `ClassCastException`.
- ⚠️ **`InternalReader`'s one narrow trap:** `case FIXED: case BYTES: → ValueReaders.byteBuffers()`
  (`InternalReader.java:224-226`), whose skip is `skipBytes()` (varint length + skip). A real Avro
  `fixed(N)` has no length prefix, so a **plain `fixed(N)`** field anywhere in the skip path desyncs
  the stream. Narrower than I first wrote: `decimal` routes through `decimalBytesReader` (which
  dispatches correctly on FIXED vs BYTES) and `uuid` through `uuids()` → `skipFixed(16)`, so both are
  **safe**. Only `fixed` with *no logical type* is affected. Note this makes a schema raw-*writable*
  (§6 lists plain `fixed` as eligible) but not raw-*partitionable* — the gate needs both notions.

**Measured — end-to-end, against the real four-stage pipeline.** This is the number that matters, and
it is better than any earlier draft claimed. (Apple Silicon, JDK 21, Avro 1.12.1, 45-field / 518 B
record, 1024 *distinct* pooled records, 1M iters, 3 warmup rounds, checksum-accumulated.)

| today's pipeline | ns/record |
|---|---|
| ① full `GenericDatumReader` decode | 849 |
| ② `AvroData.toConnectData` → Connect `Struct` | **2304** ← dominates |
| ③ Struct → Iceberg `Record` (*lower bound*, stand-in for `RecordConverter`) | 134 |
| ④ `GenericDatumWriter` re-encode | 1087 |
| **total (≥)** | **4374** |

| proposed | ns/record |
|---|---|
| memcpy + partition extraction | **383 – 481** |

**9.1–11.4× cheaper end to end.** Partition extraction is only 9–11% of the ~3900 ns saved — it does
not negate the benefit. The key insight: **stage ② alone is 2.7× stage ①.** Materialising the Connect
`Struct` is the single largest cost in the current path, and raw mode deletes it outright. Any framing
that compares extraction against stage ① alone understates the case.

⚠️ **Three earlier quantitative claims in this document were refuted on measurement:**

- 🔑 **A sizeable collection ahead of the partition source destroys the win.** With an array of 100
  nested records at field 21 and the source at field 40, extraction costs **2380 ns** — 11× above the
  band an earlier draft quoted. Avro's `BinaryDecoder.doSkipItems` only bulk-skips blocks written with
  a **negative** block count plus byte size (`BlockingBinaryEncoder`); standard `BinaryEncoder` — what
  Kafka producers use — writes *positive* counts, so `skipArray` returns N and the caller walks all N
  elements. `ValueReaders.ArrayReader.skip` does exactly that walk. Cost is **linear at ~21 ns per
  element** (verified over a 0/1/5/10/50/100/500 sweep). This is the realistic ad-tech shape, so it
  is a first-order design concern, not a footnote.
- **The practical ratio for extraction alone is ~2–4×, not 2–11×**, and the low end breaches 2×
  (1.81–1.98× in the cleanest full-decode runs). The 11× figure only applies to a single early field.
- 🔑 **`PlannedStructReader.read()` has no early exit, and this is worse than "an optimisation".** It
  walks the entire read plan, skipping every field past the last projected one. Measured with the
  100-element array at field 21 but *all* partition sources early (f1/f5/f10): hand-rolled early-exit
  **122 ns** vs Iceberg's shipped reader **2513 ns** — 20.6× worse and *identical* to its cost when the
  source is at field 40. So "put the partition sources early" buys **nothing** with the stock reader.
  **Root-plan truncation is therefore mandatory, not optional** — it is the only thing that makes the
  early-field case fast. The extractor harness measured the same effect: 367 → **58 ns**. ⚠️ Safe
  **only at the root struct**: truncating a *nested* struct's plan leaves the decoder mid-struct and
  every following sibling reads from the wrong offset — silent corruption, not an exception.

**The resulting design lever.** Field ordering plus truncation is the difference between marginal and
overwhelming:

| configuration | proposed path | vs today's 4374 ns |
|---|---|---|
| sources early + root plan truncated | 65 + 122 = **187 ns** | **23×** |
| no large collection, sources late | 11 + 372…470 = 383–481 ns | 9–11× |
| 100-element array *before* the source, no truncation | 65 + 2380 = 2445 ns | **1.8×** ← marginal |

So: **place partition-source fields ahead of any large collection in the writer schema, and truncate
the root read plan.** If a payload cannot satisfy that, raw mode still wins on the write path (stages
②③④ still vanish) but partition extraction erodes most of the gain — that combination should either
fall back to the decode path or accept the reduced win explicitly.

Two smaller results, both confirming:

- Optional unions in the skip prefix cost ~1.1 ns each (+5.6% over 16 of them) — a non-issue.
- `skipString` is genuinely O(1) in string length: 62× more string bytes changed skip cost by 0%.
  But **memcpy is O(bytes)** — 11 ns at 518 B, 212 ns at 10 KB, where it becomes 36% of the proposed
  path. The "~20 ns memcpy" premise holds only for sub-KB payloads. Still trivial against 4374 ns.
- Iceberg's generic skip machinery is **not** the bottleneck: a hand-written skip loop measured 363 ns
  vs 383 ns for the read-plan version — 5%. No reason to hand-roll skip logic (other than early exit).
- ⚠️ Quantified confirmation that **`PlannedDataReader` must not be used**: on a timestamp-heavy record,
  projection buys **0.5%** (1018 vs 1023 ns) because the `GenericReaders` default-skip builds and
  discards 20 `OffsetDateTime` objects per record on the *skip* path (1168 alloc-bytes/record).
  `GenericAvroReader` on the identical projection: **163 ns**, 6.2× faster. `InternalReader` is
  unaffected by the same defect (it maps date/time to `ints()`/`longs()`, which do override `skip`).

Existing Iceberg rules still bound what can be a partition source: the source must be a **primitive**
with all-struct ancestors — nothing beneath a `list` or `map` (`PartitionSpec.checkCompatibility`;
`Accessors` builds no accessors under list/map). That is unchanged from today, not a raw-mode
restriction. Nested sources do work: `PartitionField.sourceId()` resolves to the **leaf** field ID and
`schema.accessorForField(leafId)` returns a chained `Position2Accessor`. ⚠️ Pass an explicit partition
field name — `builderFor(schema).day("payload.event_ts")` auto-names it `payload.event_ts_day`, leaking
a dot into partition paths and metadata column names.

### 7.4 Kafka-metadata columns (now optional)

Only if you want `_kafka_topic` / `_kafka_partition` / `_kafka_offset` / `_kafka_timestamp` as
*queryable columns*. Not needed for partitioning any more.

```
file schema Sv'  =  Sv (annotated)  ++  [_kafka_topic, _kafka_partition, _kafka_offset, _kafka_ts]
file datum       =  payload bytes B ++ suffix bytes E
```

Valid because a record is a bare concatenation (§2-②): the reader consumes `Sv`'s fields from `B`,
then continues into `E`. Verified byte-identical to a full re-encode against the extended schema. ✅

⚠️ **End of the flattened depth-first encoding order only** — "end of some nested record" is not the
same thing, and a mid-record insertion silently corrupts the datum. Also ⚠️ changing an *existing*
field's nullability breaks passthrough entirely: making a field optional interleaves union tag bytes
(`[84,10,…]` → `[2,84,2,10,…]`), so the prefix is no longer reusable.

`E` is a few varints plus a short string; topic and partition are constant per writer, so memoise per
`(topic, partition)` and vary only offset and timestamp.

### 7.5 ⚠️ A wrong partition value is silent data corruption

This is the sharpest edge in the whole design, and it is worth more than a footnote. **Nothing
validates that a data file's partition value agrees with its contents** — not `ManifestWriter.addEntry`,
not `MergingSnapshotProducer.add`, not the read path. A wrong partition commits with no error. Worse:

- For an **identity**-partitioned column, `PartitionUtil.constantsMap` maps `sourceId → partition
  value` and the read plan substitutes `ValueReaders.replaceWithConstant` — **even when the column is
  physically present in the file**. So a wrong partition value silently *rewrites that column's
  apparent value* for every reader. Parquet behaves identically.
- For **non-identity** transforms there is no constant substitution, but `ResidualEvaluator` drops any
  predicate the (wrong) partition value appears to prove, so no row-level re-check happens: rows are
  returned under the wrong predicate and hidden from the right one.

So the extractor is safety-critical in the same class as the eligibility gate. Practical mitigations:
run the shadow-validation sampling of §6 with a full decode and assert the extracted partition values
match; prefer non-identity transforms (`day`, `bucket`) over `identity` for payload-derived partition
columns, since identity is the one that rewrites values; and treat any extractor change as requiring
a re-verification pass, not just a unit test.

⚠️ **A second silent-failure mode, found empirically:** if the file schema has **no field IDs**, every
field gets `partner == null`, nothing is projected, and you get an **all-null `PartitionKey` with no
exception** — every record lands in the same wrong partition. `AvroWithPartnerVisitor.visitRecord`
only looks up a partner when `AvroSchemaUtil.fieldId(field) != null`. So the annotation step of §5.1
is a hard precondition for the extractor, not just for reads, and the extractor must assert that the
projected read actually populated every partition position before trusting the key. Use
`AvroSchemaUtil.applyNameMapping(rawKafkaSchema, MappingUtil.create(tableSchema))` to attach them; it
preserves logical types.

### 7.6 Extractor cache invalidation

The skip plan is derived from (projected table schema × Avro writer schema), so cache it under
`(tableUuid, table.schema().schemaId(), table.spec().specId(), writerSchemaVersion)`. Invalidation
triggers: table column add/rename/reorder (ID stitching is name-based), spec change, type widening
(transforms must be re-bound), and any new writer-schema version. In practice kafka-connect only ever
adds columns, widens types, and makes columns optional — never renames or reorders — so new columns
are harmless to the plan and widening is the one case that must re-bind. Out-of-band renames from
another writer are the real hazard.

---

## 8. Sink-side blockers to fix

Kafka Connect itself is clean: `WorkerSinkTask` never validates or copies the converter's value
(there is no `ConnectSchema.validateValue` call in connect-runtime), the only per-record touch is an
unevaluated TRACE log placeholder, and the DLQ path re-sends the original `ConsumerRecord` bytes. An
opaque value reaches `IcebergWriter.write` byte-identical — exactly as Kafka's own
`ByteArrayConverter` already does. ✅

Four things to change:

1. **`RecordConverter.convert` accepts only `Struct` and `Map`** and throws otherwise
   (`RecordConverter.java:115-118`). Add the raw branch in `IcebergWriter.write`, upstream of it.
2. **`RecordUtils.extractFromRecordValue` likewise** — this breaks *both* `route.field` modes. Add
   header-based and topic-based routing; `HeaderBasedRouter` already survives opaque values
   untouched.
3. **`IcebergWriterFactory.autoCreateTable`** calls `SchemaUtils.toIcebergType(valueSchema)
   .asStructType()`, which throws on a BYTES schema. Not really a blocker — swap the one line for
   `AvroSchemaUtil.toIceberg(avroWriterSchema)` and the rest of the method works untouched. See §7.1.
4. **`header.converter`** must not be the default `SimpleHeaderConverter`; it mangles the binary
   `schema.store` header through `Values.parseString`. Use `ByteArrayConverter`.

On the PIE side, the win is nearly free: `Type.NULLABLE_BYTES.read()` **already returns a
`ByteBuffer.slice()` sharing the original backing array**. `DPPieDeserializer` throws it away with
`struct.getByteArray(RECORD_KEY_NAME)` (allocate + copy), and the downstream layer then re-wraps it
with `ByteBuffer.wrap`. Plumbing the slice through removes both. There is even an existing hook: the
deserializer already short-circuits to raw bytes when the inner deserializer is a
`ByteBufferDeserializer`, on both the RAW and ENVELOPE branches.

Note that the PIE payload is a **bare Avro datum with no prefix** — the schema lives in the
`schema.store` header, not in a magic-byte envelope. That is a real advantage over Confluent's wire
format, which would need a 5-byte strip (still zero-copy via `slice`, but one more thing to get
right).

**SMT fallout.** Eight of the ten PIE transforms hard-fail on opaque bytes via
`Requirements.requireStruct/requireMap` — `ExtractAndInsert`, `InsertIngestionTimestamp`,
`FlattenWithLevel`, `JsonCast`, and the rest. Survivors: `HeaderBasedRouter` (headers only, passes
the value through untouched) and `EnrichWithExternalAPITransform` in its header mode. Iceberg's own
bundled SMTs are equally value-shape dependent. **Raw mode is only available to pipelines with no
value-touching SMTs** — this needs to be a validated precondition at connector startup, not a
runtime surprise.

---

## 9. The last memcpy

Stock `DataFileWriter` costs exactly one `System.arraycopy` per record into the block staging buffer.
It cannot be avoided through the public API.

A custom OCF writer eliminates it: retain payload references and gather-write them at block flush.
The research built and validated one — ~80 lines, 40 records over 5 blocks, correct read-back and
correct split offsets. Two details it surfaced:

- Avro's `BinaryDecoder` accepts **non-minimal (zero-padded) varints**, so you can reserve a
  fixed-width 5-byte block-length slot and back-patch it. That enables true streaming without
  buffering the block.
- ⚠️ **`DataFileWriter.sync()` returns the wrong value for Iceberg split offsets** — it returns the
  position *after* the trailing sync marker, but `DataFileReader.sync(pos)` KMP-scans *forward* for
  the next marker, so it lands one block late. The correct recipe is `sync() - 16`. Verified: using
  raw `sync()` values dropped 39 of 40 rows. (Moot until we emit split offsets at all, but it is a
  landmine for anyone who tries.)

Treat this as Phase 3. Phase 1's single memcpy of a few-hundred-byte payload is already ~2 orders of
magnitude cheaper than the four tree traversals it replaces.

---

## 10. Phasing

| Phase | Scope | Risk |
|---|---|---|
| **0** | ✅ **Done.** `ZeroCopyAvroBenchmark` (JMH, in the `kafka-connect` module so it can use the real `RecordConverter`). Result: **281×** unpartitioned, 14–16× with partition extraction. See §1. | none |
| **1** | ✅ **Done.** `RawAvroPayload`, `AvroSchemaEligibility`, `AvroSchemaAnnotator`, `EncodedAvroFileAppender`, `EncodedAvroWriterFactory`, all in `org.apache.iceberg.connect.data`. 25 tests, 0 failures. Not yet wired into `IcebergWriter` — that lands with Phase 2, since the trigger for the raw branch is a converter that emits `RawAvroPayload`. | low |
| **2** | ✅ **Runnable end to end.** `RawAvroWriter` (one writer per schema version), config flag `iceberg.tables.raw-avro-enabled`, raw branch in `IcebergWriterFactory`, Avro-derived auto-create, Avro-schema-driven table evolution, and a reference `RawAvroConverter`. 39 tests. See [`ZERO_COPY_RUNNING.md`](ZERO_COPY_RUNNING.md). Still open: header/topic routing (`route-field` needs a decoded value), and the PIE converter itself. | medium |
| **3** | ✅ **Done** (minus truncation). `RawAvroPartitionExtractor`: `TypeUtil.project` + `InternalReader` + `PartitionKey.partition(row)`, writers keyed by `(schemaVersion, partition)`. Nested sources work. Still open: root-plan truncation (§7.3 — the big win when sources sit early, and mandatory if a large collection precedes them), and the §7.5 shadow-validation. | medium |
| **4** | *Optional:* Kafka-metadata suffix columns (§7.4). | low |
| **5** | Custom OCF writer (true zero copy) + split offsets. | low, isolated |

Phase 0 no longer gates the decision to build — it is done, and the answer is 281× unpartitioned
(§1). The cheaper intermediate remains available as a fallback if Phase 1 hits trouble: keep decoding
but skip stages ② and ③ by writing `GenericRecord` straight through `GenericAvroWriter`, which removes
~3000 ns of the ~6200 for a fraction of the complexity and none of the silent-corruption risk.

Phase 3 is independent of Phase 4 and can precede it; partitioned tables need Phase 3, not Phase 4.

---

## 11. Open decisions

1. **Behaviour on gate rejection:** silent fallback to the decode path (safe, opaque) or hard fail
   at connector start (loud, prevents a silent perf cliff)? I'd default to hard-fail at startup with
   an explicit opt-in to fallback.
2. **Writer-key fanout cap** for `(table, partition, schemaVersion)`, and the eviction policy. With
   fanout this is `|schemaVersions| × |partitions|` open files per task, each holding a codec buffer
   and an output stream.
3. **Shadow-validation sampling rate** in steady state — 0, or something small like 1-in-10⁴? Note
   §7.5 argues for keeping it non-zero whenever payload-field partitioning is on.
4. **`identity` on payload-derived partition columns:** allow, or restrict to `day`/`bucket`/
   `truncate`? Identity is the transform where a wrong extracted value silently rewrites the column.
5. **Writer entry point:** `PartitionedAppendWriter` (today's, keeps `WriteResult`/`abort()`) or
   hand-keyed `RollingDataWriter`s (needed to carry the schema-version dimension, since
   `FanoutWriter`'s map is private and keyed only by `(specId, partition)`). The schema-version
   dimension probably forces the latter — worth deciding early since it changes `flush()`.
