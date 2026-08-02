# Running the zero-copy Avro path

Design: [`ZERO_COPY_AVRO_DESIGN.md`](ZERO_COPY_AVRO_DESIGN.md).

All `gradlew` commands need an explicit JDK — the default `java` on this machine is Java 8 and Gradle
refuses to configure. Set it once:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

---

## 1. Fast path: no Kafka, no Docker (~5 seconds)

Drives the real `SinkWriter` — the same object `IcebergSinkTask` feeds — with records whose values are
undecoded Avro bytes plus the three describing headers, commits the data files to a catalog, and reads
the table back. Covers the header contract, routing, writer creation, the eligibility gate, field-id
annotation, schema evolution, and `appendEncoded`.

```bash
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test \
  --tests 'org.apache.iceberg.connect.data.TestRawAvroSinkWriter' \
  -DsparkVersions= -DflinkVersions=
```

The whole raw-path suite (49 tests):

```bash
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test \
  --tests 'org.apache.iceberg.connect.data.TestRawAvro*' \
  --tests 'org.apache.iceberg.connect.data.TestEncodedAvro*' \
  --tests 'org.apache.iceberg.connect.data.TestAvroSchema*' \
  --tests 'org.apache.iceberg.connect.TestRawAvroConverter' \
  -DsparkVersions= -DflinkVersions=
```

What this does **not** cover: the commit protocol (control topic, coordinator), which needs Kafka. It
is unaffected by this change — the coordinator only reads `recordCount()` off each `DataFile` — but if
you want it exercised, use §2.

## 2. Full stack: real Kafka, real Connect worker, real commits

The repo already ships a docker-compose harness: Kafka, MinIO, an Iceberg REST catalog, and a
`cp-kafka-connect` worker with the built distribution mounted as its only plugin directory.

Docker is required. On macOS you may need:

```bash
sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock
```

Build the distribution — this is what the worker loads:

```bash
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect-runtime:installDist \
  -DsparkVersions= -DflinkVersions=
```

Verify the raw-path classes made it in:

```bash
unzip -l kafka-connect/kafka-connect-runtime/build/install/iceberg-kafka-connect-runtime/lib/iceberg-kafka-connect-*.jar \
  | grep RawAvro
```

Bring the stack up:

```bash
cd kafka-connect/kafka-connect-runtime
docker compose -f docker/docker-compose.yml up -d
# wait for http://localhost:8083/connectors to answer
```

### Connector config

`RawAvroConverter` ships in the same jar as the connector here, but that is incidental — the header
contract means it does not have to (see "The converter contract" below).

```json
{
  "name": "iceberg-raw-avro",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "1",
    "topics": "events",

    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "header.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.iceberg.connect.RawAvroConverter",
    "value.converter.schema": "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"note\",\"type\":[\"null\",\"string\"]},{\"name\":\"event_ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\",\"adjust-to-utc\":true}}]}",
    "value.converter.schema.name": "com.example.Event",
    "value.converter.schema.version": "1",

    "iceberg.tables": "db.events",
    "iceberg.tables.raw-avro-enabled": "true",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.tables.auto-create-props.write.format.default": "avro",

    "iceberg.catalog": "demo",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg:8181",
    "iceberg.catalog.warehouse": "s3://warehouse",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.client.region": "us-east-1",
    "iceberg.catalog.s3.access-key-id": "minioadmin",
    "iceberg.catalog.s3.secret-access-key": "minioadmin",

    "iceberg.control.commit.interval-ms": "5000"
  }
}
```

```bash
curl -sX POST -H 'Content-Type: application/json' \
  --data @connector.json http://localhost:8083/connectors | jq .
curl -s http://localhost:8083/connectors/iceberg-raw-avro/status | jq .
```

### Producing

The payload must be a **bare Avro datum** — no Confluent magic byte, no length prefix. Produce with a
plain `ByteArraySerializer` over `GenericDatumWriter` output. `TestRawAvroSinkWriter.encodeUnchecked`
shows the encoding; the Avro schema on the producer must match `value.converter.schema` exactly,
including field order and union branch order.

If your producer emits the 5-byte Confluent prefix, strip it in a converter with
`ByteBuffer.wrap(bytes, 5, bytes.length - 5).slice()` — slicing keeps it zero-copy.

### Tear down

```bash
docker compose -f docker/docker-compose.yml down
```

---

## Requirements and current limits

| | |
|---|---|
| Table format | **Avro** (`write.format.default=avro`). The bytes are Avro; Parquet tables cannot take them. |
| Partitioning | **Supported**, on ordinary payload fields including nested ones. Partition values are decoded from the payload with everything else skipped (§7). A schema whose bytes cannot be safely skipped, or one missing a partition-source column, is refused at writer creation. |
| Deletes | Append-only. Equality/position delete writers throw. |
| Value SMTs | None. Any SMT that touches the value (`requireStruct`/`requireMap`) fails on an opaque payload. Header- and topic-only SMTs are fine. |
| Schema | Must pass `AvroSchemaEligibility`. See §6 of the design for the full matrix. |
| Metrics | Record count only — same as every Avro file Iceberg writes today (`AvroMetrics.fromWriter` is a stub). |

## The converter contract

Nothing but **bytes and strings** crosses from the converter to the sink. That is deliberate: Kafka
Connect's `PluginClassLoader` is child-first for every package outside `java*`, `javax*`,
`org.apache.kafka` and `org.slf4j`, so a shared custom value type would resolve to two different
`Class` objects whenever the converter and the connector sit in different plugin directories — and fail
on every record. Strings have no such problem, and a converter needs **no dependency on Iceberg**.

A converter emits the undecoded payload as the value, and describes it with three headers:

| header | value |
|---|---|
| `iceberg.avro.schema.name` | the writer schema's registry name |
| `iceberg.avro.schema.version` | its registry version, an integer |
| `iceberg.avro.schema` | the writer schema itself, as JSON |

```java
headers.add("iceberg.avro.schema.name", nameBytes);       // cached per version --
headers.add("iceberg.avro.schema.version", versionBytes); // these are references,
headers.add("iceberg.avro.schema", schemaJsonBytes);      // not per-record allocations

return new SchemaAndValue(anyBytesSchema, ByteBuffer.wrap(payload));  // no copy
```

Shipping the whole schema per record costs nothing meaningful: the converter and the sink run in the
same JVM on the same thread, so headers are an in-memory object that is never serialized. The sink
parses the JSON only when it sees a `(name, version)` it has not seen before — the hot path touches
only the two short headers.

Two things matter: the value should be a `ByteBuffer` view (`wrap`/`slice`) rather than a copy, and
`(name, version)` must change whenever the schema does — the sink keys one open Avro file per version
and guards every append against the file's expected version.

⚠️ **`header.converter` must be `StringConverter`.** The default `SimpleHeaderConverter` runs header
values through `Values.parseString`, which would parse the schema JSON into a `Map` rather than leaving
it a string. The sink detects this and says so, but it is easier to just set it.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Header 'iceberg.avro.schema' arrived as ...Map, not a string` | `header.converter` is the default `SimpleHeaderConverter`. Set it to `StringConverter`. |
| `record header 'iceberg.avro.schema.name' is missing` | The value converter is not setting the three headers, or an SMT stripped them. |
| `cannot be written via raw passthrough` | The gate rejected the schema. The message lists every blocker with a field path. |
| `has fields the table ... does not` | Producer moved ahead of the table. Set `iceberg.tables.evolve-schema-enabled=true`, or add the columns. |
| `partition values cannot be extracted` | The schema is writable but not skip-safe — a plain `fixed(N)` — and the table is partitioned. |
| `does not contain the partition source field` | The producer's schema lacks a column the spec partitions on. |
| `Payload schema X vN does not match this file's` | The converter reported a version that changed without the schema changing, or vice versa. |
| `Cannot convert type: ...ByteBuffer` | `iceberg.tables.raw-avro-enabled` is not set, so the record went down the decode path. |
