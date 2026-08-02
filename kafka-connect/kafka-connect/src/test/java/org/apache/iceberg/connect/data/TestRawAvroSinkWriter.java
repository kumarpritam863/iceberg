/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end for the raw Avro path, through the real sink plumbing.
 *
 * <p>Drives {@link SinkWriter} — the same object {@code IcebergSinkTask} feeds — with {@link
 * SinkRecord}s whose values are {@link RawAvroPayload}, then commits the resulting data files to a
 * real catalog and reads the table back. Everything between the converter and the commit protocol
 * is the production code path: routing, writer creation, the eligibility gate, field-id annotation,
 * and {@code appendEncoded}.
 *
 * <p>The commit protocol itself (control topic, coordinator) needs a Kafka cluster and is exercised
 * by the Docker-based tests in {@code kafka-connect-runtime/src/integration}.
 */
public class TestRawAvroSinkWriter {

  private static final String TABLE_NAME = "db.events";
  private static final TableIdentifier TABLE_ID = TableIdentifier.parse(TABLE_NAME);

  private static final Schema TABLE_SCHEMA =
      new Schema(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.required(2, "name", Types.StringType.get()),
          NestedField.optional(3, "note", Types.StringType.get()),
          NestedField.required(4, "event_ts", Types.TimestampType.withZone()));

  private static final String PRODUCER_SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"com.example\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"long\"},"
          + "{\"name\":\"name\",\"type\":\"string\"},"
          + "{\"name\":\"note\",\"type\":[\"null\",\"string\"]},"
          + "{\"name\":\"event_ts\",\"type\":{\"type\":\"long\","
          + "\"logicalType\":\"timestamp-micros\",\"adjust-to-utc\":true}}]}";

  /** v2 of the same schema: a new optional column appended at the end. */
  private static final String PRODUCER_SCHEMA_V2_JSON =
      "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"com.example\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"long\"},"
          + "{\"name\":\"name\",\"type\":\"string\"},"
          + "{\"name\":\"note\",\"type\":[\"null\",\"string\"]},"
          + "{\"name\":\"event_ts\",\"type\":{\"type\":\"long\","
          + "\"logicalType\":\"timestamp-micros\",\"adjust-to-utc\":true}},"
          + "{\"name\":\"extra\",\"type\":[\"null\",\"string\"]}]}";

  private InMemoryCatalog catalog;

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", ImmutableMap.of());
    catalog.createNamespace(Namespace.of("db"));
  }

  private static IcebergSinkConfig config(Map<String, String> extra) {
    Map<String, String> props = Maps.newHashMap();
    props.put("iceberg.catalog.type", "hadoop");
    props.put("iceberg.tables", TABLE_NAME);
    props.put("iceberg.tables.raw-avro-enabled", "true");
    props.put("name", "raw-avro-test");
    props.putAll(extra);
    return new IcebergSinkConfig(props);
  }

  @Test
  public void testRecordsLandInTheTableWithoutBeingDecoded() throws IOException {
    Table table = catalog.createTable(TABLE_ID, TABLE_SCHEMA, PartitionSpec.unpartitioned());
    SinkWriter sinkWriter = new SinkWriter(catalog, config(ImmutableMap.of()));

    org.apache.avro.Schema producer = parse(PRODUCER_SCHEMA_JSON);
    for (int i = 0; i < 30; i++) {
      sinkWriter.save(Lists.newArrayList(rawRecord(producer, "com.example.Event", 1, i)));
    }

    commit(table, sinkWriter);

    List<Record> rows = readTable(table);
    assertThat(rows).hasSize(30);
    assertThat(rows.stream().map(r -> r.getField("id"))).contains(0L, 15L, 29L);
    assertThat(rows.stream().map(r -> r.getField("name").toString())).contains("name-7");
    // Odd seeds carry a note; even ones are null. Both union branches must survive the round trip.
    assertThat(rows.stream().filter(r -> r.getField("note") == null)).hasSize(15);
  }

  @Test
  public void testInterleavedSchemaVersionsProduceOneFilePerVersion() throws IOException {
    Table table = catalog.createTable(TABLE_ID, TABLE_SCHEMA, PartitionSpec.unpartitioned());
    SinkWriter sinkWriter =
        new SinkWriter(
            catalog, config(ImmutableMap.of("iceberg.tables.evolve-schema-enabled", "true")));

    org.apache.avro.Schema v1 = parse(PRODUCER_SCHEMA_JSON);
    org.apache.avro.Schema v2 = parse(PRODUCER_SCHEMA_V2_JSON);

    // An Avro file header holds one schema, so versions cannot share a file even though Kafka
    // interleaves them.
    for (int i = 0; i < 10; i++) {
      sinkWriter.save(
          Lists.newArrayList(
              rawRecord(v1, "com.example.Event", 1, i),
              rawRecordV2(v2, "com.example.Event", 2, 100 + i)));
    }

    List<DataFile> files = collectFiles(sinkWriter);
    assertThat(files).as("one file per schema version").hasSize(2);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(20L);

    AppendFiles append = table.newAppend();
    files.forEach(append::appendFile);
    append.commit();

    // v2 added a column, so the table was evolved to hold it; v1 files simply lack it and read
    // null.
    table.refresh();
    assertThat(table.schema().findField("extra")).isNotNull();
    assertThat(table.schema().findField("extra").isOptional()).isTrue();

    List<Record> rows = readTable(table);
    assertThat(rows).hasSize(20);
    assertThat(rows.stream().map(r -> r.getField("id"))).contains(0L, 9L, 100L, 109L);
    assertThat(rows.stream().filter(r -> r.getField("extra") != null)).hasSize(10);
  }

  @Test
  public void testTableIsAutoCreatedFromTheAvroWriterSchema() throws IOException {
    SinkWriter sinkWriter =
        new SinkWriter(
            catalog, config(ImmutableMap.of("iceberg.tables.auto-create-enabled", "true")));

    org.apache.avro.Schema producer = parse(PRODUCER_SCHEMA_JSON);
    sinkWriter.save(Lists.newArrayList(rawRecord(producer, "com.example.Event", 1, 1)));

    Table table = catalog.loadTable(TABLE_ID);
    // The Avro schema carries real nullability and logical types, so the created table gets better
    // types than Connect-schema inference would have produced.
    assertThat(table.schema().findField("id").type()).isEqualTo(Types.LongType.get());
    assertThat(table.schema().findField("name").isRequired()).isTrue();
    assertThat(table.schema().findField("note").isOptional()).isTrue();
    assertThat(table.schema().findField("event_ts").type())
        .isEqualTo(Types.TimestampType.withZone());

    commit(table, sinkWriter);
    assertThat(readTable(catalog.loadTable(TABLE_ID))).hasSize(1);
  }

  @Test
  public void testIneligibleSchemaFailsLoudlyRatherThanCorruptingData() {
    catalog.createTable(TABLE_ID, TABLE_SCHEMA, PartitionSpec.unpartitioned());
    SinkWriter sinkWriter = new SinkWriter(catalog, config(ImmutableMap.of()));

    // timestamp-millis has the same layout as Iceberg's timestamp but readers multiply by 1000, so
    // passthrough would silently produce values 1000x off. The gate must stop it.
    org.apache.avro.Schema millis =
        parse(
            "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"com.example\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"long\"},"
                + "{\"name\":\"name\",\"type\":\"string\"},"
                + "{\"name\":\"note\",\"type\":[\"null\",\"string\"]},"
                + "{\"name\":\"event_ts\",\"type\":{\"type\":\"long\","
                + "\"logicalType\":\"timestamp-millis\"}}]}");

    GenericData.Record record = new GenericData.Record(millis);
    record.put("id", 1L);
    record.put("name", "n");
    record.put("note", null);
    record.put("event_ts", 1_700_000_000_000L);

    assertThatThrownBy(
            () ->
                sinkWriter.save(
                    Lists.newArrayList(sinkRecord(encodeUnchecked(record, millis), millis, 1))))
        .hasMessageContaining("cannot be written via raw passthrough")
        .hasMessageContaining("1000x wrong");
  }

  @Test
  public void testPartitionedTablePartitionsFromThePayload() throws IOException {
    // day(event_ts) -- the partition value has to come out of the payload bytes without decoding
    // the
    // rest of the record.
    Table table =
        catalog.createTable(
            TABLE_ID,
            TABLE_SCHEMA,
            PartitionSpec.builderFor(TABLE_SCHEMA).day("event_ts", "event_day").build());
    SinkWriter sinkWriter = new SinkWriter(catalog, config(ImmutableMap.of()));

    org.apache.avro.Schema producer = parse(PRODUCER_SCHEMA_JSON);
    // Three distinct days, a day apart in micros.
    long dayMicros = 86_400_000_000L;
    for (int day = 0; day < 3; day++) {
      for (int i = 0; i < 4; i++) {
        sinkWriter.save(
            Lists.newArrayList(
                rawRecordAt(producer, day * 4 + i, 1_700_000_000_000_000L + day * dayMicros)));
      }
    }

    List<DataFile> files = collectFiles(sinkWriter);
    assertThat(files).as("one file per day").hasSize(3);
    assertThat(files.stream().mapToLong(DataFile::recordCount).sum()).isEqualTo(12L);

    // Each file must be tagged with the partition its rows actually belong to.
    assertThat(files.stream().map(f -> table.spec().partitionToPath(f.partition())))
        .containsExactlyInAnyOrder(
            "event_day=2023-11-14", "event_day=2023-11-15", "event_day=2023-11-16");

    AppendFiles append = table.newAppend();
    files.forEach(append::appendFile);
    append.commit();
    assertThat(readTable(table)).hasSize(12);
  }

  @Test
  public void testPartitionedTableRejectsASchemaItCannotSkip() {
    // A plain fixed(N) makes InternalReader's skip read a length prefix that is not there, which
    // would desynchronize the walk. Writable, but not partitionable -- and this table is
    // partitioned.
    Schema schema =
        new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()),
            NestedField.optional(3, "note", Types.StringType.get()),
            NestedField.required(4, "event_ts", Types.TimestampType.withZone()),
            NestedField.required(5, "digest", Types.FixedType.ofLength(8)));
    catalog.createTable(
        TABLE_ID, schema, PartitionSpec.builderFor(schema).day("event_ts", "event_day").build());
    SinkWriter sinkWriter = new SinkWriter(catalog, config(ImmutableMap.of()));

    org.apache.avro.Schema producer =
        parse(
            "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"com.example\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"long\"},"
                + "{\"name\":\"name\",\"type\":\"string\"},"
                + "{\"name\":\"note\",\"type\":[\"null\",\"string\"]},"
                + "{\"name\":\"event_ts\",\"type\":{\"type\":\"long\","
                + "\"logicalType\":\"timestamp-micros\",\"adjust-to-utc\":true}},"
                + "{\"name\":\"digest\",\"type\":{\"type\":\"fixed\",\"name\":\"D8\",\"size\":8}}]}");

    GenericData.Record record = new GenericData.Record(producer);
    record.put("id", 1L);
    record.put("name", "n");
    record.put("note", null);
    record.put("event_ts", 1_700_000_000_000_000L);
    record.put("digest", new GenericData.Fixed(producer.getField("digest").schema(), new byte[8]));

    assertThatThrownBy(
            () ->
                sinkWriter.save(
                    Lists.newArrayList(sinkRecord(encodeUnchecked(record, producer), producer, 1))))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("partition values cannot be extracted");
  }

  @Test
  public void testPartitionSourceMissingFromTheProducerSchemaIsRejected() {
    // The producer never sends the column the spec partitions on, so every record would land in one
    // null partition. Refuse rather than quietly mis-partition the whole topic.
    Schema schema =
        new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()),
            NestedField.optional(3, "note", Types.StringType.get()),
            NestedField.required(4, "event_ts", Types.TimestampType.withZone()),
            NestedField.optional(5, "region", Types.StringType.get()));
    catalog.createTable(
        TABLE_ID, schema, PartitionSpec.builderFor(schema).identity("region").build());
    SinkWriter sinkWriter = new SinkWriter(catalog, config(ImmutableMap.of()));

    org.apache.avro.Schema producer = parse(PRODUCER_SCHEMA_JSON);
    assertThatThrownBy(
            () ->
                sinkWriter.save(Lists.newArrayList(rawRecord(producer, "com.example.Event", 1, 1))))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("does not contain the partition source field");
  }

  @Test
  public void testTombstonesAreSkipped() throws IOException {
    Table table = catalog.createTable(TABLE_ID, TABLE_SCHEMA, PartitionSpec.unpartitioned());
    SinkWriter sinkWriter = new SinkWriter(catalog, config(ImmutableMap.of()));

    org.apache.avro.Schema producer = parse(PRODUCER_SCHEMA_JSON);
    sinkWriter.save(
        Lists.newArrayList(
            rawRecord(producer, "com.example.Event", 1, 1),
            new SinkRecord("topic", 0, null, null, null, null, 99L),
            rawRecord(producer, "com.example.Event", 1, 2)));

    commit(table, sinkWriter);
    assertThat(readTable(table)).hasSize(2);
  }

  @Test
  public void testRawModeOffFallsBackToTheDecodePath() {
    catalog.createTable(TABLE_ID, TABLE_SCHEMA, PartitionSpec.unpartitioned());
    SinkWriter sinkWriter =
        new SinkWriter(
            catalog, config(ImmutableMap.of("iceberg.tables.raw-avro-enabled", "false")));

    org.apache.avro.Schema producer = parse(PRODUCER_SCHEMA_JSON);
    // With the flag off, RecordConverter sees the payload object and rejects it -- proving the
    // branch
    // is genuinely gated on config rather than on the value type alone.
    assertThatThrownBy(
            () ->
                sinkWriter.save(Lists.newArrayList(rawRecord(producer, "com.example.Event", 1, 1))))
        .rootCause()
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Cannot convert type")
        .hasMessageContaining("ByteBuffer");
  }

  // ------------------------------------------------------------- helpers

  private static org.apache.avro.Schema parse(String json) {
    return new org.apache.avro.Schema.Parser().parse(json);
  }

  private void commit(Table table, SinkWriter sinkWriter) {
    List<DataFile> files = collectFiles(sinkWriter);
    if (files.isEmpty()) {
      return;
    }
    AppendFiles append = table.newAppend();
    files.forEach(append::appendFile);
    append.commit();
  }

  private List<DataFile> collectFiles(SinkWriter sinkWriter) {
    return sinkWriter.completeWrite().writerResults().stream()
        .flatMap(result -> result.dataFiles().stream())
        .collect(java.util.stream.Collectors.toList());
  }

  private static List<Record> readTable(Table table) throws IOException {
    table.refresh();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      return Lists.newArrayList(rows);
    }
  }

  private static SinkRecord rawRecord(
      org.apache.avro.Schema schema, String schemaName, int version, int seed) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("id", (long) seed);
    record.put("name", "name-" + seed);
    record.put("note", seed % 2 == 0 ? null : "note-" + seed);
    record.put("event_ts", 1_700_000_000_000_000L + seed);
    return sinkRecord(encodeUnchecked(record, schema), schema, version, schemaName, seed);
  }

  private static SinkRecord rawRecordAt(
      org.apache.avro.Schema schema, int seed, long eventTsMicros) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("id", (long) seed);
    record.put("name", "name-" + seed);
    record.put("note", seed % 2 == 0 ? null : "note-" + seed);
    record.put("event_ts", eventTsMicros);
    return sinkRecord(encodeUnchecked(record, schema), schema, 1, schema.getFullName(), seed);
  }

  private static SinkRecord rawRecordV2(
      org.apache.avro.Schema schema, String schemaName, int version, int seed) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("id", (long) seed);
    record.put("name", "name-" + seed);
    record.put("note", seed % 2 == 0 ? null : "note-" + seed);
    record.put("event_ts", 1_700_000_000_000_000L + seed);
    record.put("extra", "extra-" + seed);
    return sinkRecord(encodeUnchecked(record, schema), schema, version, schemaName, seed);
  }

  private static SinkRecord sinkRecord(byte[] payload, org.apache.avro.Schema schema, int version) {
    return sinkRecord(payload, schema, version, schema.getFullName(), 0);
  }

  /**
   * What a raw-mode converter emits: the undecoded payload as bytes, plus three headers describing
   * its writer schema. No shared custom type crosses the converter boundary, which is what makes
   * this immune to Connect's plugin classloader isolation.
   */
  private static SinkRecord sinkRecord(
      byte[] payload, org.apache.avro.Schema schema, int version, String schemaName, int offset) {
    SinkRecord record =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            SchemaBuilder.type(Type.BYTES).optional().name("RawAvro").build(),
            ByteBuffer.wrap(payload),
            offset);
    record.headers().addString("iceberg.avro.schema.name", schemaName);
    record.headers().addString("iceberg.avro.schema.version", Integer.toString(version));
    record.headers().addString("iceberg.avro.schema", schema.toString());
    return record;
  }

  private static byte[] encodeUnchecked(GenericData.Record record, org.apache.avro.Schema schema) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      new GenericDatumWriter<GenericData.Record>(schema).write(record, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
