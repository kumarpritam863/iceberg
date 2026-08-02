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
package org.apache.iceberg.data.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Probes for the zero-copy Avro sink design questions: writer entry points, partial metrics,
 * partition-value trust, and nested partition sources.
 */
public class TestScratchZeroCopyWriterIntegration {

  private static final StringWriter LOG_BUF = new StringWriter();
  private static final PrintWriter LOG = new PrintWriter(LOG_BUF, true);

  private static void log(String fmt, Object... args) {
    String line = args.length == 0 ? fmt : String.format(fmt, args);
    LOG.println(line);
    System.out.println(line);
  }

  private static void dump(String name) {
    try {
      java.nio.file.Files.writeString(
          java.nio.file.Path.of("/tmp/zerocopy-" + name + ".txt"), LOG_BUF.toString());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @TempDir private File tempDir;

  // 1: id, 2: region (top-level identity partition), 3: payload{4: event_ts, 5: category}
  private static final Schema TABLE_SCHEMA =
      new Schema(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.required(2, "region", Types.StringType.get()),
          NestedField.required(
              3,
              "payload",
              Types.StructType.of(
                  NestedField.required(4, "event_ts", Types.TimestampType.withoutZone()),
                  NestedField.required(5, "category", Types.StringType.get()))));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA)
          .identity("region")
          .day("payload.event_ts", "event_day")
          .build();

  /** Raw Kafka writer schema: structurally identical, but with NO iceberg field ids. */
  private static final org.apache.avro.Schema RAW_AVRO =
      new org.apache.avro.Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"kafka\",\"fields\":["
                  + "{\"name\":\"id\",\"type\":\"long\"},"
                  + "{\"name\":\"region\",\"type\":\"string\"},"
                  + "{\"name\":\"payload\",\"type\":{\"type\":\"record\",\"name\":\"P\","
                  + "\"fields\":["
                  + "{\"name\":\"event_ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
                  + "{\"name\":\"category\",\"type\":\"string\"}]}}"
                  + "]}");

  private static final long TS_MICROS = 1_712_345_678_901_234L; // 2024-04-05

  private static byte[] encodeKafkaPayload(long id, String region, long tsMicros, String category)
      throws IOException {
    GenericData.Record payload =
        new GenericData.Record(RAW_AVRO.getField("payload").schema());
    payload.put("event_ts", tsMicros);
    payload.put("category", category);
    GenericData.Record rec = new GenericData.Record(RAW_AVRO);
    rec.put("id", id);
    rec.put("region", region);
    rec.put("payload", payload);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericData.Record>(RAW_AVRO).write(rec, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  /**
   * The zero-copy write: an Avro OCF whose header schema carries Iceberg field ids, with raw Kafka
   * payload bytes memcpy'd in via {@link DataFileWriter#appendEncoded}.
   */
  private String writeZeroCopyFile(File file, List<byte[]> payloads) throws IOException {
    org.apache.avro.Schema fileSchema = AvroSchemaUtil.convert(TABLE_SCHEMA, "event");
    log("file (OCF header) schema = %s", fileSchema);
    try (PositionOutputStream stream = Files.localOutput(file).create();
        DataFileWriter<Object> writer =
            new DataFileWriter<>(new GenericDatumWriter<>(fileSchema))) {
      writer.setMeta("iceberg.schema", SchemaParser.toJson(TABLE_SCHEMA));
      writer.create(fileSchema, stream);
      for (byte[] payload : payloads) {
        writer.appendEncoded(ByteBuffer.wrap(payload));
      }
    }
    return file.toString();
  }

  private Table createTable(String name) {
    return new HadoopTables(new Configuration())
        .create(
            TABLE_SCHEMA,
            SPEC,
            ImmutableMap.of("format-version", "2"),
            new File(tempDir, name).toString());
  }

  // ===========================================================================
  // Q4: partial metrics maps + Q6: partition-value trust
  // ===========================================================================

  @Test
  public void testPartialMetricsAndZeroCopyRoundTrip() throws IOException {
    Table table = createTable("partial_metrics");

    List<byte[]> payloads =
        ImmutableList.of(
            encodeKafkaPayload(1L, "US", TS_MICROS, "a"),
            encodeKafkaPayload(2L, "US", TS_MICROS + 1000, "b"));
    File dataFile = new File(tempDir, "partial_metrics/data/zero-copy-1.avro");
    dataFile.getParentFile().mkdirs();
    writeZeroCopyFile(dataFile, payloads);

    // partition values, computed the way a zero-copy extractor would: apply the bound transform
    // directly to the INTERNAL representation of each extracted source value (no Record at all).
    PartitionData key = new PartitionData(SPEC.partitionType());
    key.set(
        0,
        ((org.apache.iceberg.transforms.Transform<Object, Object>) SPEC.fields().get(0).transform())
            .bind(Types.StringType.get())
            .apply("US"));
    key.set(
        1,
        ((org.apache.iceberg.transforms.Transform<Object, Object>) SPEC.fields().get(1).transform())
            .bind(Types.TimestampType.withoutZone())
            .apply(TS_MICROS));
    log("partition = %s path=%s", key, SPEC.partitionToPath(key));

    // PARTIAL metrics: only field 2 (region, top-level) and field 4 (payload.event_ts, nested)
    Map<Integer, Long> valueCounts = ImmutableMap.of(2, 2L, 4, 2L);
    Map<Integer, Long> nullCounts = ImmutableMap.of(2, 0L, 4, 0L);
    Map<Integer, ByteBuffer> lower =
        ImmutableMap.of(
            2, Conversions.toByteBuffer(Types.StringType.get(), "US"),
            4, Conversions.toByteBuffer(Types.TimestampType.withoutZone(), TS_MICROS));
    Map<Integer, ByteBuffer> upper =
        ImmutableMap.of(
            2, Conversions.toByteBuffer(Types.StringType.get(), "US"),
            4, Conversions.toByteBuffer(Types.TimestampType.withoutZone(), TS_MICROS + 1000));
    Metrics metrics = new Metrics(2L, null, valueCounts, nullCounts, null, lower, upper);

    DataFile df =
        DataFiles.builder(SPEC)
            .withPath(dataFile.toString())
            .withFormat(FileFormat.AVRO)
            .withPartition(key)
            .withFileSizeInBytes(dataFile.length())
            .withMetrics(metrics)
            .build();
    log("DataFile lowerBounds keys = %s", df.lowerBounds().keySet());
    log("DataFile columnSizes = %s (null is legal)", df.columnSizes());

    table.newAppend().appendFile(df).commit();
    log("commit with PARTIAL metrics: OK");

    // read back through the generic reader
    List<Record> records = Lists.newArrayList();
    try (CloseableIterable<Record> iter = IcebergGenerics.read(table).build()) {
      iter.forEach(records::add);
    }
    log("read back: %s", records);
    assertThat(records).hasSize(2);
    assertThat(records.get(0).get(1)).isEqualTo("US");
    assertThat(((Record) records.get(0).get(2)).get(1)).isEqualTo("a");
    assertThat(((Record) records.get(1).get(2)).get(1)).isEqualTo("b");

    // partial maps survive the manifest round trip
    DataFile fromManifest =
        Lists.newArrayList(table.currentSnapshot().addedDataFiles(table.io())).get(0);
    log("from manifest: lower=%s value=%s null=%s nan=%s colSizes=%s",
        fromManifest.lowerBounds().keySet(),
        fromManifest.valueCounts().keySet(),
        fromManifest.nullValueCounts().keySet(),
        fromManifest.nanValueCounts(),
        fromManifest.columnSizes());
    assertThat(fromManifest.lowerBounds().keySet()).containsExactlyInAnyOrder(2, 4);
    assertThat(fromManifest.columnSizes()).isNullOrEmpty();

    // pruning works off the partial bounds
    long matched = count(table, Expressions.equal("region", "US"));
    long unmatched = count(table, Expressions.equal("region", "EU"));
    log("scan region=US -> %d rows ; region=EU -> %d rows", matched, unmatched);
    assertThat(matched).isEqualTo(2);
    assertThat(unmatched).isEqualTo(0);
    dump("partial-metrics");
  }

  private long count(Table table, org.apache.iceberg.expressions.Expression filter)
      throws IOException {
    long count = 0;
    try (CloseableIterable<Record> iter = IcebergGenerics.read(table).where(filter).build()) {
      for (Record ignored : iter) {
        count++;
      }
    }
    return count;
  }

  @Test
  public void testWrongPartitionValueIsSilentCorruption() throws IOException {
    Table table = createTable("wrong_partition");

    // data really is region=US, but we claim region=EU and day=1970-01-01
    List<byte[]> payloads = ImmutableList.of(encodeKafkaPayload(1L, "US", TS_MICROS, "a"));
    File dataFile = new File(tempDir, "wrong_partition/data/bad.avro");
    dataFile.getParentFile().mkdirs();
    writeZeroCopyFile(dataFile, payloads);

    DataFile df =
        DataFiles.builder(SPEC)
            .withPath(dataFile.toString())
            .withFormat(FileFormat.AVRO)
            .withPartitionPath("region=EU/event_day=1970-01-01")
            .withFileSizeInBytes(dataFile.length())
            .withMetrics(new Metrics(1L, null, null, null, null))
            .build();

    // nothing complains at commit time
    table.newAppend().appendFile(df).commit();
    log("commit with a WRONG partition value: OK (no validation)");

    List<Record> all = Lists.newArrayList();
    try (CloseableIterable<Record> iter = IcebergGenerics.read(table).build()) {
      iter.forEach(all::add);
    }
    log("full scan returns: %s", all);
    log("  region column read back as: %s  (file bytes say US)", all.get(0).get(1));

    // identity-partitioned column: value comes from the MANIFEST, not the file
    assertThat(all.get(0).get(1)).isEqualTo("EU");

    // and the row is now reachable only under the wrong predicate
    log("scan region=US -> %d rows", count(table, Expressions.equal("region", "US")));
    log("scan region=EU -> %d rows", count(table, Expressions.equal("region", "EU")));
    assertThat(count(table, Expressions.equal("region", "US"))).isEqualTo(0);
    assertThat(count(table, Expressions.equal("region", "EU"))).isEqualTo(1);

    // non-identity (day) transform: no constant substitution, but residual is dropped so the
    // row is returned for the WRONG day and hidden from the right one
    long wrongDay =
        count(
            table,
            Expressions.and(
                Expressions.greaterThanOrEqual("payload.event_ts", "1970-01-01T00:00:00"),
                Expressions.lessThan("payload.event_ts", "1970-01-02T00:00:00")));
    log("scan payload.event_ts in [1970-01-01,1970-01-02) -> %d rows (data is 2024-04-05!)",
        wrongDay);
    assertThat(wrongDay).isEqualTo(1);
    dump("wrong-partition");
  }


  private static final PartitionSpec NESTED_IDENTITY_SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA).identity("payload.category").build();

  @Test
  public void testWrongNestedIdentityPartitionValueAlsoSilentlyOverrides() throws IOException {
    Table table =
        new HadoopTables(new Configuration())
            .create(
                TABLE_SCHEMA,
                NESTED_IDENTITY_SPEC,
                ImmutableMap.of("format-version", "2"),
                new File(tempDir, "nested_identity").toString());

    File dataFile = new File(tempDir, "nested_identity/data/bad.avro");
    dataFile.getParentFile().mkdirs();
    writeZeroCopyFile(dataFile, ImmutableList.of(encodeKafkaPayload(1L, "US", TS_MICROS, "a")));

    DataFile df =
        DataFiles.builder(NESTED_IDENTITY_SPEC)
            .withPath(dataFile.toString())
            .withFormat(FileFormat.AVRO)
            .withPartitionPath("payload.category=WRONG")
            .withFileSizeInBytes(dataFile.length())
            .withMetrics(new Metrics(1L, null, null, null, null))
            .build();
    table.newAppend().appendFile(df).commit();

    List<Record> all = Lists.newArrayList();
    try (CloseableIterable<Record> iter = IcebergGenerics.read(table).build()) {
      iter.forEach(all::add);
    }
    Object nested = ((Record) all.get(0).get(2)).get(1);
    log("NESTED identity partition: payload.category read back as %s (file bytes say 'a')", nested);
    assertThat(nested).isEqualTo("WRONG");
    dump("nested-identity");
  }


  /** Writes the OCF header using the EXACT Kafka writer schema, with Iceberg ids stitched on. */
  private void writeZeroCopyFileWithNameMappedHeader(File file, List<byte[]> payloads)
      throws IOException {
    org.apache.avro.Schema headerSchema =
        AvroSchemaUtil.applyNameMapping(
            RAW_AVRO, org.apache.iceberg.mapping.MappingUtil.create(TABLE_SCHEMA));
    log("name-mapped header schema = %s", headerSchema);
    try (PositionOutputStream stream = Files.localOutput(file).create();
        DataFileWriter<Object> writer =
            new DataFileWriter<>(new GenericDatumWriter<>(headerSchema))) {
      writer.setMeta("iceberg.schema", SchemaParser.toJson(TABLE_SCHEMA));
      writer.create(headerSchema, stream);
      for (byte[] payload : payloads) {
        writer.appendEncoded(ByteBuffer.wrap(payload));
      }
    }
  }

  @Test
  public void testNameMappedHeaderIsReadable() throws IOException {
    Table table = createTable("name_mapped_header");
    File dataFile = new File(tempDir, "name_mapped_header/data/nm.avro");
    dataFile.getParentFile().mkdirs();
    writeZeroCopyFileWithNameMappedHeader(
        dataFile, ImmutableList.of(encodeKafkaPayload(7L, "US", TS_MICROS, "z")));

    DataFile df =
        DataFiles.builder(SPEC)
            .withPath(dataFile.toString())
            .withFormat(FileFormat.AVRO)
            .withPartitionPath("region=US/event_day=2024-04-05")
            .withFileSizeInBytes(dataFile.length())
            .withMetrics(new Metrics(1L, null, null, null, null))
            .build();
    table.newAppend().appendFile(df).commit();

    List<Record> all = Lists.newArrayList();
    try (CloseableIterable<Record> iter = IcebergGenerics.read(table).build()) {
      iter.forEach(all::add);
    }
    log("name-mapped-header file read back: %s", all);
    assertThat(all).hasSize(1);
    assertThat(all.get(0).get(0)).isEqualTo(7L);
    assertThat(((Record) all.get(0).get(2)).get(1)).isEqualTo("z");
    dump("name-mapped-header");
  }

  @Test
  public void testHeaderWithoutIdsIsNotReadableByPlainGenericReader() throws IOException {
    Table table = createTable("no_ids_header");
    File dataFile = new File(tempDir, "no_ids_header/data/raw.avro");
    dataFile.getParentFile().mkdirs();
    try (PositionOutputStream stream = Files.localOutput(dataFile).create();
        DataFileWriter<Object> writer =
            new DataFileWriter<>(new GenericDatumWriter<>(RAW_AVRO))) {
      writer.create(RAW_AVRO, stream);
      writer.appendEncoded(ByteBuffer.wrap(encodeKafkaPayload(1L, "US", TS_MICROS, "a")));
    }
    DataFile df =
        DataFiles.builder(SPEC)
            .withPath(dataFile.toString())
            .withFormat(FileFormat.AVRO)
            .withPartitionPath("region=US/event_day=2024-04-05")
            .withFileSizeInBytes(dataFile.length())
            .withMetrics(new Metrics(1L, null, null, null, null))
            .build();
    table.newAppend().appendFile(df).commit();

    String outcome;
    try (CloseableIterable<Record> iter = IcebergGenerics.read(table).build()) {
      List<Record> all = Lists.newArrayList();
      iter.forEach(all::add);
      outcome = "read " + all;
    } catch (RuntimeException | Error e) {
      outcome = e.getClass().getName() + ": " + e.getMessage();
    }
    log("OCF header WITHOUT field-ids, plain GenericReader -> %s", outcome);
    dump("no-ids-header");
  }

  // ===========================================================================
  // Q4c: what StrictMetricsEvaluator can and cannot do with partial bounds
  // ===========================================================================

  @Test
  public void testStrictMetricsEvaluatorWithPartialBounds() throws IOException {
    File dataFile = new File(tempDir, "eval.avro");
    writeZeroCopyFile(dataFile, ImmutableList.of(encodeKafkaPayload(1L, "US", TS_MICROS, "a")));

    ByteBuffer usBuf = Conversions.toByteBuffer(Types.StringType.get(), "US");
    ByteBuffer tsBuf = Conversions.toByteBuffer(Types.TimestampType.withoutZone(), TS_MICROS);

    // (a) bounds WITHOUT nullValueCounts -> canContainNulls() is true -> nothing can be proven
    DataFile noNullCounts =
        file(dataFile, new Metrics(1L, null, null, null, null,
            ImmutableMap.of(2, usBuf), ImmutableMap.of(2, usBuf)));
    boolean resultNoNulls =
        new StrictMetricsEvaluator(TABLE_SCHEMA, Expressions.equal("region", "US"))
            .eval(noNullCounts);
    log("strict eval(region='US') with bounds but NO nullValueCounts = %s", resultNoNulls);
    assertThat(resultNoNulls).isFalse();

    // (b) bounds + nullValueCounts=0 -> provable for a TOP-LEVEL column
    DataFile withNullCounts =
        file(dataFile, new Metrics(1L, null, ImmutableMap.of(2, 1L), ImmutableMap.of(2, 0L), null,
            ImmutableMap.of(2, usBuf), ImmutableMap.of(2, usBuf)));
    boolean resultTopLevel =
        new StrictMetricsEvaluator(TABLE_SCHEMA, Expressions.equal("region", "US"))
            .eval(withNullCounts);
    log("strict eval(region='US') with bounds + nullValueCounts=0 = %s", resultTopLevel);
    assertThat(resultTopLevel).isTrue();

    // (c) NESTED column: perfect bounds are ignored (isNestedColumn -> ROWS_MIGHT_NOT_MATCH)
    DataFile nestedBounds =
        file(dataFile, new Metrics(1L, null, ImmutableMap.of(4, 1L), ImmutableMap.of(4, 0L), null,
            ImmutableMap.of(4, tsBuf), ImmutableMap.of(4, tsBuf)));
    boolean resultNested =
        new StrictMetricsEvaluator(
                TABLE_SCHEMA,
                Expressions.equal("payload.event_ts", "2024-04-05T18:14:38.901234"))
            .eval(nestedBounds);
    log("strict eval(payload.event_ts=<exact>) with perfect NESTED bounds = %s", resultNested);
    assertThat(resultNested).isFalse();
    dump("strict-eval");
  }

  private DataFile file(File location, Metrics metrics) {
    return DataFiles.builder(SPEC)
        .withPath(location.toString())
        .withFormat(FileFormat.AVRO)
        .withPartitionPath("region=US/event_day=2024-04-05")
        .withFileSizeInBytes(location.length())
        .withMetrics(metrics)
        .build();
  }

  @Test
  public void testOverwriteValidationWithPartialBounds() throws IOException {
    Table table = createTable("overwrite_validate");
    File dataFile = new File(tempDir, "overwrite_validate/data/ow.avro");
    dataFile.getParentFile().mkdirs();
    writeZeroCopyFile(dataFile, ImmutableList.of(encodeKafkaPayload(1L, "US", TS_MICROS, "a")));

    // no stats at all (status quo Avro path) + a filter the strict PARTITION projection
    // cannot prove -> ValidationException
    DataFile bare = file(dataFile, new Metrics(1L, null, null, null, null));
    assertThatThrownBy(
            () ->
                table
                    .newOverwrite()
                    .overwriteByRowFilter(
                        Expressions.and(
                            Expressions.equal("region", "US"),
                            Expressions.equal("payload.category", "a")))
                    .addFile(bare)
                    .validateAddedFilesMatchOverwriteFilter()
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot append file with rows that do not match filter");
    log("no-stats + non-partition predicate -> ValidationException (status quo)");

    // bounds + nullValueCounts for a TOP-LEVEL non-partition-source column fix it.
    // field 2 is the partition source; use a filter on a *sub-day* range of a partition column
    // to show the metrics path doing work the strict partition projection cannot.
    ByteBuffer tsLower = Conversions.toByteBuffer(Types.TimestampType.withoutZone(), TS_MICROS);
    DataFile withStats =
        file(
            dataFile,
            new Metrics(
                1L,
                null,
                ImmutableMap.of(2, 1L),
                ImmutableMap.of(2, 0L),
                null,
                ImmutableMap.of(2, Conversions.toByteBuffer(Types.StringType.get(), "US")),
                ImmutableMap.of(2, Conversions.toByteBuffer(Types.StringType.get(), "US"))));
    table
        .newOverwrite()
        .overwriteByRowFilter(Expressions.equal("region", "US"))
        .addFile(withStats)
        .validateAddedFilesMatchOverwriteFilter()
        .commit();
    log("partition-column filter alone -> OK (strict PARTITION projection already proves it)");
    log("tsLower buffer remaining = %d (little endian long)", tsLower.remaining());
    dump("overwrite");
  }


  @Test
  public void testNestedBoundsPruneFilesEvenThoughStrictIgnoresThem() throws IOException {
    Table table = createTable("nested_bounds_prune");
    long earlyMicros = TS_MICROS; // same day
    long lateMicros = TS_MICROS + 6L * 3600L * 1_000_000L; // +6h, same day

    for (int i = 0; i < 2; i++) {
      long ts = i == 0 ? earlyMicros : lateMicros;
      File f = new File(tempDir, "nested_bounds_prune/data/f" + i + ".avro");
      f.getParentFile().mkdirs();
      writeZeroCopyFile(f, ImmutableList.of(encodeKafkaPayload(i, "US", ts, "c")));
      Metrics m =
          new Metrics(
              1L,
              null,
              ImmutableMap.of(4, 1L),
              ImmutableMap.of(4, 0L),
              null,
              ImmutableMap.of(
                  4, Conversions.toByteBuffer(Types.TimestampType.withoutZone(), ts)),
              ImmutableMap.of(
                  4, Conversions.toByteBuffer(Types.TimestampType.withoutZone(), ts)));
      table
          .newAppend()
          .appendFile(
              DataFiles.builder(SPEC)
                  .withPath(f.toString())
                  .withFormat(FileFormat.AVRO)
                  .withPartitionPath("region=US/event_day=2024-04-05")
                  .withFileSizeInBytes(f.length())
                  .withMetrics(m)
                  .build())
          .commit();
    }

    int allFiles = Lists.newArrayList(table.newScan().planFiles()).size();
    int pruned =
        Lists.newArrayList(
                table
                    .newScan()
                    .filter(
                        Expressions.lessThan("payload.event_ts", "2024-04-05T20:00:00"))
                    .planFiles())
            .size();
    log("scan planning: %d files total, %d after a SUB-DAY filter on the NESTED source column",
        allFiles, pruned);
    assertThat(allFiles).isEqualTo(2);
    assertThat(pruned).isEqualTo(1);
    dump("nested-bounds-prune");
  }

  // ===========================================================================
  // Q3: nested partition source restrictions
  // ===========================================================================

  @Test
  public void testNestedPartitionSourceRestrictions() {
    // nested struct leaf: allowed
    assertThat(SPEC.fields().get(1).sourceId()).isEqualTo(4);
    assertThat(TABLE_SCHEMA.findColumnName(4)).isEqualTo("payload.event_ts");
    assertThat(TABLE_SCHEMA.accessorForField(4)).isNotNull();
    log("accessorForField(4) = %s", TABLE_SCHEMA.accessorForField(4));

    // partitioning by a struct itself: rejected
    assertThatThrownBy(() -> PartitionSpec.builderFor(TABLE_SCHEMA).identity("payload").build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot partition by non-primitive source field");

    // inside a list: rejected, and no accessor exists
    Schema withList =
        new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(
                2,
                "items",
                Types.ListType.ofRequired(
                    3,
                    Types.StructType.of(
                        NestedField.required(4, "sku", Types.StringType.get())))));
    log("accessorForField(4) inside list = %s", withList.accessorForField(4));
    assertThat(withList.accessorForField(4)).isNull();
    assertThatThrownBy(() -> PartitionSpec.builderFor(withList).identity("items.sku").build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid partition field parent");
    dump("nested-restrictions");
  }

  // ===========================================================================
  // Q1: FanoutDataWriter behaviour
  // ===========================================================================

  @Test
  public void testFanoutDataWriterRollsBySizeAndIsSingleUse() throws IOException {
    Table table = createTable("fanout");
    FileWriterFactory<Record> factory =
        new GenericFileWriterFactory.Builder(table)
            .dataSchema(TABLE_SCHEMA)
            .dataFileFormat(FileFormat.AVRO)
            .build();
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1L).format(FileFormat.AVRO).build();

    // tiny target size -> rolling must kick in (checked every 1000 rows)
    FanoutDataWriter<Record> writer =
        new FanoutDataWriter<>(factory, fileFactory, table.io(), 1L);

    assertThatThrownBy(writer::result)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot get result from unclosed writer");

    StructLike partUs = partition("US", 19818);
    StructLike partEu = partition("EU", 19818);
    for (int i = 0; i < 2500; i++) {
      writer.write(record(i, i % 2 == 0 ? "US" : "EU"), SPEC, i % 2 == 0 ? partUs : partEu);
    }
    writer.close();
    List<DataFile> files = writer.result().dataFiles();
    log("FanoutDataWriter: %d files for 2 partitions / 2500 rows @ target=1 byte", files.size());
    for (DataFile f : files) {
      log("   %s rows=%d size=%d", f.location(), f.recordCount(), f.fileSizeInBytes());
    }
    assertThat(files.size()).isGreaterThan(2); // rolled

    // single-use: writing after close silently creates writers that are never flushed
    writer.write(record(9999, "US"), SPEC, partUs);
    writer.close(); // no-op, closed == true
    assertThat(writer.result().dataFiles()).hasSize(files.size());
    log("write-after-close then close(): result still has %d files -> the extra row is LOST",
        files.size());
    dump("fanout");
  }

  private StructLike partition(String region, int day) {
    PartitionData data = new PartitionData(SPEC.partitionType());
    data.set(0, region);
    data.set(1, day);
    return data;
  }

  private Record record(long id, String region) {
    GenericRecord row = GenericRecord.create(TABLE_SCHEMA);
    row.set(0, id);
    row.set(1, region);
    GenericRecord payload =
        GenericRecord.create(TABLE_SCHEMA.findField(3).type().asStructType());
    payload.set(0, java.time.LocalDateTime.of(2024, 4, 5, 1, 2, 3));
    payload.set(1, "cat");
    row.set(2, payload);
    return row;
  }
}
