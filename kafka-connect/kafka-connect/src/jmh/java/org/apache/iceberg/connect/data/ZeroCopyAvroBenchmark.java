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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Phase 0 of the zero-copy Avro passthrough design (see {@code
 * kafka-connect/ZERO_COPY_AVRO_DESIGN.md}).
 *
 * <p>Measures today's four-stage sink path against the proposed raw-bytes path, per record:
 *
 * <pre>
 *   today:    Kafka bytes --(1)--> GenericRecord --(2)--> Connect Struct
 *                         --(3)--> Iceberg Record --(4)--> Avro data file
 *   proposed: Kafka bytes ------------ memcpy ------------> Avro data file
 * </pre>
 *
 * <p>Earlier hand-rolled harnesses put the end-to-end win at 9-11x but measured stages 3 and 4 with
 * stand-ins. This benchmark pins them with the real classes: stage 3 is the actual {@link
 * RecordConverter} (which needs a live {@link Table}, hence this benchmark living in this package)
 * and stage 4 is the actual Iceberg {@link FileAppender}.
 *
 * <p>Stage 2 remains a stand-in: the production converter is Apple's {@code AvroData} in a separate
 * repository. The stand-in here is a hand-written per-field copy for this benchmark's schema only,
 * so it is a <em>lower bound</em> — Confluent's general-purpose {@code AvroData} was independently
 * measured at ~2304 ns/record on the same record shape, and Apple's is more general still.
 *
 * <p>To run:
 *
 * <pre>{@code
 * JAVA_HOME=$(/usr/libexec/java_home -v 17) ./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:jmh \
 *     -PjmhIncludeRegex=ZeroCopyAvroBenchmark \
 *     -DsparkVersions= -DflinkVersions=
 * }</pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(1)
public class ZeroCopyAvroBenchmark {

  /** Distinct records, so the benchmark is not measuring one cache-hot buffer. */
  private static final int POOL_SIZE = 1024;

  private static final int STRING_FIELDS = 12;
  private static final int LONG_FIELDS = 6;
  private static final int INT_FIELDS = 6;
  private static final int OPTIONAL_STRING_FIELDS = 10;
  private static final int TAGS_PER_RECORD = 5;
  private static final int ATTRS_PER_RECORD = 4;

  private Schema connectSchema;
  private org.apache.iceberg.Schema icebergSchema;
  private org.apache.avro.Schema avroSchema;

  private Table table;
  private RecordConverter recordConverter;

  // Pools, all POOL_SIZE long and index-aligned.
  private byte[][] encodedPool;
  private GenericData.Record[] avroPool;
  private Struct[] structPool;
  private Record[] icebergPool;

  private int cursor = 0;

  private GenericDatumReader<GenericData.Record> datumReader;
  private BinaryDecoder decoder;
  private byte[] memcpyTarget;

  // Live appenders, recreated per iteration so files do not grow without bound.
  private FileAppender<Record> icebergAppender;
  private DataFileWriter<Object> rawWriter;

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    this.connectSchema = buildConnectSchema();

    IcebergSinkConfig config =
        new IcebergSinkConfig(
            ImmutableMap.of(
                "iceberg.catalog.type", "hadoop",
                "iceberg.tables", "db.events",
                "name", "zero-copy-bench"));

    this.icebergSchema =
        new org.apache.iceberg.Schema(
            SchemaUtils.toIcebergType(connectSchema, config).asStructType().fields());
    // The matching Avro schema, field-ids included. In production this is the Kafka writer schema
    // annotated with the table's field ids; here we derive it so the two are exact by construction.
    this.avroSchema = AvroSchemaUtil.convert(icebergSchema, "event");

    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("bench", ImmutableMap.of());
    catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("db"));
    this.table =
        catalog.createTable(
            TableIdentifier.of("db", "events"), icebergSchema, PartitionSpec.unpartitioned());

    this.recordConverter = new RecordConverter(table, config);

    this.datumReader = new GenericDatumReader<>(avroSchema);

    buildPools();

    long totalBytes = 0;
    for (byte[] bytes : encodedPool) {
      totalBytes += bytes.length;
    }
    this.memcpyTarget = new byte[(int) (totalBytes / POOL_SIZE) * 2];
    System.out.printf(
        "%n  schema: %d top-level fields, avg encoded payload %d bytes, pool %d records%n",
        icebergSchema.columns().size(), totalBytes / POOL_SIZE, POOL_SIZE);
  }

  private void buildPools() throws IOException {
    this.avroPool = new GenericData.Record[POOL_SIZE];
    this.structPool = new Struct[POOL_SIZE];
    this.icebergPool = new Record[POOL_SIZE];
    this.encodedPool = new byte[POOL_SIZE][];

    DatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(avroSchema);
    for (int i = 0; i < POOL_SIZE; i++) {
      GenericData.Record avroRecord = newAvroRecord(i);
      avroPool[i] = avroRecord;

      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(1024);
      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      writer.write(avroRecord, encoder);
      encoder.flush();
      encodedPool[i] = out.toByteArray();

      structPool[i] = toConnectStruct(avroRecord);
      icebergPool[i] = recordConverter.convert(structPool[i]);
    }
  }

  @Setup(Level.Iteration)
  public void setupIteration() throws IOException {
    // Both paths write to a discarding sink and use no compression, so the comparison is
    // apples-to-apples and neither accumulates gigabytes across millions of invocations.
    // (Iceberg's Avro default is gzip; leaving it on would have measured deflate, not the writer.)
    this.icebergAppender =
        Avro.write(new DiscardingOutputFile())
            .schema(icebergSchema)
            .named("event")
            .createWriterFunc(DataWriter::create)
            .set(TableProperties.AVRO_COMPRESSION, "uncompressed")
            .overwrite()
            .build();

    // The proposed path: a DataFileWriter fed only through appendEncoded. The DatumWriter is never
    // invoked (DataFileWriter.init calls setSchema on it and nothing else), so a no-op suffices.
    this.rawWriter = new DataFileWriter<>(new NoOpDatumWriter());
    rawWriter.setCodec(CodecFactory.nullCodec());
    rawWriter.create(avroSchema, new DiscardingOutputFile().createOrOverwrite());
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration() throws IOException {
    icebergAppender.close();
    rawWriter.close();
  }

  private int next() {
    int idx = cursor;
    cursor = (cursor + 1) & (POOL_SIZE - 1);
    return idx;
  }

  // ---------------------------------------------------------------- stages

  /** Stage 1: Kafka bytes to Avro GenericRecord. Needed today; deleted by raw mode. */
  @Benchmark
  public void stage1_decodeToGenericRecord(Blackhole bh) throws IOException {
    byte[] bytes = encodedPool[next()];
    decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
    bh.consume(datumReader.read(null, decoder));
  }

  /**
   * Stage 2 (stand-in, LOWER BOUND): Avro GenericRecord to Connect Struct. The production
   * implementation is Apple's general-purpose {@code AvroData}; Confluent's equivalent measured
   * ~2304 ns on this record shape. Treat this number as a floor, not an estimate.
   */
  @Benchmark
  public void stage2_genericRecordToStructLowerBound(Blackhole bh) {
    bh.consume(toConnectStruct(avroPool[next()]));
  }

  /** Stage 3: Connect Struct to Iceberg Record, via the REAL {@link RecordConverter}. */
  @Benchmark
  public void stage3_structToIcebergRecord(Blackhole bh) {
    bh.consume(recordConverter.convert(structPool[next()]));
  }

  /** Stage 4: Iceberg Record to Avro data file, via the REAL Iceberg {@link FileAppender}. */
  @Benchmark
  public void stage4_appendIcebergRecord() {
    icebergAppender.add(icebergPool[next()]);
  }

  // ------------------------------------------------------------ end to end

  /** Today's full path: decode, to Struct, to Record, append. */
  @Benchmark
  public void today_fourStagePipeline() throws IOException {
    byte[] bytes = encodedPool[next()];
    decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
    GenericData.Record avroRecord = datumReader.read(null, decoder);
    Struct struct = toConnectStruct(avroRecord);
    icebergAppender.add(recordConverter.convert(struct));
  }

  /** The proposed path: the payload bytes go straight into the data file. */
  @Benchmark
  public void proposed_appendEncoded() throws IOException {
    byte[] bytes = encodedPool[next()];
    rawWriter.appendEncoded(ByteBuffer.wrap(bytes));
  }

  /** Floor: what an unavoidable copy of the payload costs on its own. */
  @Benchmark
  public void baseline_memcpyOnly(Blackhole bh) {
    byte[] bytes = encodedPool[next()];
    System.arraycopy(bytes, 0, memcpyTarget, 0, bytes.length);
    bh.consume(memcpyTarget);
  }

  // ------------------------------------------------------------- fixtures

  private static Schema buildConnectSchema() {
    SchemaBuilder builder = SchemaBuilder.struct().name("event");
    builder.field("event_id", Schema.STRING_SCHEMA);
    builder.field("event_ts", Timestamp.SCHEMA);
    for (int i = 0; i < STRING_FIELDS; i++) {
      builder.field("s" + i, Schema.STRING_SCHEMA);
    }
    for (int i = 0; i < LONG_FIELDS; i++) {
      builder.field("l" + i, Schema.INT64_SCHEMA);
    }
    for (int i = 0; i < INT_FIELDS; i++) {
      builder.field("i" + i, Schema.INT32_SCHEMA);
    }
    for (int i = 0; i < OPTIONAL_STRING_FIELDS; i++) {
      builder.field("os" + i, Schema.OPTIONAL_STRING_SCHEMA);
    }
    builder.field("ratio", Schema.FLOAT64_SCHEMA);
    builder.field("flag", Schema.BOOLEAN_SCHEMA);
    builder.field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build());
    builder.field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build());
    builder.field(
        "payload",
        SchemaBuilder.struct()
            .name("payload")
            .field("inner_id", Schema.STRING_SCHEMA)
            .field("inner_count", Schema.INT32_SCHEMA)
            .field("inner_ratio", Schema.FLOAT64_SCHEMA)
            .build());
    return builder.build();
  }

  private GenericData.Record newAvroRecord(int seed) {
    GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("event_id", "evt-" + seed + "-0123456789abcdef");
    // Connect Timestamp maps to an Iceberg timestamp, encoded as micros.
    record.put("event_ts", 1_700_000_000_000_000L + seed * 1_000_000L);
    for (int i = 0; i < STRING_FIELDS; i++) {
      record.put("s" + i, "value-" + i + "-" + seed);
    }
    for (int i = 0; i < LONG_FIELDS; i++) {
      record.put("l" + i, (long) seed * (i + 1));
    }
    for (int i = 0; i < INT_FIELDS; i++) {
      record.put("i" + i, seed + i);
    }
    for (int i = 0; i < OPTIONAL_STRING_FIELDS; i++) {
      // Half null, so the union skip/read path sees both branches.
      record.put("os" + i, (i % 2 == 0) ? null : "opt-" + i + "-" + seed);
    }
    record.put("ratio", seed / 7.0d);
    record.put("flag", seed % 2 == 0);

    List<String> tags = Lists.newArrayListWithCapacity(TAGS_PER_RECORD);
    for (int i = 0; i < TAGS_PER_RECORD; i++) {
      tags.add("tag-" + i + "-" + seed);
    }
    record.put("tags", tags);

    Map<String, String> attrs = Maps.newHashMapWithExpectedSize(ATTRS_PER_RECORD);
    for (int i = 0; i < ATTRS_PER_RECORD; i++) {
      attrs.put("k" + i, "v" + i + "-" + seed);
    }
    record.put("attrs", attrs);

    org.apache.avro.Schema payloadSchema = avroSchema.getField("payload").schema();
    GenericData.Record payload = new GenericData.Record(payloadSchema);
    payload.put("inner_id", "inner-" + seed);
    payload.put("inner_count", seed);
    payload.put("inner_ratio", seed / 3.0d);
    record.put("payload", payload);

    return record;
  }

  /**
   * Stand-in for Apple's {@code AvroData}: a hand-written per-field copy specialised to this
   * benchmark's schema. Deliberately the cheapest possible implementation, so stage 2 is reported
   * as a lower bound rather than a realistic cost.
   */
  // Connect's Timestamp logical type mandates java.util.Date as its runtime representation, so
  // error-prone's JavaUtilDate check cannot be honoured here.
  @SuppressWarnings("JavaUtilDate")
  private Struct toConnectStruct(GenericData.Record avroRecord) {
    Struct struct = new Struct(connectSchema);
    struct.put("event_id", avroRecord.get("event_id").toString());
    struct.put("event_ts", new java.util.Date(((Long) avroRecord.get("event_ts")) / 1000L));
    for (int i = 0; i < STRING_FIELDS; i++) {
      struct.put("s" + i, avroRecord.get("s" + i).toString());
    }
    for (int i = 0; i < LONG_FIELDS; i++) {
      struct.put("l" + i, avroRecord.get("l" + i));
    }
    for (int i = 0; i < INT_FIELDS; i++) {
      struct.put("i" + i, avroRecord.get("i" + i));
    }
    for (int i = 0; i < OPTIONAL_STRING_FIELDS; i++) {
      Object value = avroRecord.get("os" + i);
      struct.put("os" + i, value == null ? null : value.toString());
    }
    struct.put("ratio", avroRecord.get("ratio"));
    struct.put("flag", avroRecord.get("flag"));

    @SuppressWarnings("unchecked")
    List<Object> tags = (List<Object>) avroRecord.get("tags");
    List<String> connectTags = Lists.newArrayListWithCapacity(tags.size());
    for (Object tag : tags) {
      connectTags.add(tag.toString());
    }
    struct.put("tags", connectTags);

    @SuppressWarnings("unchecked")
    Map<Object, Object> attrs = (Map<Object, Object>) avroRecord.get("attrs");
    Map<String, String> connectAttrs = Maps.newHashMapWithExpectedSize(attrs.size());
    for (Map.Entry<Object, Object> entry : attrs.entrySet()) {
      connectAttrs.put(entry.getKey().toString(), entry.getValue().toString());
    }
    struct.put("attrs", connectAttrs);

    GenericData.Record payload = (GenericData.Record) avroRecord.get("payload");
    Struct payloadStruct = new Struct(connectSchema.field("payload").schema());
    payloadStruct.put("inner_id", payload.get("inner_id").toString());
    payloadStruct.put("inner_count", payload.get("inner_count"));
    payloadStruct.put("inner_ratio", payload.get("inner_ratio"));
    struct.put("payload", payloadStruct);

    return struct;
  }

  /**
   * {@link DataFileWriter} requires a {@link DatumWriter} but never calls {@code write} unless
   * {@code append} is used; {@code appendEncoded} bypasses it entirely.
   */
  private static class NoOpDatumWriter implements DatumWriter<Object> {
    @Override
    public void setSchema(org.apache.avro.Schema schema) {}

    @Override
    public void write(Object datum, Encoder out) {
      throw new UnsupportedOperationException("appendEncoded only");
    }
  }

  /**
   * An {@link OutputFile} that discards everything while tracking position. Both write paths need a
   * sink that survives millions of appends; an in-memory buffer would hit the 2 GB array limit, and
   * a real file would measure the filesystem. Discarding keeps the measurement on the writer.
   */
  private static class DiscardingOutputFile implements OutputFile {
    @Override
    public PositionOutputStream create() {
      return new DiscardingPositionOutputStream();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return create();
    }

    @Override
    public String location() {
      return "discarding://bench";
    }

    @Override
    public InputFile toInputFile() {
      throw new UnsupportedOperationException("write-only");
    }
  }

  private static class DiscardingPositionOutputStream extends PositionOutputStream {
    private long position = 0;

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void write(int b) {
      position += 1;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      position += len;
    }
  }
}
