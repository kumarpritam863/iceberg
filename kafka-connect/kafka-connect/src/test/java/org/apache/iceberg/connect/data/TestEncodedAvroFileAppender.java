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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

/**
 * Proves the core claim of the zero-copy write path: Avro bytes produced by a Kafka producer can be
 * appended into an Iceberg data file unchanged and read back as correct table data.
 *
 * <p>See {@code kafka-connect/ZERO_COPY_AVRO_DESIGN.md}.
 */
public class TestEncodedAvroFileAppender {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Schema TABLE_SCHEMA =
      new Schema(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.required(2, "name", Types.StringType.get()),
          NestedField.optional(3, "note", Types.StringType.get()),
          NestedField.required(4, "count", Types.IntegerType.get()),
          NestedField.required(5, "ratio", Types.DoubleType.get()),
          NestedField.required(6, "active", Types.BooleanType.get()),
          NestedField.required(7, "event_ts", Types.TimestampType.withZone()),
          NestedField.required(8, "day", Types.DateType.get()),
          NestedField.required(9, "tags", Types.ListType.ofRequired(20, Types.StringType.get())),
          NestedField.required(
              10,
              "attrs",
              Types.MapType.ofRequired(21, 22, Types.StringType.get(), Types.StringType.get())),
          NestedField.required(
              11,
              "payload",
              Types.StructType.of(
                  NestedField.required(23, "inner_id", Types.StringType.get()),
                  NestedField.optional(24, "inner_count", Types.IntegerType.get()))));

  private static final String TABLE_SCHEMA_JSON =
      org.apache.iceberg.SchemaParser.toJson(TABLE_SCHEMA);

  /**
   * The schema a Kafka producer would use: identical structure and identical binary layout to the
   * table's canonical Avro schema, but with no Iceberg id properties — which is the shape a
   * schema-registry writer schema actually arrives in.
   */
  private static org.apache.avro.Schema producerSchema() {
    try {
      JsonNode tree = MAPPER.readTree(AvroSchemaUtil.convert(TABLE_SCHEMA, "Event").toString());
      stripIds(tree);
      return new org.apache.avro.Schema.Parser().parse(MAPPER.writeValueAsString(tree));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void stripIds(JsonNode node) {
    if (node instanceof ObjectNode) {
      ObjectNode object = (ObjectNode) node;
      object.remove(Lists.newArrayList("field-id", "element-id", "key-id", "value-id"));
      object.forEach(TestEncodedAvroFileAppender::stripIds);
    } else if (node.isArray()) {
      node.forEach(TestEncodedAvroFileAppender::stripIds);
    }
  }

  @Test
  public void testRawBytesRoundTripThroughIceberg() throws IOException {
    org.apache.avro.Schema writerSchema = producerSchema();
    org.apache.avro.Schema fileSchema = AvroSchemaAnnotator.annotate(writerSchema, TABLE_SCHEMA);

    List<GenericData.Record> source = Lists.newArrayList();
    for (int i = 0; i < 25; i++) {
      source.add(newProducerRecord(writerSchema, i));
    }

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<RawAvroPayload> appender =
        new EncodedAvroFileAppender(
            fileSchema,
            "Event",
            1,
            TABLE_SCHEMA_JSON,
            out,
            org.apache.avro.file.CodecFactory.nullCodec(),
            ImmutableMap.of(),
            true)) {
      for (GenericData.Record record : source) {
        // The bytes a producer put on the topic, handed through without decoding.
        appender.add(
            new RawAvroPayload(
                ByteBuffer.wrap(encode(record, writerSchema)), writerSchema, "Event", 1));
      }
    }

    List<Record> readBack = read(out, TABLE_SCHEMA);

    assertThat(readBack).hasSameSizeAs(source);
    for (int i = 0; i < source.size(); i++) {
      GenericData.Record expected = source.get(i);
      Record actual = readBack.get(i);
      assertThat(actual.getField("id")).isEqualTo(expected.get("id"));
      assertThat(actual.getField("name")).hasToString(expected.get("name").toString());
      assertThat(actual.getField("count")).isEqualTo(expected.get("count"));
      assertThat(actual.getField("ratio")).isEqualTo(expected.get("ratio"));
      assertThat(actual.getField("active")).isEqualTo(expected.get("active"));
      // Half the records carry a null in the optional field, exercising both union branches.
      if (expected.get("note") == null) {
        assertThat(actual.getField("note")).isNull();
      } else {
        assertThat(actual.getField("note")).hasToString(expected.get("note").toString());
      }
      assertThat((List<?>) actual.getField("tags")).hasSize(3);
      assertThat((java.util.Map<?, ?>) actual.getField("attrs")).hasSize(2);
      assertThat(actual.getField("payload")).isNotNull();
    }
  }

  @Test
  public void testPayloadFromADifferentSchemaVersionIsRejected() throws IOException {
    org.apache.avro.Schema writerSchema = producerSchema();
    org.apache.avro.Schema fileSchema = AvroSchemaAnnotator.annotate(writerSchema, TABLE_SCHEMA);

    try (FileAppender<RawAvroPayload> appender =
        new EncodedAvroFileAppender(
            fileSchema,
            "Event",
            1,
            TABLE_SCHEMA_JSON,
            new InMemoryOutputFile(),
            org.apache.avro.file.CodecFactory.nullCodec(),
            ImmutableMap.of(),
            true)) {
      byte[] encoded = encode(newProducerRecord(writerSchema, 1), writerSchema);

      // Version 2 of the same schema is a different layout by definition; mixing it into a file
      // whose
      // header describes v1 would be silent corruption.
      assertThatThrownBy(
              () ->
                  appender.add(
                      new RawAvroPayload(ByteBuffer.wrap(encoded), writerSchema, "Event", 2)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Event v2 does not match this file's Event v1");

      assertThatThrownBy(
              () ->
                  appender.add(
                      new RawAvroPayload(ByteBuffer.wrap(encoded), writerSchema, "OtherEvent", 1)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("OtherEvent v1 does not match this file's Event v1");
    }
  }

  @Test
  public void testEmptyPayloadIsRejected() throws IOException {
    org.apache.avro.Schema writerSchema = producerSchema();
    org.apache.avro.Schema fileSchema = AvroSchemaAnnotator.annotate(writerSchema, TABLE_SCHEMA);

    try (FileAppender<RawAvroPayload> appender =
        new EncodedAvroFileAppender(
            fileSchema,
            "Event",
            1,
            TABLE_SCHEMA_JSON,
            new InMemoryOutputFile(),
            org.apache.avro.file.CodecFactory.nullCodec(),
            ImmutableMap.of(),
            true)) {
      assertThatThrownBy(
              () ->
                  appender.add(
                      new RawAvroPayload(ByteBuffer.allocate(0), writerSchema, "Event", 1)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("zero-length payload");
    }
  }

  @Test
  public void testMetricsCarryRecordCountOnly() throws IOException {
    org.apache.avro.Schema writerSchema = producerSchema();
    org.apache.avro.Schema fileSchema = AvroSchemaAnnotator.annotate(writerSchema, TABLE_SCHEMA);

    FileAppender<RawAvroPayload> appender =
        new EncodedAvroFileAppender(
            fileSchema,
            "Event",
            1,
            TABLE_SCHEMA_JSON,
            new InMemoryOutputFile(),
            org.apache.avro.file.CodecFactory.nullCodec(),
            ImmutableMap.of(),
            true);

    assertThatThrownBy(appender::metrics)
        .as("metrics are only valid after close")
        .isInstanceOf(IllegalStateException.class);

    for (int i = 0; i < 7; i++) {
      appender.add(
          new RawAvroPayload(
              ByteBuffer.wrap(encode(newProducerRecord(writerSchema, i), writerSchema)),
              writerSchema,
              "Event",
              1));
    }
    assertThat(appender.length())
        .as("length must work before close, for roll decisions")
        .isPositive();
    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(metrics.recordCount()).isEqualTo(7L);
    // Matching Iceberg's own Avro appender, which also emits no column statistics.
    assertThat(metrics.columnSizes()).isNull();
    assertThat(metrics.valueCounts()).isNull();
    assertThat(metrics.nullValueCounts()).isNull();
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
    assertThat(appender.splitOffsets()).isNull();
  }

  @Test
  public void testCompressedRoundTrip() throws IOException {
    org.apache.avro.Schema writerSchema = producerSchema();
    org.apache.avro.Schema fileSchema = AvroSchemaAnnotator.annotate(writerSchema, TABLE_SCHEMA);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<RawAvroPayload> appender =
        new EncodedAvroFileAppender(
            fileSchema,
            "Event",
            1,
            TABLE_SCHEMA_JSON,
            out,
            org.apache.avro.file.CodecFactory.deflateCodec(6),
            ImmutableMap.of(),
            true)) {
      for (int i = 0; i < 40; i++) {
        appender.add(
            new RawAvroPayload(
                ByteBuffer.wrap(encode(newProducerRecord(writerSchema, i), writerSchema)),
                writerSchema,
                "Event",
                1));
      }
    }

    // Compression is applied per block at flush time, independently of how datums entered the
    // buffer.
    assertThat(read(out, TABLE_SCHEMA)).hasSize(40);
  }

  @Test
  public void testProjectionOfASubsetOfColumns() throws IOException {
    org.apache.avro.Schema writerSchema = producerSchema();
    org.apache.avro.Schema fileSchema = AvroSchemaAnnotator.annotate(writerSchema, TABLE_SCHEMA);

    OutputFile out = new InMemoryOutputFile();
    try (FileAppender<RawAvroPayload> appender =
        new EncodedAvroFileAppender(
            fileSchema,
            "Event",
            1,
            TABLE_SCHEMA_JSON,
            out,
            org.apache.avro.file.CodecFactory.nullCodec(),
            ImmutableMap.of(),
            true)) {
      appender.add(
          new RawAvroPayload(
              ByteBuffer.wrap(encode(newProducerRecord(writerSchema, 5), writerSchema)),
              writerSchema,
              "Event",
              1));
    }

    // Reading two columns must skip the rest correctly -- this is the read-side counterpart of the
    // skip machinery the partition extractor relies on.
    Schema projection =
        new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(4, "count", Types.IntegerType.get()));
    List<Record> projected = read(out, projection);

    assertThat(projected).hasSize(1);
    assertThat(projected.get(0).getField("id")).isEqualTo(5L);
    assertThat(projected.get(0).getField("count")).isEqualTo(5);
  }

  // ------------------------------------------------------------- helpers

  private static GenericData.Record newProducerRecord(org.apache.avro.Schema schema, int seed) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("id", (long) seed);
    record.put("name", "name-" + seed);
    record.put("note", seed % 2 == 0 ? null : "note-" + seed);
    record.put("count", seed);
    record.put("ratio", seed / 4.0d);
    record.put("active", seed % 3 == 0);
    record.put("event_ts", 1_700_000_000_000_000L + seed);
    record.put("day", 19_000 + seed);
    record.put("tags", Lists.newArrayList("a" + seed, "b" + seed, "c" + seed));
    record.put("attrs", ImmutableMap.of("k1", "v" + seed, "k2", "w" + seed));

    org.apache.avro.Schema payloadSchema = schema.getField("payload").schema();
    GenericData.Record payload = new GenericData.Record(payloadSchema);
    payload.put("inner_id", "inner-" + seed);
    payload.put("inner_count", seed % 4 == 0 ? null : seed);
    record.put("payload", payload);

    return record;
  }

  private static byte[] encode(GenericData.Record record, org.apache.avro.Schema schema)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericData.Record>(schema).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  private static List<Record> read(OutputFile file, Schema projection) throws IOException {
    try (AvroIterable<Record> reader =
        Avro.read(file.toInputFile())
            .project(projection)
            .createResolvingReader(PlannedDataReader::create)
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}
