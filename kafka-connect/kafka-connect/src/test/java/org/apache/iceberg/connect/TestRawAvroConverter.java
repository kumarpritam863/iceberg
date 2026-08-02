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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

public class TestRawAvroConverter {

  private static final String SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"com.example\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"long\"},"
          + "{\"name\":\"name\",\"type\":\"string\"}]}";

  private static RawAvroConverter converter(String... extra) {
    RawAvroConverter converter = new RawAvroConverter();
    ImmutableMap.Builder<String, String> props = ImmutableMap.builder();
    props.put(RawAvroConverter.SCHEMA_CONFIG, SCHEMA_JSON);
    for (int i = 0; i < extra.length; i += 2) {
      props.put(extra[i], extra[i + 1]);
    }
    converter.configure(props.build(), false);
    return converter;
  }

  private static String header(RecordHeaders headers, String key) {
    return new String(headers.lastHeader(key).value(), StandardCharsets.UTF_8);
  }

  @Test
  public void testPassesBytesThroughAndDescribesThemInHeaders() throws IOException {
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(SCHEMA_JSON);
    GenericData.Record record = new GenericData.Record(schema);
    record.put("id", 42L);
    record.put("name", "hello");
    byte[] encoded = encode(record, schema);

    RecordHeaders headers = new RecordHeaders();
    SchemaAndValue result = converter().toConnectData("topic", headers, encoded);

    // Zero-copy: the value must be a view over the Kafka array, not a duplicate of it.
    assertThat(result.value()).isInstanceOf(ByteBuffer.class);
    ByteBuffer buffer = (ByteBuffer) result.value();
    assertThat(buffer.hasArray()).isTrue();
    assertThat(buffer.array()).isSameAs(encoded);

    // No custom type crosses the boundary -- only bytes and strings.
    assertThat(header(headers, "iceberg.avro.schema.name")).isEqualTo("com.example.Event");
    assertThat(header(headers, "iceberg.avro.schema.version")).isEqualTo("1");
    assertThat(new org.apache.avro.Schema.Parser().parse(header(headers, "iceberg.avro.schema")))
        .isEqualTo(schema);
  }

  @Test
  public void testHeaderBytesAreReusedAcrossRecords() {
    RawAvroConverter converter = converter();

    RecordHeaders first = new RecordHeaders();
    RecordHeaders second = new RecordHeaders();
    converter.toConnectData("topic", first, new byte[] {1});
    converter.toConnectData("topic", second, new byte[] {2});

    // The schema JSON is encoded once at configure() time, so shipping it per record costs a
    // reference, not an allocation.
    assertThat(first.lastHeader("iceberg.avro.schema").value())
        .isSameAs(second.lastHeader("iceberg.avro.schema").value());
  }

  @Test
  public void testSchemaNameAndVersionCanBeOverridden() {
    RawAvroConverter converter =
        converter(
            RawAvroConverter.SCHEMA_NAME_CONFIG, "MyEvent",
            RawAvroConverter.SCHEMA_VERSION_CONFIG, "7");

    RecordHeaders headers = new RecordHeaders();
    converter.toConnectData("topic", headers, new byte[] {1, 2, 3});

    assertThat(header(headers, "iceberg.avro.schema.name")).isEqualTo("MyEvent");
    assertThat(header(headers, "iceberg.avro.schema.version")).isEqualTo("7");
  }

  @Test
  public void testTombstonePassesThroughAsNull() {
    assertThat(converter().toConnectData("topic", new RecordHeaders(), null).value()).isNull();
  }

  @Test
  public void testNonNumericVersionFailsAtStartupNotPerRecord() {
    assertThatThrownBy(() -> converter(RawAvroConverter.SCHEMA_VERSION_CONFIG, "not-a-number"))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void testMissingSchemaConfigFailsWithAnActionableMessage() {
    assertThatThrownBy(() -> new RawAvroConverter().configure(ImmutableMap.of(), false))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("value.converter." + RawAvroConverter.SCHEMA_CONFIG);
  }

  @Test
  public void testKeyConverterUseIsRejected() {
    assertThatThrownBy(
            () ->
                new RawAvroConverter()
                    .configure(ImmutableMap.of(RawAvroConverter.SCHEMA_CONFIG, SCHEMA_JSON), true))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("value converter");
  }

  @Test
  public void testNonRecordSchemaIsRejected() {
    assertThatThrownBy(
            () ->
                new RawAvroConverter()
                    .configure(
                        ImmutableMap.of(RawAvroConverter.SCHEMA_CONFIG, "\"string\""), false))
        .isInstanceOf(DataException.class)
        .hasMessageContaining("must be a record schema");
  }

  @Test
  public void testHeaderlessConversionIsRejected() {
    // The schema has to travel somewhere; without headers there is nowhere to put it.
    assertThatThrownBy(() -> converter().toConnectData("topic", new byte[] {1}))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("headers");
  }

  @Test
  public void testFromConnectDataIsUnsupported() {
    assertThatThrownBy(() -> converter().fromConnectData("topic", null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private static byte[] encode(GenericData.Record record, org.apache.avro.Schema schema)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericData.Record>(schema).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }
}
