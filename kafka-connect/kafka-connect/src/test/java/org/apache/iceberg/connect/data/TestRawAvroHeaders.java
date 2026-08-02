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

import java.nio.charset.StandardCharsets;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 * The header contract is the whole converter-to-sink interface, so its failure modes need to
 * produce messages someone can act on.
 */
public class TestRawAvroHeaders {

  private static final String SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"com.example\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"long\"}]}";

  private static SinkRecord record(Object name, Object version, Object schema) {
    SinkRecord record =
        new SinkRecord("topic", 0, null, null, Schema.OPTIONAL_BYTES_SCHEMA, new byte[] {1}, 5L);
    if (name != null) {
      record.headers().add(RawAvroHeaders.SCHEMA_NAME, name, null);
    }
    if (version != null) {
      record.headers().add(RawAvroHeaders.SCHEMA_VERSION, version, null);
    }
    if (schema != null) {
      record.headers().add(RawAvroHeaders.SCHEMA, schema, null);
    }
    return record;
  }

  @Test
  public void testReadsCoordinatesFromStringHeaders() {
    RawAvroHeaders.Coordinates coordinates =
        new RawAvroHeaders().read(record("com.example.Event", "7", SCHEMA_JSON));

    assertThat(coordinates.schemaName()).isEqualTo("com.example.Event");
    assertThat(coordinates.schemaVersion()).isEqualTo(7);
    assertThat(coordinates.writerSchema().getFullName()).isEqualTo("com.example.Event");
  }

  @Test
  public void testAcceptsByteHeadersToo() {
    // ByteArrayConverter leaves header values as raw bytes rather than strings.
    RawAvroHeaders.Coordinates coordinates =
        new RawAvroHeaders()
            .read(
                record(
                    "com.example.Event".getBytes(StandardCharsets.UTF_8),
                    "7".getBytes(StandardCharsets.UTF_8),
                    SCHEMA_JSON.getBytes(StandardCharsets.UTF_8)));

    assertThat(coordinates.schemaName()).isEqualTo("com.example.Event");
    assertThat(coordinates.schemaVersion()).isEqualTo(7);
  }

  @Test
  public void testSchemaIsParsedOncePerVersion() {
    RawAvroHeaders headers = new RawAvroHeaders();

    org.apache.avro.Schema first =
        headers.read(record("com.example.Event", "1", SCHEMA_JSON)).writerSchema();
    org.apache.avro.Schema second =
        headers.read(record("com.example.Event", "1", SCHEMA_JSON)).writerSchema();

    // Same instance, so the JSON was not re-parsed -- that is the point of shipping it per record
    // being cheap.
    assertThat(second).isSameAs(first);
  }

  @Test
  public void testCacheIsScopedPerInstanceNotGlobal() {
    // A global cache keyed on name and version would hand back the first schema here, silently
    // writing records under the wrong schema.
    String other =
        "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"com.example\",\"fields\":["
            + "{\"name\":\"different\",\"type\":\"string\"}]}";

    new RawAvroHeaders().read(record("com.example.Event", "1", SCHEMA_JSON));
    RawAvroHeaders.Coordinates fresh =
        new RawAvroHeaders().read(record("com.example.Event", "1", other));

    assertThat(fresh.writerSchema().getFields()).hasSize(1);
    assertThat(fresh.writerSchema().getFields().get(0).name()).isEqualTo("different");
  }

  @Test
  public void testPresentDistinguishesRawRecords() {
    assertThat(RawAvroHeaders.present(record("com.example.Event", "1", SCHEMA_JSON))).isTrue();
    assertThat(RawAvroHeaders.present(record(null, null, null))).isFalse();
  }

  @Test
  public void testMissingHeadersNameTheContract() {
    assertThatThrownBy(() -> new RawAvroHeaders().read(record(null, "1", SCHEMA_JSON)))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining(RawAvroHeaders.SCHEMA_NAME)
        .hasMessageContaining("offset 5");

    assertThatThrownBy(() -> new RawAvroHeaders().read(record("com.example.Event", "1", null)))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining(RawAvroHeaders.SCHEMA);
  }

  @Test
  public void testMangledSchemaHeaderPointsAtHeaderConverter() {
    // The trap: the default SimpleHeaderConverter runs Values.parseString over header values, which
    // turns an Avro schema into a Map. The error has to say so, or this costs someone an afternoon.
    SinkRecord mangled = record("com.example.Event", "1", ImmutableMap.of("type", "record"));

    assertThatThrownBy(() -> new RawAvroHeaders().read(mangled))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("not a string")
        .hasMessageContaining("StringConverter");
  }

  @Test
  public void testNonNumericVersionIsRejected() {
    assertThatThrownBy(
            () -> new RawAvroHeaders().read(record("com.example.Event", "v1", SCHEMA_JSON)))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining(RawAvroHeaders.SCHEMA_VERSION);
  }

  @Test
  public void testUnparseableAndNonRecordSchemasAreRejected() {
    assertThatThrownBy(() -> new RawAvroHeaders().read(record("E", "1", "{not json")))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("not a parseable Avro schema");

    assertThatThrownBy(() -> new RawAvroHeaders().read(record("E", "1", "\"string\"")))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("must be a record schema");
  }

  @Test
  public void testParseSkipsTheCacheForOneOffUse() {
    // Used by table auto-creation, where caching would be pointless and the record is a sample.
    RawAvroHeaders.Coordinates first =
        RawAvroHeaders.parse(record("com.example.Event", "1", SCHEMA_JSON));
    RawAvroHeaders.Coordinates second =
        RawAvroHeaders.parse(record("com.example.Event", "1", SCHEMA_JSON));

    assertThat(first.writerSchema()).isEqualTo(second.writerSchema());
    assertThat(first.writerSchema()).isNotSameAs(second.writerSchema());
  }
}
