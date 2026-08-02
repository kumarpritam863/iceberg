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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * A minimal value converter for the zero-copy Avro write path: it hands the record's bytes to the
 * sink undecoded, and describes them with three record headers.
 *
 * <p>This is a <b>single-schema</b> converter — the Avro writer schema comes from connector config,
 * not from a registry — which makes it useful for testing the raw path end to end and for topics
 * that only ever carry one schema version. A production converter resolves the schema per record
 * (from a registry id in a header, or a magic-byte prefix) and then sets the same three headers.
 *
 * <p>Config:
 *
 * <pre>
 *   value.converter                  org.apache.iceberg.connect.RawAvroConverter
 *   value.converter.schema           &lt;the Avro writer schema, as JSON&gt;
 *   value.converter.schema.name      identity for the schema, default: the Avro record full name
 *   value.converter.schema.version   integer, default 1
 * </pre>
 *
 * <p>Also requires, on the connector:
 *
 * <pre>
 *   header.converter                 org.apache.kafka.connect.storage.StringConverter
 * </pre>
 *
 * <p>The default {@code SimpleHeaderConverter} runs header values through {@code
 * Values.parseString}, which would turn the schema JSON into a {@code Map} instead of leaving it a
 * string.
 *
 * <h2>Why headers</h2>
 *
 * <p>Nothing but bytes and strings crosses from the converter to the sink. Kafka Connect's plugin
 * classloaders are child-first for custom packages, so a shared value type loaded from a converter
 * in the worker's {@code libs/} and a connector in a plugin directory would resolve to two distinct
 * {@code Class} objects and fail on every record. Headers avoid that entirely, and mean a converter
 * needs no compile-time dependency on Iceberg — just these three header names.
 *
 * <p>The payload on the topic must be a bare Avro datum with no framing — no Confluent magic byte,
 * no length prefix. If your producer writes a 5-byte Confluent prefix, strip it with {@code
 * ByteBuffer.wrap(bytes, 5, bytes.length - 5).slice()}; slicing keeps this zero-copy.
 */
public class RawAvroConverter implements Converter {

  public static final String SCHEMA_CONFIG = "schema";
  public static final String SCHEMA_NAME_CONFIG = "schema.name";
  public static final String SCHEMA_VERSION_CONFIG = "schema.version";

  // The wire contract with the sink. Duplicated as literals rather than imported, so a converter
  // never needs to depend on Iceberg -- see RawAvroHeaders in org.apache.iceberg.connect.data.
  private static final String SCHEMA_NAME_HEADER = "iceberg.avro.schema.name";
  private static final String SCHEMA_VERSION_HEADER = "iceberg.avro.schema.version";
  private static final String SCHEMA_HEADER = "iceberg.avro.schema";

  private static final Schema CONNECT_SCHEMA =
      SchemaBuilder.bytes().optional().name("org.apache.iceberg.connect.RawAvro").build();

  private String schemaName;
  private String schemaVersion;
  // Cached UTF-8 encodings, so every record attaches the same arrays rather than re-encoding.
  private byte[] schemaNameBytes;
  private byte[] schemaVersionBytes;
  private byte[] schemaJsonBytes;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (isKey) {
      throw new DataException(
          RawAvroConverter.class.getSimpleName()
              + " is a value converter; raw Avro passthrough does not apply to record keys");
    }

    Object schemaJson = configs.get(SCHEMA_CONFIG);
    if (schemaJson == null || schemaJson.toString().trim().isEmpty()) {
      throw new DataException(
          "Missing required config 'value.converter."
              + SCHEMA_CONFIG
              + "': the Avro writer schema, as JSON. Every datum in an Iceberg Avro data file must be "
              + "encoded against a known schema, and this converter does not consult a registry.");
    }

    org.apache.avro.Schema writerSchema =
        new org.apache.avro.Schema.Parser().parse(schemaJson.toString());
    if (writerSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
      throw new DataException(
          "value.converter."
              + SCHEMA_CONFIG
              + " must be a record schema, was "
              + writerSchema.getType());
    }

    Object name = configs.get(SCHEMA_NAME_CONFIG);
    this.schemaName =
        name == null || name.toString().trim().isEmpty()
            ? writerSchema.getFullName()
            : name.toString().trim();

    Object version = configs.get(SCHEMA_VERSION_CONFIG);
    this.schemaVersion = version == null ? "1" : version.toString().trim();
    // Fail at startup rather than per record if it is not a number.
    Integer.parseInt(schemaVersion);

    this.schemaNameBytes = schemaName.getBytes(StandardCharsets.UTF_8);
    this.schemaVersionBytes = schemaVersion.getBytes(StandardCharsets.UTF_8);
    // Reserialize from the parsed schema rather than passing the config text through, so the sink
    // receives canonical JSON regardless of how the config was formatted.
    this.schemaJsonBytes = writerSchema.toString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    throw new UnsupportedOperationException(
        RawAvroConverter.class.getSimpleName()
            + " needs record headers to describe the payload's schema");
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    if (value == null) {
      // Tombstone. The sink skips these.
      return SchemaAndValue.NULL;
    }

    // Set on the consumer record's headers, which WorkerSinkTask converts into the SinkRecord's
    // headers *after* the value converter runs -- so these reach the sink.
    headers.add(SCHEMA_NAME_HEADER, schemaNameBytes);
    headers.add(SCHEMA_VERSION_HEADER, schemaVersionBytes);
    headers.add(SCHEMA_HEADER, schemaJsonBytes);

    // ByteBuffer.wrap does not copy. The sink's appendEncoded copies once, into the Avro block
    // buffer, and leaves the buffer's position untouched.
    return new SchemaAndValue(CONNECT_SCHEMA, ByteBuffer.wrap(value));
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    throw new UnsupportedOperationException(
        RawAvroConverter.class.getSimpleName()
            + " only converts records into Iceberg, not out of it");
  }
}
