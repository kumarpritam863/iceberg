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

import java.util.Locale;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * The record-header contract between a raw-Avro converter and this sink.
 *
 * <p>A converter that hands over undecoded Avro bytes describes them with three headers:
 *
 * <pre>
 *   iceberg.avro.schema.name      the writer schema's registry name        e.g. com.example.Event
 *   iceberg.avro.schema.version   the writer schema's registry version     e.g. 7
 *   iceberg.avro.schema           the writer schema itself, as JSON
 * </pre>
 *
 * <p>Headers rather than a shared value type, deliberately. Kafka Connect's {@code
 * PluginClassLoader} is child-first for every package outside {@code java*}, {@code javax*}, {@code
 * org.apache.kafka} and {@code org.slf4j}, so a custom class passed from a converter in the
 * worker's {@code libs/} to a connector in a plugin directory resolves to two different {@code
 * Class} objects and fails on every record. Strings have no such problem. This also means the
 * converter needs no compile-time dependency on Iceberg at all — it just sets three headers whose
 * names are a documented wire contract.
 *
 * <p>Shipping the whole schema costs nothing meaningful: the converter and the sink run in the same
 * JVM on the same thread, so headers are an in-memory object that is never serialized. A converter
 * that caches the JSON per version attaches the same {@code String} reference to every record.
 *
 * <p>The parsed {@link Schema} is cached per instance by name and version, so the JSON is parsed
 * once per schema version rather than once per record. The cache is deliberately <em>not</em>
 * static: a global one keyed on name and version would hand back a stale schema if two sources ever
 * disagreed about what a given version contains, and would leak state between tables. Scoping it to
 * one writer matches the invariant the appender already enforces — every datum in a file shares one
 * schema version.
 *
 * <p>Note the hot path only ever touches the two short headers. The schema header is read on a
 * cache miss only, which matters because each read materializes a fresh {@code String} of the whole
 * JSON.
 */
class RawAvroHeaders {

  static final String SCHEMA_NAME = "iceberg.avro.schema.name";
  static final String SCHEMA_VERSION = "iceberg.avro.schema.version";
  static final String SCHEMA = "iceberg.avro.schema";

  /** Keyed by "name/version". Small and stable; a task writes a handful of live versions. */
  private final Map<String, Coordinates> cache = Maps.newHashMap();

  /**
   * The writer schema's identity and definition, as described by a record's headers. Immutable and
   * cached per version, so the per-record path allocates none of this.
   */
  static class Coordinates {
    private final String schemaName;
    private final int schemaVersion;
    private final Schema writerSchema;
    private final String cacheKey;

    private Coordinates(
        String schemaName, int schemaVersion, Schema writerSchema, String cacheKey) {
      this.schemaName = schemaName;
      this.schemaVersion = schemaVersion;
      this.writerSchema = writerSchema;
      this.cacheKey = cacheKey;
    }

    /**
     * "name/version". Shared so downstream per-version caches key off the same string rather than
     * rebuilding it per record.
     */
    String cacheKey() {
      return cacheKey;
    }

    String schemaName() {
      return schemaName;
    }

    int schemaVersion() {
      return schemaVersion;
    }

    Schema writerSchema() {
      return writerSchema;
    }
  }

  /**
   * True when a record carries the raw-Avro headers. Used to decide whether the raw write path
   * applies to a given table, so a misconfigured connector fails at writer creation rather than
   * mid-batch.
   */
  static boolean present(SinkRecord record) {
    return record.headers() != null && record.headers().lastWithName(SCHEMA_NAME) != null;
  }

  /**
   * Reads and parses the coordinates with no caching. For one-off use — table auto-creation — not
   * the per-record path.
   */
  static Coordinates parse(SinkRecord record) {
    String name = requireString(record, SCHEMA_NAME);
    int version = version(record);
    return new Coordinates(name, version, parseSchema(record, name, version), name + "/" + version);
  }

  /**
   * Reads the writer schema coordinates from a record's headers, parsing the schema JSON only the
   * first time a given name and version is seen.
   *
   * @throws ConnectException if a header is missing, or arrived as something other than a string
   */
  Coordinates read(SinkRecord record) {
    String name = requireString(record, SCHEMA_NAME);
    int version = version(record);

    String cacheKey = name + "/" + version;
    Coordinates cached = cache.get(cacheKey);
    if (cached != null) {
      return cached;
    }

    Coordinates coordinates =
        new Coordinates(name, version, parseSchema(record, name, version), cacheKey);
    cache.put(cacheKey, coordinates);
    return coordinates;
  }

  private static int version(SinkRecord record) {
    String versionText = requireString(record, SCHEMA_VERSION);
    try {
      return Integer.parseInt(versionText.trim());
    } catch (NumberFormatException e) {
      throw new ConnectException(
          String.format(
              Locale.ROOT, "Header %s is not an integer: '%s'", SCHEMA_VERSION, versionText),
          e);
    }
  }

  private static Schema parseSchema(SinkRecord record, String name, int version) {
    String json = requireString(record, SCHEMA);
    Schema parsed;
    try {
      parsed = new Schema.Parser().parse(json);
    } catch (RuntimeException e) {
      throw new ConnectException(
          String.format(
              Locale.ROOT,
              "Header %s for %s v%d is not a parseable Avro schema. First 200 chars: %s",
              SCHEMA,
              name,
              version,
              json.length() > 200 ? json.substring(0, 200) : json),
          e);
    }

    if (parsed.getType() != Schema.Type.RECORD) {
      throw new ConnectException(
          String.format(
              Locale.ROOT,
              "Header %s for %s v%d must be a record schema, was %s",
              SCHEMA,
              name,
              version,
              parsed.getType()));
    }

    return parsed;
  }

  private static String requireString(SinkRecord record, String headerName) {
    Header header = record.headers() == null ? null : record.headers().lastWithName(headerName);
    if (header == null || header.value() == null) {
      throw new ConnectException(
          String.format(
              Locale.ROOT,
              "Raw Avro mode is enabled but record header '%s' is missing (topic %s, partition %d, "
                  + "offset %d). The value converter must set %s, %s and %s.",
              headerName,
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset(),
              SCHEMA_NAME,
              SCHEMA_VERSION,
              SCHEMA));
    }

    Object value = header.value();
    if (value instanceof String) {
      return (String) value;
    }
    if (value instanceof byte[]) {
      return new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8);
    }

    // The usual cause: the worker's default SimpleHeaderConverter ran Values.parseString over the
    // header, which turns a JSON schema into a Map rather than leaving it a String.
    throw new ConnectException(
        String.format(
            Locale.ROOT,
            "Header '%s' arrived as %s, not a string. Set header.converter to "
                + "org.apache.kafka.connect.storage.StringConverter -- the default SimpleHeaderConverter "
                + "parses header values and will mangle an Avro schema into a Map.",
            headerName,
            value.getClass().getName()));
  }
}
