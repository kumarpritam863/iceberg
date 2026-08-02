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

import java.nio.ByteBuffer;
import java.util.Locale;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A Kafka record value that is still an Avro-encoded datum.
 *
 * <p>The row type of the zero-copy write path. The sink builds one of these per record by pairing
 * the record's value bytes with the writer schema described by its headers (see {@link
 * RawAvroHeaders}), then appends {@link #payload()} into an Iceberg Avro data file byte-for-byte —
 * skipping decode, Struct materialization, Iceberg-record conversion, and re-encode.
 *
 * <p>Sink-internal on purpose. Nothing crosses the converter boundary but bytes and strings, so
 * there is no shared custom type for Connect's plugin classloaders to load twice.
 *
 * <h2>Buffer ownership</h2>
 *
 * <p>{@link #payload()} is typically a {@link ByteBuffer#slice()} over the original Kafka {@code
 * byte[]} — no copy. The sink must not retain it past {@code put()}, and must not mutate it. {@code
 * DataFileWriter.appendEncoded} copies the bytes into its block buffer and leaves the position
 * unchanged, so a single buffer can be handed straight through.
 */
final class RawAvroPayload {

  private final ByteBuffer payload;
  private final Schema writerSchema;
  private final String schemaName;
  private final int schemaVersion;

  RawAvroPayload(ByteBuffer payload, Schema writerSchema, String schemaName, int schemaVersion) {
    Preconditions.checkNotNull(payload, "payload cannot be null");
    Preconditions.checkNotNull(writerSchema, "writerSchema cannot be null");
    Preconditions.checkNotNull(schemaName, "schemaName cannot be null");
    Preconditions.checkArgument(
        writerSchema.getType() == Schema.Type.RECORD,
        "writerSchema must be a record, was: %s",
        writerSchema.getType());
    this.payload = payload;
    this.writerSchema = writerSchema;
    this.schemaName = schemaName;
    this.schemaVersion = schemaVersion;
  }

  /**
   * The Avro binary encoding of exactly one datum, with no framing, magic bytes, or length prefix.
   *
   * <p>Must be the encoding of {@link #writerSchema()}. Nothing downstream validates this — {@code
   * appendEncoded} copies the bytes verbatim — so a mismatch is silent data corruption. See {@link
   * AvroSchemaEligibility} for the checks that make the contract safe to rely on.
   */
  ByteBuffer payload() {
    return payload;
  }

  /** The schema the payload bytes were encoded against. */
  Schema writerSchema() {
    return writerSchema;
  }

  /** Registry name of the writer schema, used as part of the per-version cache key. */
  String schemaName() {
    return schemaName;
  }

  /**
   * Registry version of the writer schema.
   *
   * <p>An Avro object-container file header carries exactly one schema, so the sink must keep one
   * open file per distinct version. This is the discriminator.
   */
  int schemaVersion() {
    return schemaVersion;
  }

  /** Number of payload bytes, without consuming the buffer. */
  int size() {
    return payload.remaining();
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "RawAvroPayload(%s v%d, %d bytes)",
        schemaName,
        schemaVersion,
        payload.remaining());
  }
}
