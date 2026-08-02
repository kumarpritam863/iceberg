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
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A {@link FileAppender} that writes already-encoded Avro datums into an Avro data file without
 * decoding them.
 *
 * <p>Each {@link RawAvroPayload} is handed to {@code DataFileWriter.appendEncoded}, which copies
 * the bytes into the current block buffer and bumps the block's object count. There is no {@code
 * DatumWriter} involved and no re-encode: the Kafka payload bytes become the data file's bytes.
 *
 * <p>Every datum in an Avro object-container file must be encoded against the file header's single
 * schema, so one of these handles exactly one writer-schema version. The caller keeps one appender
 * per {@code (table, partition, schemaVersion)}.
 *
 * <h2>What this does not check</h2>
 *
 * <p>{@code appendEncoded} validates nothing. If the bytes do not match the header schema the
 * result is either an unreadable block or, worse, plausible-but-wrong values with no error at write
 * or read time. Two guards are cheap enough to keep in the hot path — identity of the writer
 * schema, and a non-empty payload — and they are here. Everything structural is the responsibility
 * of {@link AvroSchemaEligibility}, run once per schema version before an appender is ever created.
 *
 * <h2>Metrics</h2>
 *
 * <p>{@link #metrics()} reports the record count and nothing else. That is not a regression:
 * Iceberg's own {@code AvroMetrics.fromWriter} is a stub returning {@code new Metrics(numRecords,
 * null, null, null, null)}, so <em>every</em> Avro data file Iceberg writes today already carries
 * zero column statistics regardless of {@code write.metadata.metrics.default}. Column-level stats
 * are optional per the spec, both metrics evaluators null-guard and fail open, and the
 * kafka-connect commit path only reads {@code recordCount()}.
 */
class EncodedAvroFileAppender implements FileAppender<RawAvroPayload> {

  private final String expectedSchemaName;
  private final int expectedSchemaVersion;
  private final PositionOutputStream stream;
  private final DataFileWriter<Object> writer;

  private long recordCount = 0L;
  private boolean closed = false;

  /**
   * @param fileSchema the annotated writer schema, written verbatim into the file header
   * @param expectedSchemaName registry name every payload must carry
   * @param expectedSchemaVersion registry version every payload must carry
   * @param icebergSchema table schema, recorded as {@code iceberg.schema} file metadata to match
   *     what Iceberg's own Avro appender writes
   * @param file where to write
   * @param codec block compression, applied at block flush independently of how datums arrived
   * @param metadata extra key/value file metadata
   */
  EncodedAvroFileAppender(
      Schema fileSchema,
      String expectedSchemaName,
      int expectedSchemaVersion,
      org.apache.iceberg.Schema icebergSchema,
      OutputFile file,
      CodecFactory codec,
      Map<String, String> metadata,
      boolean overwrite) {
    this.expectedSchemaName = expectedSchemaName;
    this.expectedSchemaVersion = expectedSchemaVersion;
    this.stream = overwrite ? file.createOrOverwrite() : file.create();

    DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(new UnusedDatumWriter());
    dataFileWriter.setCodec(codec);
    // Iceberg's Avro appender records the table schema in the file header; keep parity so tooling
    // that reads iceberg.schema behaves the same for raw-written files.
    dataFileWriter.setMeta("iceberg.schema", SchemaParser.toJson(icebergSchema));
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      dataFileWriter.setMeta(entry.getKey(), entry.getValue());
    }

    try {
      this.writer = dataFileWriter.create(fileSchema, stream);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create Avro file: %s", file.location());
    }
  }

  @Override
  public void add(RawAvroPayload row) {
    Preconditions.checkState(!closed, "Cannot add to a closed appender");
    // Guard on (name, version), not on schema equality: this file's header schema is the
    // *annotated*
    // form -- field ids injected, record names rewritten -- so it is never equal to the producer
    // schema the payload was encoded against. The registry coordinates are the real discriminator,
    // and comparing them is O(1) rather than a deep structural walk per record.
    Preconditions.checkArgument(
        expectedSchemaName.equals(row.schemaName()) && expectedSchemaVersion == row.schemaVersion(),
        "Payload schema %s v%s does not match this file's %s v%s. Every datum in an Avro file must be"
            + " encoded against the header schema; appending this would corrupt the file.",
        row.schemaName(),
        row.schemaVersion(),
        expectedSchemaName,
        expectedSchemaVersion);
    Preconditions.checkArgument(
        row.size() > 0,
        "Refusing to append a zero-length payload (%s v%s): it would increment the block count "
            + "without contributing a datum, desynchronizing every reader of this block.",
        row.schemaName(),
        row.schemaVersion());

    try {
      writer.appendEncoded(row.payload());
      recordCount += 1L;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to append encoded datum");
    }
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(closed, "Cannot return metrics while appending to an open file.");
    return new Metrics(recordCount);
  }

  /**
   * Byte length written so far. Must work before {@code close()}: the rolling writers call this per
   * batch to decide when to start a new file.
   */
  @Override
  public long length() {
    try {
      return stream.storedLength();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get stream length");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      // Closes the current block and the underlying stream.
      writer.close();
      closed = true;
    }
  }

  /**
   * {@link DataFileWriter} requires a {@link DatumWriter} and calls {@code setSchema} on it during
   * {@code create}, but only calls {@code write} from {@code append}. This appender only ever uses
   * {@code appendEncoded}, so reaching {@code write} means a datum took the wrong path.
   */
  private static class UnusedDatumWriter implements DatumWriter<Object> {
    @Override
    public void setSchema(Schema schema) {}

    @Override
    public void write(Object datum, Encoder out) {
      throw new UnsupportedOperationException(
          "EncodedAvroFileAppender writes pre-encoded bytes; DatumWriter.write should never be called");
    }
  }
}
