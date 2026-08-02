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

import java.util.Map;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Creates data writers that append pre-encoded Avro payloads.
 *
 * <p>Iceberg's writer stack is generic in the row type all the way down — {@code DataWriter.write}
 * is literally {@code appender.add(row)}, and the rolling and fanout writers only look at row
 * counts and byte lengths. So supplying a {@link FileWriterFactory} of {@link RawAvroPayload} is
 * enough to reuse the whole stack; nothing in {@code iceberg-core} needs to change.
 *
 * <p>One factory serves one writer-schema version, because an Avro file header carries a single
 * schema. The caller keys factories by {@code (table, schemaVersion)} and hands each one out to the
 * writers for that version.
 *
 * <p>Delete writers are unsupported: raw passthrough is an append-only path. Equality deletes would
 * need the row's values, which is precisely what this path declines to decode.
 */
class EncodedAvroWriterFactory implements FileWriterFactory<RawAvroPayload> {

  private final org.apache.avro.Schema fileSchema;
  private final String schemaName;
  private final int schemaVersion;
  private final String icebergSchemaJson;
  private final CodecFactory codec;
  private final Map<String, String> metadata;

  EncodedAvroWriterFactory(
      org.apache.avro.Schema fileSchema,
      String schemaName,
      int schemaVersion,
      org.apache.iceberg.Schema icebergSchema,
      String codecName,
      Map<String, String> metadata) {
    this.fileSchema = fileSchema;
    this.schemaName = schemaName;
    this.schemaVersion = schemaVersion;
    // Serialized once per factory rather than once per file: files roll by size and are
    // reopened on every flush, and a wide schema is not cheap to serialize.
    this.icebergSchemaJson = org.apache.iceberg.SchemaParser.toJson(icebergSchema);
    this.codec = toCodec(codecName);
    this.metadata = ImmutableMap.copyOf(metadata);
  }

  @Override
  public DataWriter<RawAvroPayload> newDataWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    EncodedAvroFileAppender appender =
        new EncodedAvroFileAppender(
            fileSchema,
            schemaName,
            schemaVersion,
            icebergSchemaJson,
            file.encryptingOutputFile(),
            codec,
            metadata,
            /* overwrite= */ false);

    return new DataWriter<>(
        appender,
        FileFormat.AVRO,
        file.encryptingOutputFile().location(),
        spec,
        partition,
        file.keyMetadata());
  }

  @Override
  public EqualityDeleteWriter<RawAvroPayload> newEqualityDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    throw new UnsupportedOperationException(
        "Raw Avro passthrough is append-only: equality deletes would require decoding the payload");
  }

  @Override
  public PositionDeleteWriter<RawAvroPayload> newPositionDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    throw new UnsupportedOperationException(
        "Raw Avro passthrough is append-only: position deletes are not supported");
  }

  /**
   * Mirrors the codec names Iceberg's own Avro write path accepts, so {@code
   * write.avro.compression-codec} behaves identically for raw-written files.
   */
  private static CodecFactory toCodec(String name) {
    switch (name.toLowerCase(java.util.Locale.ROOT)) {
      case DataFileConstants.NULL_CODEC:
      case "uncompressed":
        return CodecFactory.nullCodec();
      case DataFileConstants.DEFLATE_CODEC:
      case "gzip":
        return CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
      case DataFileConstants.SNAPPY_CODEC:
        return CodecFactory.snappyCodec();
      case DataFileConstants.ZSTANDARD_CODEC:
      case "zstd":
        return CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL);
      case DataFileConstants.BZIP2_CODEC:
        return CodecFactory.bzip2Codec();
      default:
        throw new IllegalArgumentException("Unsupported Avro compression codec: " + name);
    }
  }
}
