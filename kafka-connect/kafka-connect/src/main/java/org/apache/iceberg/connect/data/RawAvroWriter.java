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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes Kafka records whose values are still Avro-encoded, straight into Iceberg Avro data files.
 *
 * <p>No decode, no Connect {@code Struct}, no Iceberg {@code Record}, no re-encode: the payload
 * bytes a producer put on the topic become the data file's bytes. See {@code
 * kafka-connect/ZERO_COPY_AVRO_DESIGN.md}.
 *
 * <h2>One writer per schema version</h2>
 *
 * <p>An Avro object-container file header carries exactly one schema, and every datum in the file
 * must be encoded against it. Kafka delivers versions interleaved, so this class keeps one {@link
 * RollingDataWriter} per writer-schema version and routes each record to the matching one. Each
 * such writer still rolls by target file size on its own.
 *
 * <p>The per-version setup — eligibility check, field-id annotation, writer factory — runs once and
 * is cached. That is what keeps the per-record path down to a buffer copy.
 *
 * <h2>Partitioned tables</h2>
 *
 * <p>Supported. {@link RawAvroPartitionExtractor} decodes only the partition-source fields out of
 * the payload and skips the rest, so writers are keyed by {@code (schemaVersion, partition)}. A
 * schema whose bytes cannot be safely skipped — see {@link
 * AvroSchemaEligibility.Result#partitionable()} — is refused on a partitioned table rather than
 * risking a desynchronized skip walk.
 */
class RawAvroWriter implements RecordWriter {

  private static final Logger LOG = LoggerFactory.getLogger(RawAvroWriter.class);

  private final Table table;
  private final TableReference tableReference;
  private final IcebergSinkConfig config;
  private final Map<String, String> tableProps;
  private final long targetFileSize;

  /**
   * Writers keyed first by "&lt;schemaName&gt;/&lt;version&gt;" — an Avro file header holds one
   * schema — then by partition within that version. Unpartitioned tables use a single-entry inner
   * map keyed on an empty struct.
   */
  private final Map<String, StructLikeMap<RollingDataWriter<RawAvroPayload>>> writers;

  private final RawAvroHeaders headers = new RawAvroHeaders();
  private final Map<String, org.apache.avro.Schema> annotatedSchemas;
  private final Map<String, RawAvroPartitionExtractor> extractors;
  private final List<IcebergWriterResult> completedResults;

  RawAvroWriter(Table table, TableReference tableReference, IcebergSinkConfig config) {
    this.table = table;
    this.tableReference = tableReference;
    this.config = config;

    this.tableProps = Maps.newHashMap(table.properties());
    tableProps.putAll(config.writeProps());
    this.targetFileSize =
        PropertyUtil.propertyAsLong(
            tableProps,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    this.writers = Maps.newHashMap();
    this.annotatedSchemas = Maps.newHashMap();
    this.extractors = Maps.newHashMap();
    this.completedResults = Lists.newArrayList();
  }

  @Override
  public void write(SinkRecord record) {
    // Tombstones carry no payload; there is nothing to append.
    if (record.value() == null) {
      return;
    }

    RawAvroHeaders.Coordinates coordinates = headers.read(record);
    RawAvroPayload payload = toPayload(record, coordinates);

    // Deliberately outside the try below. Preparing a writer can fail because the schema is
    // ineligible or the table cannot be evolved -- those are configuration faults that should fail
    // the task, not per-record DataExceptions that send every record to the dead-letter queue.
    RollingDataWriter<RawAvroPayload> writer = writerFor(payload, coordinates);

    try {
      writer.write(payload);
    } catch (Exception e) {
      throw new DataException(
          String.format(
              Locale.ROOT,
              "An error occurred appending raw Avro payload, topic: %s, partition: %d, offset: %d, "
                  + "schema: %s v%d",
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset(),
              payload.schemaName(),
              payload.schemaVersion()),
          e);
    }
  }

  /**
   * Pairs the record's value bytes with the writer schema its headers describe.
   *
   * <p>The value must be the bare Avro encoding of one datum -- no framing, no magic byte, no
   * length prefix. A {@link ByteBuffer} is passed through without copying; a {@code byte[]} is
   * wrapped.
   */
  private RawAvroPayload toPayload(SinkRecord record, RawAvroHeaders.Coordinates coordinates) {
    Object value = record.value();
    ByteBuffer bytes;
    if (value instanceof ByteBuffer) {
      bytes = (ByteBuffer) value;
    } else if (value instanceof byte[]) {
      bytes = ByteBuffer.wrap((byte[]) value);
    } else {
      throw new DataException(
          String.format(
              Locale.ROOT,
              "Raw Avro mode expects the record value to be bytes, got %s. The value converter must "
                  + "emit the undecoded Avro payload as a ByteBuffer or byte[].",
              value.getClass().getName()));
    }

    return new RawAvroPayload(
        bytes, coordinates.writerSchema(), coordinates.schemaName(), coordinates.schemaVersion());
  }

  private RollingDataWriter<RawAvroPayload> writerFor(
      RawAvroPayload payload, RawAvroHeaders.Coordinates coordinates) {
    // Key built once per version by RawAvroHeaders, not rebuilt per record.
    String key = coordinates.cacheKey();

    // Preparing the schema also decides whether this version can be partitioned at all, so it has
    // to
    // run before the extractor is built. get-then-put rather than computeIfAbsent: the lambda would
    // capture `payload` and allocate on every record, cache hit included.
    org.apache.avro.Schema annotated = annotatedSchemas.get(key);
    if (annotated == null) {
      annotated = prepareSchema(payload);
      annotatedSchemas.put(key, annotated);
    }

    StructLike partition = partitionFor(key, annotated, payload);

    StructLikeMap<RollingDataWriter<RawAvroPayload>> byPartition = writers.get(key);
    if (byPartition == null) {
      byPartition = StructLikeMap.create(table.spec().partitionType());
      writers.put(key, byPartition);
    }

    RollingDataWriter<RawAvroPayload> writer = byPartition.get(partition);
    if (writer == null) {
      // The extractor hands back a mutable, reused key, so copy before it becomes a map key.
      StructLike stored =
          partition instanceof PartitionKey ? ((PartitionKey) partition).copy() : partition;
      writer = newWriter(annotated, payload, stored);
      byPartition.put(stored, writer);
    }
    return writer;
  }

  /**
   * The partition this record belongs to, decoded from the payload without materializing the
   * record. An empty struct for an unpartitioned table, which keys the single writer per schema
   * version.
   */
  private StructLike partitionFor(
      String key, org.apache.avro.Schema annotated, RawAvroPayload payload) {
    if (table.spec().isUnpartitioned()) {
      return EMPTY_PARTITION;
    }

    RawAvroPartitionExtractor extractor = extractors.get(key);
    if (extractor == null) {
      extractor = new RawAvroPartitionExtractor(table.spec(), table.schema(), annotated);
      extractors.put(key, extractor);
    }
    // No duplicate() needed: the extractor reads array/offset/length and never touches position.
    return extractor.partition(payload.payload());
  }

  private RollingDataWriter<RawAvroPayload> newWriter(
      org.apache.avro.Schema annotated, RawAvroPayload payload, StructLike partition) {
    // A distinct operationId per writer keeps generated file names unique, since partitionId and
    // taskId are shared across every writer this class holds.
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(org.apache.iceberg.FileFormat.AVRO)
            .build();

    EncodedAvroWriterFactory writerFactory =
        new EncodedAvroWriterFactory(
            annotated,
            payload.schemaName(),
            payload.schemaVersion(),
            table.schema(),
            tableProps.getOrDefault(
                TableProperties.AVRO_COMPRESSION, TableProperties.AVRO_COMPRESSION_DEFAULT),
            ImmutableMap.of());

    LOG.info(
        "Opening raw Avro writer for table {}, schema {} v{}, partition {}",
        tableReference.identifier(),
        payload.schemaName(),
        payload.schemaVersion(),
        table.spec().isUnpartitioned()
            ? "<unpartitioned>"
            : table.spec().partitionToPath(partition));

    return new RollingDataWriter<>(
        writerFactory,
        fileFactory,
        table.io(),
        targetFileSize,
        table.spec(),
        table.spec().isUnpartitioned() ? null : partition);
  }

  /**
   * Runs the once-per-version safety work: check that the schema's encoding is representable in
   * Iceberg at all, evolve the table if the producer has moved ahead of it, then attach the table's
   * field ids by name.
   */
  private org.apache.avro.Schema prepareSchema(RawAvroPayload payload) {
    AvroSchemaEligibility.Result eligibility = AvroSchemaEligibility.check(payload.writerSchema());
    if (!eligibility.writable()) {
      // Failing loudly is the point. Nothing downstream would catch a bad schema: appendEncoded
      // copies bytes verbatim, so the alternative is silently corrupt table data.
      throw new ConnectException(
          String.format(
              Locale.ROOT,
              "Avro schema %s v%d cannot be written via raw passthrough. %s",
              payload.schemaName(),
              payload.schemaVersion(),
              eligibility.explain()));
    }

    if (!table.spec().isUnpartitioned() && !eligibility.partitionable()) {
      // Writing is fine; extracting partition values from these bytes is not. Refusing beats
      // writing
      // every record into one wrong partition, or desynchronizing the skip walk.
      throw new ConnectException(
          String.format(
              Locale.ROOT,
              "Avro schema %s v%d can be written raw but partition values cannot be extracted from it, "
                  + "and table %s is partitioned. %s",
              payload.schemaName(),
              payload.schemaVersion(),
              tableReference.identifier(),
              eligibility.explain()));
    }

    List<String> unmatched =
        AvroSchemaAnnotator.unmatchedFields(payload.writerSchema(), table.schema());
    if (!unmatched.isEmpty()) {
      if (!config.evolveSchemaEnabled()) {
        throw new ConnectException(
            String.format(
                Locale.ROOT,
                "Avro schema %s v%d has fields the table %s does not: %s. Every field needs an Iceberg"
                    + " field id -- partial annotation would make unannotated columns read back as"
                    + " nulls -- so either add the columns, or enable"
                    + " iceberg.tables.evolve-schema-enabled.",
                payload.schemaName(),
                payload.schemaVersion(),
                tableReference.identifier(),
                unmatched));
      }
      evolveTable(payload, unmatched);
    }

    return AvroSchemaAnnotator.annotate(payload.writerSchema(), table.schema());
  }

  /**
   * Adds the columns a newer producer schema has and the table does not.
   *
   * <p>Only the schema is needed for this, never the values: the Avro writer schema converts
   * straight to Iceberg types. New columns are added as optional, since existing files do not
   * contain them.
   */
  private void evolveTable(RawAvroPayload payload, List<String> unmatched) {
    // Sanitized for the same reason as auto-create: an added column's doc becomes a catalog column
    // comment, and Glue rejects newlines in one.
    org.apache.iceberg.Schema fromAvro =
        SchemaDocs.sanitize(AvroSchemaUtil.toIceberg(payload.writerSchema()));

    UpdateSchema update = table.updateSchema();
    boolean any = false;
    for (String path : unmatched) {
      // unmatchedFields also reports missing collection element/key/value ids, which are not
      // columns
      // and cannot be added; those mean a genuine type mismatch rather than a missing column.
      if (path.contains("[]") || path.contains("{}")) {
        throw new ConnectException(
            String.format(
                Locale.ROOT,
                "Cannot evolve table %s for schema %s v%d: '%s' is a collection whose element types do"
                    + " not line up with the table. Fix the table schema by hand.",
                tableReference.identifier(),
                payload.schemaName(),
                payload.schemaVersion(),
                path));
      }

      Types.NestedField field = fromAvro.findField(path);
      if (field == null) {
        throw new ConnectException(
            String.format(
                Locale.ROOT,
                "Cannot evolve table %s: '%s' is absent from the table but could not be resolved in"
                    + " Avro schema %s v%d either.",
                tableReference.identifier(),
                path,
                payload.schemaName(),
                payload.schemaVersion()));
      }

      int lastDot = path.lastIndexOf('.');
      if (lastDot < 0) {
        update.addColumn(null, path, field.type());
      } else {
        update.addColumn(path.substring(0, lastDot), path.substring(lastDot + 1), field.type());
      }
      any = true;
      LOG.info(
          "Evolving table {}: adding optional column {} {} for schema {} v{}",
          tableReference.identifier(),
          path,
          field.type(),
          payload.schemaName(),
          payload.schemaVersion());
    }

    if (any) {
      update.commit();
      table.refresh();
    }
  }

  @Override
  public List<IcebergWriterResult> complete() {
    flush();

    List<IcebergWriterResult> results = Lists.newArrayList(completedResults);
    completedResults.clear();
    return results;
  }

  private void flush() {
    List<org.apache.iceberg.DataFile> dataFiles = Lists.newArrayList();
    for (StructLikeMap<RollingDataWriter<RawAvroPayload>> byPartition : writers.values()) {
      for (RollingDataWriter<RawAvroPayload> writer : byPartition.values()) {
        try {
          writer.close();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        dataFiles.addAll(writer.result().dataFiles());
      }
    }

    // One result per flush. Every file here shares the table's current spec, which is what the
    // commit
    // protocol needs -- a DataWritten event carries a single partition struct.
    if (!dataFiles.isEmpty()) {
      completedResults.add(
          new IcebergWriterResult(
              tableReference, dataFiles, Lists.newArrayList(), table.spec().partitionType()));
    }

    // RollingDataWriter is single-use: writing after close silently loses rows. Drop them all so
    // the
    // next batch opens fresh writers.
    writers.clear();
    extractors.clear();
  }

  @Override
  public void close() {
    for (StructLikeMap<RollingDataWriter<RawAvroPayload>> byPartition : writers.values()) {
      for (RollingDataWriter<RawAvroPayload> writer : byPartition.values()) {
        try {
          writer.close();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
    writers.clear();
    extractors.clear();
  }

  /** Key for the single writer an unpartitioned table needs. */
  private static final StructLike EMPTY_PARTITION =
      new StructLike() {
        @Override
        public int size() {
          return 0;
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
          throw new IndexOutOfBoundsException("Unpartitioned: no partition values");
        }

        @Override
        public <T> void set(int pos, T value) {
          throw new IndexOutOfBoundsException("Unpartitioned: no partition values");
        }
      };
}
