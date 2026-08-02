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
import java.util.Set;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.InternalReader;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Extracts partition values from an Avro payload without decoding the rest of it.
 *
 * <p>This is what lets raw passthrough work on a partitioned table. Iceberg's partitioning path
 * only ever asks a row for its partition-source fields — {@code StructTransform.wrap} calls {@code
 * accessors[i].get(row)} and nothing else — so a read projected down to just those fields is
 * sufficient. Everything else in the record is skipped, not materialized.
 *
 * <p>The projection is built with {@link TypeUtil#project}, which keeps the nesting a nested
 * partition source needs, and read with {@link InternalReader}. That reader is the right one for
 * two independent reasons: its rows are already {@link StructLike}, so {@code
 * PartitionKey.partition(row)} works with no adapter, and it produces Iceberg's internal
 * representations — {@code Integer} days, {@code Long} micros — which is exactly what the bound
 * transforms expect. The generic object model would hand back {@code LocalDate}/{@code
 * OffsetDateTime} and fail the cast.
 *
 * <p>Two caveats, both handled elsewhere:
 *
 * <ul>
 *   <li>{@code InternalReader} maps a plain {@code fixed(N)} to the same reader as {@code bytes},
 *       whose skip reads a length prefix that is not there. {@link AvroSchemaEligibility#check}
 *       reports that separately as not {@code partitionable()}, and the writer refuses such a
 *       schema on a partitioned table.
 *   <li>If the file schema carried no field ids, every field's partner would be null and the
 *       partition key would come back all-null with no error. {@link AvroSchemaAnnotator#annotate}
 *       is all-or-nothing, so by construction the schema handed here is fully annotated.
 * </ul>
 *
 * <p>Not implemented here: truncating the root read plan after the last projected field. {@code
 * ValueReaders.PlannedStructReader} has no early exit, so a read walks the whole record even when
 * every partition source sits near the front. Truncating is measurably faster but is only safe at
 * the root struct, and getting it wrong reads subsequent fields from the wrong offset — silent
 * corruption. Correctness first.
 */
class RawAvroPartitionExtractor {

  private static final Splitter DOT = Splitter.on('.');

  private final PartitionSpec spec;
  private final Schema projectedSchema;
  private final InternalReader<StructLike> reader;
  private final PartitionKey partitionKey;

  private BinaryDecoder decoder;
  private StructLike reuse;

  /**
   * @param spec the table's partition spec
   * @param tableSchema the table schema, source of the partition field ids
   * @param annotatedFileSchema the writer schema with Iceberg field ids attached
   */
  RawAvroPartitionExtractor(
      PartitionSpec spec, Schema tableSchema, org.apache.avro.Schema annotatedFileSchema) {
    this.spec = spec;

    Set<Integer> sourceIds = Sets.newHashSet();
    for (PartitionField field : spec.fields()) {
      // sourceId is the leaf field id, even for a nested source.
      sourceIds.add(field.sourceId());
    }

    // Keeps ancestors of nested sources, drops every sibling.
    this.projectedSchema = TypeUtil.project(tableSchema, sourceIds);

    checkSourcesPresent(annotatedFileSchema, tableSchema, sourceIds);

    this.reader = InternalReader.create(projectedSchema);
    reader.setSchema(annotatedFileSchema);
    this.partitionKey = new PartitionKey(spec, projectedSchema);
  }

  /**
   * Computes the partition for one payload. The returned key is reused across calls, so callers
   * must {@link PartitionKey#copy()} it before retaining it — which is what {@code StructLikeMap}
   * keying does.
   */
  PartitionKey partition(ByteBuffer payload) {
    this.decoder =
        DecoderFactory.get()
            .binaryDecoder(
                payload.array(),
                payload.arrayOffset() + payload.position(),
                payload.remaining(),
                decoder);
    try {
      this.reuse = reader.read(reuse, decoder);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    partitionKey.partition(reuse);
    return partitionKey;
  }

  /**
   * A partition source the producer's schema does not contain would read as null for every record,
   * quietly funnelling everything into one partition. Fail at writer creation instead.
   */
  private void checkSourcesPresent(
      org.apache.avro.Schema fileSchema, Schema tableSchema, Set<Integer> sourceIds) {
    List<String> missing = Lists.newArrayList();
    for (int sourceId : sourceIds) {
      String path = tableSchema.findColumnName(sourceId);
      if (path == null) {
        throw new ConnectException(
            String.format(
                Locale.ROOT,
                "Partition source field id %d is not in the table schema; the spec and schema disagree",
                sourceId));
      }
      if (!fieldExists(fileSchema, path)) {
        missing.add(path);
      }
    }

    if (!missing.isEmpty()) {
      throw new ConnectException(
          String.format(
              Locale.ROOT,
              "Avro schema %s does not contain the partition source field(s) %s. Every record would "
                  + "partition as null, so the whole topic would land in one partition. Either the "
                  + "producer is on a schema older than the partition spec, or the spec partitions on "
                  + "a column the producer never sends.",
              fileSchema.getFullName(),
              missing));
    }
  }

  private static boolean fieldExists(org.apache.avro.Schema record, String dottedPath) {
    org.apache.avro.Schema current = record;
    for (String part : DOT.split(dottedPath)) {
      current = unwrapOption(current);
      if (current.getType() != org.apache.avro.Schema.Type.RECORD) {
        return false;
      }
      org.apache.avro.Schema.Field field = current.getField(part);
      if (field == null) {
        return false;
      }
      current = field.schema();
    }
    return true;
  }

  private static org.apache.avro.Schema unwrapOption(org.apache.avro.Schema schema) {
    if (schema.getType() != org.apache.avro.Schema.Type.UNION) {
      return schema;
    }
    for (org.apache.avro.Schema branch : schema.getTypes()) {
      if (branch.getType() != org.apache.avro.Schema.Type.NULL) {
        return branch;
      }
    }
    return schema;
  }

  PartitionSpec spec() {
    return spec;
  }
}
