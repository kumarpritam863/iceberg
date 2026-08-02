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

import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Attaches Iceberg field IDs to a Kafka writer schema so the annotated schema can be written into
 * an Avro data file's header and read back as table data.
 *
 * <p>Iceberg's Avro reader resolves fields by ID against the file's own field order, skipping file
 * fields the table does not have and null-filling table fields the file lacks. That is what lets
 * each data file carry a foreign writer schema verbatim: no normalization at write time,
 * reconciliation per file at read time. The only thing the file needs is IDs, and those come from
 * matching field <em>names</em> against the table schema — which is exactly what {@link
 * NameMapping} does.
 *
 * <h2>All or nothing</h2>
 *
 * <p>Partial annotation is <b>worse than none</b>. A single ID anywhere makes {@code
 * AvroSchemaUtil.hasIds} true, and {@code NameMappingDatumReader} then skips the name-mapping
 * fallback for the whole file — so any unannotated column reads back as silent nulls, or hard-fails
 * if the table column is required. {@link #annotate} therefore verifies that every field received
 * an ID and refuses the schema otherwise.
 *
 * <h2>Record names</h2>
 *
 * <p>Record names are rewritten to {@code r<n>}. Avro names never affect the wire format, but
 * Iceberg's {@code GenericAvroReader} will try to load the record's full name as a class and, if it
 * resolves to an {@code IndexedRecord} on the worker classpath, instantiate that instead of a
 * generic record — which breaks as soon as the table gains a column. Registry-generated {@code
 * SpecificRecord} names are exactly this hazard inside a Kafka Connect worker, so the names are
 * neutralised.
 *
 * <p>Field IDs assigned by the catalog are authoritative: {@code TableMetadata.newTableMetadata}
 * runs {@code TypeUtil.assignFreshIds} on create, so any IDs invented before the table existed are
 * discarded. Always annotate against a loaded table's schema, never against a schema you built to
 * create it with.
 */
public class AvroSchemaAnnotator {

  private AvroSchemaAnnotator() {}

  /**
   * Returns {@code writerSchema} with Iceberg field IDs attached, matched by name against {@code
   * tableSchema}.
   *
   * @param writerSchema the Kafka writer schema, normally carrying no IDs
   * @param tableSchema the schema of the loaded Iceberg table
   * @return an annotated copy, safe to use as an Avro data file header schema
   * @throws IllegalArgumentException if any field could not be matched to a table field
   */
  public static Schema annotate(Schema writerSchema, org.apache.iceberg.Schema tableSchema) {
    NameMapping mapping = MappingUtil.create(tableSchema);
    Schema annotated = AvroSchemaUtil.applyNameMapping(writerSchema, mapping);

    List<String> unmatched = Lists.newArrayList();
    collectUnmatched(annotated, "", unmatched);
    if (!unmatched.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot annotate Avro schema '%s': no Iceberg field id for %s. Partial annotation is "
                  + "unsafe -- one id anywhere disables the reader's name-mapping fallback for the "
                  + "whole file, so unannotated columns would read back as nulls. Either add the "
                  + "missing columns to the table or fall back to the decode path.",
              writerSchema.getFullName(), unmatched));
    }

    return renameRecords(annotated, Maps.newHashMap(), new int[] {1});
  }

  /**
   * Reports fields that {@link #annotate} would reject, without throwing. Useful for logging why a
   * schema version fell back to the decode path.
   */
  public static List<String> unmatchedFields(
      Schema writerSchema, org.apache.iceberg.Schema tableSchema) {
    Schema annotated =
        AvroSchemaUtil.applyNameMapping(writerSchema, MappingUtil.create(tableSchema));
    List<String> unmatched = Lists.newArrayList();
    collectUnmatched(annotated, "", unmatched);
    return unmatched;
  }

  private static void collectUnmatched(Schema schema, String path, List<String> unmatched) {
    switch (schema.getType()) {
      case RECORD:
        for (Schema.Field field : schema.getFields()) {
          String fieldPath = path.isEmpty() ? field.name() : path + "." + field.name();
          if (field.getObjectProp(AvroSchemaUtil.FIELD_ID_PROP) == null) {
            unmatched.add(fieldPath);
          }
          collectUnmatched(field.schema(), fieldPath, unmatched);
        }
        return;

      case UNION:
        // The gate has already established this is a 2-branch option union.
        for (Schema branch : schema.getTypes()) {
          if (branch.getType() != Schema.Type.NULL) {
            collectUnmatched(branch, path, unmatched);
          }
        }
        return;

      case ARRAY:
        if (schema.getObjectProp(AvroSchemaUtil.ELEMENT_ID_PROP) == null) {
          unmatched.add(path + "[] (element-id)");
        }
        collectUnmatched(schema.getElementType(), path + "[]", unmatched);
        return;

      case MAP:
        if (schema.getObjectProp(AvroSchemaUtil.KEY_ID_PROP) == null) {
          unmatched.add(path + "{} (key-id)");
        }
        if (schema.getObjectProp(AvroSchemaUtil.VALUE_ID_PROP) == null) {
          unmatched.add(path + "{} (value-id)");
        }
        collectUnmatched(schema.getValueType(), path + "{}", unmatched);
        return;

      default:
        // primitives carry no ids of their own
    }
  }

  /**
   * Rewrites every record name to {@code r<n>}, preserving structure and all properties. Names are
   * irrelevant to the encoding but a name that resolves to a class on the classpath changes how
   * Iceberg's generic reader materializes rows.
   */
  private static Schema renameRecords(Schema schema, Map<Schema, Schema> rewritten, int[] counter) {
    Schema alreadyDone = rewritten.get(schema);
    if (alreadyDone != null) {
      return alreadyDone;
    }

    switch (schema.getType()) {
      case RECORD:
        {
          Schema renamed =
              Schema.createRecord("r" + counter[0]++, schema.getDoc(), null, schema.isError());
          // Register before recursing so a self-reference resolves; the gate rejects recursion, but
          // this keeps the rewrite total rather than relying on that.
          rewritten.put(schema, renamed);

          List<Schema.Field> fields = Lists.newArrayListWithExpectedSize(schema.getFields().size());
          for (Schema.Field field : schema.getFields()) {
            Schema.Field copy =
                new Schema.Field(
                    field.name(),
                    renameRecords(field.schema(), rewritten, counter),
                    field.doc(),
                    field.defaultVal(),
                    field.order());
            for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
              copy.addProp(prop.getKey(), prop.getValue());
            }
            fields.add(copy);
          }
          renamed.setFields(fields);
          copyProps(schema, renamed);
          return renamed;
        }

      case UNION:
        {
          List<Schema> branches = Lists.newArrayListWithExpectedSize(schema.getTypes().size());
          for (Schema branch : schema.getTypes()) {
            branches.add(renameRecords(branch, rewritten, counter));
          }
          return copyProps(schema, Schema.createUnion(branches));
        }

      case ARRAY:
        return copyProps(
            schema, Schema.createArray(renameRecords(schema.getElementType(), rewritten, counter)));

      case MAP:
        return copyProps(
            schema, Schema.createMap(renameRecords(schema.getValueType(), rewritten, counter)));

      default:
        // Primitives, fixed and enum are immutable and carry no nested records. Enum and fixed are
        // named types, but renaming them is unnecessary: only record names drive class lookup.
        return schema;
    }
  }

  private static Schema copyProps(Schema from, Schema to) {
    for (Map.Entry<String, Object> prop : from.getObjectProps().entrySet()) {
      to.addProp(prop.getKey(), prop.getValue());
    }
    return to;
  }
}
