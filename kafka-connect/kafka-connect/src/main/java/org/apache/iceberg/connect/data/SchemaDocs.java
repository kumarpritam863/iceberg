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
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Makes field docs safe for catalogs that surface them as column comments.
 *
 * <p>Deriving a table schema from an Avro writer schema carries the Avro {@code doc} strings into
 * Iceberg field docs ({@code SchemaToType} does this), and some catalogs then publish those as
 * column comments. AWS Glue rejects a comment containing a newline — its {@code Column.Comment}
 * must match {@code [ -퟿-�က0-ჿFF\t]*}, which admits tab but not {@code \n} or {@code \r}.
 * Multi-line docs are completely ordinary in hand-written Avro schemas, so an unsanitized doc fails
 * table creation outright:
 *
 * <pre>
 *   ValidationException: Value 'keys:
 *        - "model_server_version"' at 'table.storageDescriptor.columns.5.member.comment'
 *        failed to satisfy constraint: Member must satisfy regular expression pattern: ...
 * </pre>
 *
 * <p>Rather than drop the docs — they are genuinely useful as column comments — collapse each run
 * of whitespace into a single space and trim. That keeps the text readable and satisfies the
 * constraint.
 *
 * <p>The Connect-schema path never set docs at all, so this only matters for schemas derived from
 * Avro.
 */
class SchemaDocs {

  /**
   * Glue's documented limit for {@code Column.Comment}. Truncating here is belt-and-braces: the
   * observed failure was the newline pattern, not length, but a long doc would fail the same call
   * and is cheaper to prevent than to diagnose against a remote catalog.
   */
  private static final int MAX_DOC_LENGTH = 255;

  private SchemaDocs() {}

  /**
   * Returns {@code schema} with every field doc normalized to a single line.
   *
   * <p>Intended for a schema freshly produced by {@code AvroSchemaUtil.toIceberg}, which carries no
   * field defaults — only ids, names, types, nullability and docs are preserved.
   */
  static Schema sanitize(Schema schema) {
    return new Schema(sanitizeStruct(schema.asStruct()).fields());
  }

  private static Types.StructType sanitizeStruct(Types.StructType struct) {
    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(struct.fields().size());
    for (Types.NestedField field : struct.fields()) {
      Type sanitizedType = sanitizeType(field.type());
      String doc = oneLine(field.doc());
      fields.add(
          field.isRequired()
              ? Types.NestedField.required(field.fieldId(), field.name(), sanitizedType, doc)
              : Types.NestedField.optional(field.fieldId(), field.name(), sanitizedType, doc));
    }
    return Types.StructType.of(fields);
  }

  private static Type sanitizeType(Type type) {
    if (type.isStructType()) {
      return sanitizeStruct(type.asStructType());
    }

    if (type.isListType()) {
      Types.ListType list = type.asListType();
      Type element = sanitizeType(list.elementType());
      return list.isElementOptional()
          ? Types.ListType.ofOptional(list.elementId(), element)
          : Types.ListType.ofRequired(list.elementId(), element);
    }

    if (type.isMapType()) {
      Types.MapType map = type.asMapType();
      Type key = sanitizeType(map.keyType());
      Type value = sanitizeType(map.valueType());
      return map.isValueOptional()
          ? Types.MapType.ofOptional(map.keyId(), map.valueId(), key, value)
          : Types.MapType.ofRequired(map.keyId(), map.valueId(), key, value);
    }

    return type;
  }

  private static String oneLine(String doc) {
    if (doc == null) {
      return null;
    }

    String collapsed = doc.replaceAll("\\s+", " ").trim();
    if (collapsed.isEmpty()) {
      return null;
    }

    return collapsed.length() <= MAX_DOC_LENGTH
        ? collapsed
        : collapsed.substring(0, MAX_DOC_LENGTH - 3) + "...";
  }
}
