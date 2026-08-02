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

import java.util.regex.Pattern;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.junit.jupiter.api.Test;

/**
 * Avro {@code doc} strings become Iceberg field docs, which some catalogs publish as column
 * comments. Glue rejects a comment containing a newline, and multi-line docs are ordinary in
 * hand-written Avro, so an unsanitized doc fails table creation against a real catalog.
 */
public class TestSchemaDocs {

  /** Glue's constraint on {@code Column.Comment}: printable plus tab, but no newline. */
  private static final Pattern GLUE_COMMENT =
      Pattern.compile("[\\u0020-\\uD7FF\\uE000-\\uFFFD\\t]*");

  private static Schema fromAvro(String fieldsJson) {
    return AvroSchemaUtil.toIceberg(
        new org.apache.avro.Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"E\",\"fields\":[" + fieldsJson + "]}"));
  }

  @Test
  public void testCollapsesMultiLineDocsToOneLine() {
    // Verbatim shapes from the Glue failure this exists to prevent.
    Schema sanitized =
        SchemaDocs.sanitize(
            fromAvro(
                "{\"name\":\"a\",\"type\":\"long\",\"doc\":\"keys:\\n     - \\\"model_server_version\\\"\"},"
                    + "{\"name\":\"b\",\"type\":\"string\",\"doc\":\"Features that are common across"
                    + " candidates -- equiv to commonFeatures\\n        e.g. request level features\"}"));

    assertThat(sanitized.findField("a").doc())
        .isEqualTo("keys: - \"model_server_version\"")
        .matches(GLUE_COMMENT.asMatchPredicate());
    assertThat(sanitized.findField("b").doc())
        .isEqualTo(
            "Features that are common across candidates -- equiv to commonFeatures e.g. request"
                + " level features")
        .matches(GLUE_COMMENT.asMatchPredicate());
  }

  @Test
  public void testEveryDocSatisfiesGlueAfterSanitizing() {
    Schema sanitized =
        SchemaDocs.sanitize(
            fromAvro(
                "{\"name\":\"a\",\"type\":\"long\",\"doc\":\"line one\\nline two\\r\\nline three\"},"
                    + "{\"name\":\"b\",\"type\":\"string\",\"doc\":\"tabbed\\tand\\nnewlined\"},"
                    + "{\"name\":\"c\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                    + "[{\"name\":\"x\",\"type\":\"int\",\"doc\":\"nested\\ndoc\"}]}}"));

    for (org.apache.iceberg.types.Types.NestedField field : sanitized.columns()) {
      if (field.doc() != null) {
        assertThat(field.doc()).matches(GLUE_COMMENT.asMatchPredicate());
      }
    }
    assertThat(sanitized.findField("c.x").doc()).isEqualTo("nested doc");
  }

  @Test
  public void testStructureAndTypesAreUnchanged() {
    // Sanitizing must not perturb ids, names, nullability or types -- the byte layout the raw path
    // depends on is derived from these.
    Schema original =
        fromAvro(
            "{\"name\":\"a\",\"type\":\"long\",\"doc\":\"multi\\nline\"},"
                + "{\"name\":\"b\",\"type\":[\"null\",\"string\"]},"
                + "{\"name\":\"c\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
                + "{\"name\":\"d\",\"type\":{\"type\":\"map\",\"values\":\"long\"}},"
                + "{\"name\":\"e\",\"type\":{\"type\":\"long\","
                + "\"logicalType\":\"timestamp-micros\",\"adjust-to-utc\":true}}");

    Schema sanitized = SchemaDocs.sanitize(original);

    assertThat(sanitized.asStruct().fields()).hasSameSizeAs(original.asStruct().fields());
    for (org.apache.iceberg.types.Types.NestedField before : original.columns()) {
      org.apache.iceberg.types.Types.NestedField after = sanitized.findField(before.fieldId());
      assertThat(after).isNotNull();
      assertThat(after.name()).isEqualTo(before.name());
      assertThat(after.type()).isEqualTo(before.type());
      assertThat(after.isRequired()).isEqualTo(before.isRequired());
    }
  }

  @Test
  public void testAbsentAndBlankDocsStayNull() {
    Schema sanitized =
        SchemaDocs.sanitize(
            fromAvro(
                "{\"name\":\"a\",\"type\":\"long\"},"
                    + "{\"name\":\"b\",\"type\":\"long\",\"doc\":\"   \\n  \"}"));

    assertThat(sanitized.findField("a").doc()).isNull();
    assertThat(sanitized.findField("b").doc()).as("whitespace-only doc is not a comment").isNull();
  }

  @Test
  public void testOverlongDocIsTruncated() {
    String longDoc = "x".repeat(400);
    Schema sanitized =
        SchemaDocs.sanitize(
            fromAvro("{\"name\":\"a\",\"type\":\"long\",\"doc\":\"" + longDoc + "\"}"));

    assertThat(sanitized.findField("a").doc()).hasSize(255).endsWith("...");
  }
}
