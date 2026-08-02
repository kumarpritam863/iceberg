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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.avro.LogicalTypes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

public class TestAvroSchemaAnnotator {

  private static final Schema TABLE_SCHEMA =
      new Schema(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.optional(2, "name", Types.StringType.get()),
          NestedField.required(3, "event_ts", Types.TimestampType.withZone()),
          NestedField.required(4, "tags", Types.ListType.ofRequired(20, Types.StringType.get())),
          NestedField.required(
              5,
              "attrs",
              Types.MapType.ofRequired(21, 22, Types.StringType.get(), Types.StringType.get())),
          NestedField.required(
              6,
              "payload",
              Types.StructType.of(NestedField.required(23, "inner", Types.StringType.get()))));

  private static org.apache.avro.Schema producerSchema() {
    return new org.apache.avro.Schema.Parser()
        .parse(
            "{\"type\":\"record\",\"name\":\"MyEvent\",\"namespace\":\"com.example\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"long\"},"
                + "{\"name\":\"name\",\"type\":[\"null\",\"string\"]},"
                + "{\"name\":\"event_ts\",\"type\":{\"type\":\"long\","
                + "\"logicalType\":\"timestamp-micros\",\"adjust-to-utc\":true}},"
                + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
                + "{\"name\":\"attrs\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
                + "{\"name\":\"payload\",\"type\":{\"type\":\"record\",\"name\":\"Payload\","
                + "\"fields\":[{\"name\":\"inner\",\"type\":\"string\"}]}}]}");
  }

  @Test
  public void testAssignsTableFieldIdsByName() {
    org.apache.avro.Schema annotated = AvroSchemaAnnotator.annotate(producerSchema(), TABLE_SCHEMA);

    assertThat(annotated.getField("id").getObjectProp(AvroSchemaUtil.FIELD_ID_PROP)).isEqualTo(1);
    assertThat(annotated.getField("name").getObjectProp(AvroSchemaUtil.FIELD_ID_PROP)).isEqualTo(2);
    assertThat(annotated.getField("event_ts").getObjectProp(AvroSchemaUtil.FIELD_ID_PROP))
        .isEqualTo(3);
    assertThat(annotated.getField("tags").getObjectProp(AvroSchemaUtil.FIELD_ID_PROP)).isEqualTo(4);
    assertThat(annotated.getField("attrs").getObjectProp(AvroSchemaUtil.FIELD_ID_PROP))
        .isEqualTo(5);

    // Collections carry their own ids, which the reader needs just as much as the field ids.
    assertThat(annotated.getField("tags").schema().getObjectProp(AvroSchemaUtil.ELEMENT_ID_PROP))
        .isEqualTo(20);
    assertThat(annotated.getField("attrs").schema().getObjectProp(AvroSchemaUtil.KEY_ID_PROP))
        .isEqualTo(21);
    assertThat(annotated.getField("attrs").schema().getObjectProp(AvroSchemaUtil.VALUE_ID_PROP))
        .isEqualTo(22);

    // Nested records too.
    assertThat(
            annotated
                .getField("payload")
                .schema()
                .getField("inner")
                .getObjectProp(AvroSchemaUtil.FIELD_ID_PROP))
        .isEqualTo(23);
  }

  @Test
  public void testPreservesLogicalTypesAndUnionOrder() {
    org.apache.avro.Schema annotated = AvroSchemaAnnotator.annotate(producerSchema(), TABLE_SCHEMA);

    org.apache.avro.Schema ts = annotated.getField("event_ts").schema();
    assertThat(ts.getLogicalType()).isInstanceOf(LogicalTypes.TimestampMicros.class);
    assertThat(ts.getObjectProp("adjust-to-utc")).isEqualTo(Boolean.TRUE);

    // The wire layout depends on branch order, so annotation must not reorder it.
    org.apache.avro.Schema name = annotated.getField("name").schema();
    assertThat(name.getType()).isEqualTo(org.apache.avro.Schema.Type.UNION);
    assertThat(name.getTypes().get(0).getType()).isEqualTo(org.apache.avro.Schema.Type.NULL);
    assertThat(name.getTypes().get(1).getType()).isEqualTo(org.apache.avro.Schema.Type.STRING);
  }

  @Test
  public void testRewritesRecordNamesSoNoClassLookupCanSucceed() {
    org.apache.avro.Schema annotated = AvroSchemaAnnotator.annotate(producerSchema(), TABLE_SCHEMA);

    // GenericAvroReader tries to load the record's full name as a class; a registry-generated
    // SpecificRecord name on the worker classpath would change how rows materialize.
    assertThat(annotated.getFullName()).isNotEqualTo("com.example.MyEvent");
    assertThat(annotated.getFullName()).matches("r\\d+");
    assertThat(annotated.getField("payload").schema().getFullName()).matches("r\\d+");
  }

  @Test
  public void testFieldOrderAndTypesAreUnchanged() {
    org.apache.avro.Schema producer = producerSchema();
    org.apache.avro.Schema annotated = AvroSchemaAnnotator.annotate(producer, TABLE_SCHEMA);

    // Byte layout is determined entirely by declaration order and types, so annotation must be
    // layout-neutral or every payload written against the producer schema becomes garbage.
    assertThat(annotated.getFields()).hasSameSizeAs(producer.getFields());
    for (int i = 0; i < producer.getFields().size(); i++) {
      assertThat(annotated.getFields().get(i).name()).isEqualTo(producer.getFields().get(i).name());
      assertThat(annotated.getFields().get(i).schema().getType())
          .isEqualTo(producer.getFields().get(i).schema().getType());
    }
  }

  @Test
  public void testRejectsSchemaWithAFieldTheTableDoesNotHave() {
    org.apache.avro.Schema extraField =
        new org.apache.avro.Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"E\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"not_in_table\",\"type\":\"string\"}]}");

    // Partial annotation is worse than none: one id anywhere disables the reader's name-mapping
    // fallback for the whole file, so unannotated columns would read back as silent nulls.
    assertThatThrownBy(() -> AvroSchemaAnnotator.annotate(extraField, TABLE_SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not_in_table")
        .hasMessageContaining("Partial annotation is unsafe");
  }

  @Test
  public void testUnmatchedFieldsReportsWithoutThrowing() {
    org.apache.avro.Schema extraField =
        new org.apache.avro.Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"E\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"},"
                    + "{\"name\":\"nope\",\"type\":\"string\"}]}");

    assertThat(AvroSchemaAnnotator.unmatchedFields(extraField, TABLE_SCHEMA))
        .containsExactly("nope");
    assertThat(AvroSchemaAnnotator.unmatchedFields(producerSchema(), TABLE_SCHEMA)).isEmpty();
  }

  @Test
  public void testSchemaMissingTableColumnsIsStillAnnotatable() {
    // A producer on an older schema version simply omits newer columns; the reader null-fills them.
    org.apache.avro.Schema subset =
        new org.apache.avro.Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"E\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"}]}");

    org.apache.avro.Schema annotated = AvroSchemaAnnotator.annotate(subset, TABLE_SCHEMA);

    assertThat(annotated.getFields()).hasSize(1);
    assertThat(annotated.getField("id").getObjectProp(AvroSchemaUtil.FIELD_ID_PROP)).isEqualTo(1);
  }
}
