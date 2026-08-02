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

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

/**
 * The gate is the only thing preventing a schema whose bytes read back as silently wrong data from
 * reaching a table, so each rejection has its own test naming the failure mode it prevents.
 */
public class TestAvroSchemaEligibility {

  private static Schema parse(String fieldsJson) {
    return new Schema.Parser()
        .parse("{\"type\":\"record\",\"name\":\"Event\",\"fields\":[" + fieldsJson + "]}");
  }

  @Test
  public void testAcceptsTheSupportedTypes() {
    Schema schema =
        parse(
            "{\"name\":\"a\",\"type\":\"boolean\"},"
                + "{\"name\":\"b\",\"type\":\"int\"},"
                + "{\"name\":\"c\",\"type\":\"long\"},"
                + "{\"name\":\"d\",\"type\":\"float\"},"
                + "{\"name\":\"e\",\"type\":\"double\"},"
                + "{\"name\":\"f\",\"type\":\"bytes\"},"
                + "{\"name\":\"g\",\"type\":\"string\"},"
                + "{\"name\":\"h\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
                + "{\"name\":\"i\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}},"
                + "{\"name\":\"j\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
                + "{\"name\":\"k\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-nanos\"}},"
                + "{\"name\":\"l\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}},"
                + "{\"name\":\"m\",\"type\":{\"type\":\"fixed\",\"name\":\"d16\",\"size\":8,\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}},"
                + "{\"name\":\"n\",\"type\":{\"type\":\"fixed\",\"name\":\"u\",\"size\":16,\"logicalType\":\"uuid\"}},"
                + "{\"name\":\"o\",\"type\":[\"null\",\"string\"]},"
                + "{\"name\":\"p\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"q\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
                + "{\"name\":\"r\",\"type\":{\"type\":\"map\",\"values\":\"long\"}},"
                + "{\"name\":\"s\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                + "[{\"name\":\"x\",\"type\":\"int\"}]}},"
                + "{\"name\":\"t\",\"type\":\"int\",\"default\":42}");

    AvroSchemaEligibility.Result result = AvroSchemaEligibility.check(schema);

    assertThat(result.writeBlockers()).isEmpty();
    assertThat(result.writable()).isTrue();
    assertThat(result.partitionable()).isTrue();
  }

  @Test
  public void testBothNullUnionOrderingsAreAccepted() {
    // Iceberg builds union readers in file-schema branch order and dispatches on the decoded index,
    // so null-second is as valid as null-first.
    assertThat(
            AvroSchemaEligibility.check(parse("{\"name\":\"a\",\"type\":[\"null\",\"long\"]}"))
                .writable())
        .isTrue();
    assertThat(
            AvroSchemaEligibility.check(parse("{\"name\":\"a\",\"type\":[\"long\",\"null\"]}"))
                .writable())
        .isTrue();
  }

  @Test
  public void testRejectsEnum() {
    AvroSchemaEligibility.Result result =
        AvroSchemaEligibility.check(
            parse(
                "{\"name\":\"a\",\"type\":{\"type\":\"enum\",\"name\":\"E\","
                    + "\"symbols\":[\"X\",\"Y\"]}}"));

    assertThat(result.writable()).isFalse();
    assertThat(result.writeBlockers()).singleElement().asString().contains("zigzag ordinal");
  }

  @Test
  public void testRejectsStringBackedUuid() {
    // The nastiest case: no error at write or read time, just wrong values and a desynced stream.
    AvroSchemaEligibility.Result result =
        AvroSchemaEligibility.check(
            parse("{\"name\":\"a\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}"));

    assertThat(result.writable()).isFalse();
    assertThat(result.writeBlockers()).singleElement().asString().contains("16 raw bytes");
  }

  @Test
  public void testRejectsTimestampMillis() {
    AvroSchemaEligibility.Result result =
        AvroSchemaEligibility.check(
            parse(
                "{\"name\":\"a\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}"));

    assertThat(result.writable()).isFalse();
    assertThat(result.writeBlockers()).singleElement().asString().contains("1000x wrong");
  }

  @Test
  public void testRejectsTimeMillisAndLocalTimestamps() {
    assertThat(
            AvroSchemaEligibility.check(
                    parse(
                        "{\"name\":\"a\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}}"))
                .writable())
        .isFalse();

    for (String logical :
        new String[] {
          "local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos"
        }) {
      AvroSchemaEligibility.Result result =
          AvroSchemaEligibility.check(
              parse(
                  "{\"name\":\"a\",\"type\":{\"type\":\"long\",\"logicalType\":\""
                      + logical
                      + "\"}}"));
      assertThat(result.writable()).as(logical).isFalse();
    }
  }

  @Test
  public void testRejectsMultiBranchAndNonOptionUnions() {
    AvroSchemaEligibility.Result threeBranch =
        AvroSchemaEligibility.check(
            parse("{\"name\":\"a\",\"type\":[\"null\",\"string\",\"long\"]}"));
    assertThat(threeBranch.writable()).isFalse();
    assertThat(threeBranch.writeBlockers()).singleElement().asString().contains("3 branches");

    AvroSchemaEligibility.Result noNull =
        AvroSchemaEligibility.check(parse("{\"name\":\"a\",\"type\":[\"string\",\"long\"]}"));
    assertThat(noNull.writable()).isFalse();
    assertThat(noNull.writeBlockers()).singleElement().asString().contains("neither is null");
  }

  @Test
  public void testRejectsRecursiveRecord() {
    Schema recursive =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
                    + "{\"name\":\"value\",\"type\":\"int\"},"
                    + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"]}]}");

    AvroSchemaEligibility.Result result = AvroSchemaEligibility.check(recursive);

    assertThat(result.writable()).isFalse();
    assertThat(result.writeBlockers()).singleElement().asString().contains("recursive");
  }

  @Test
  public void testRejectsReusedNamedType() {
    // One set of injected field ids cannot serve two positions in the tree.
    Schema reused =
        parse(
            "{\"name\":\"a\",\"type\":{\"type\":\"record\",\"name\":\"Pair\",\"fields\":"
                + "[{\"name\":\"x\",\"type\":\"int\"}]}},"
                + "{\"name\":\"b\",\"type\":\"Pair\"}");

    AvroSchemaEligibility.Result result = AvroSchemaEligibility.check(reused);

    assertThat(result.writable()).isFalse();
    assertThat(result.writeBlockers()).singleElement().asString().contains("globally unique");
  }

  @Test
  public void testPlainFixedIsWritableButNotPartitionable() {
    // GenericAvroReader reads fixed(N) correctly, but InternalReader -- which partition extraction
    // uses -- skips it as length-prefixed bytes and desyncs.
    AvroSchemaEligibility.Result result =
        AvroSchemaEligibility.check(
            parse("{\"name\":\"a\",\"type\":{\"type\":\"fixed\",\"name\":\"F\",\"size\":12}}"));

    assertThat(result.writable()).isTrue();
    assertThat(result.partitionable()).isFalse();
    assertThat(result.partitionBlockers()).singleElement().asString().contains("desynchronize");
  }

  @Test
  public void testReportsEveryBlockerNotJustTheFirst() {
    AvroSchemaEligibility.Result result =
        AvroSchemaEligibility.check(
            parse(
                "{\"name\":\"a\",\"type\":{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"X\"]}},"
                    + "{\"name\":\"b\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
                    + "{\"name\":\"c\",\"type\":[\"null\",\"string\",\"long\"]}"));

    assertThat(result.writeBlockers()).hasSize(3);
    assertThat(result.explain()).contains("not writable");
  }

  @Test
  public void testFindsBlockersNestedInsideCollectionsAndRecords() {
    // Unprojected fields still need a working skip-reader, so a bad type anywhere is fatal.
    assertThat(
            AvroSchemaEligibility.check(
                    parse(
                        "{\"name\":\"a\",\"type\":{\"type\":\"array\",\"items\":"
                            + "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"X\"]}}}"))
                .writable())
        .as("inside an array")
        .isFalse();

    assertThat(
            AvroSchemaEligibility.check(
                    parse(
                        "{\"name\":\"a\",\"type\":{\"type\":\"map\",\"values\":"
                            + "{\"type\":\"string\",\"logicalType\":\"uuid\"}}}"))
                .writable())
        .as("inside a map")
        .isFalse();

    assertThat(
            AvroSchemaEligibility.check(
                    parse(
                        "{\"name\":\"a\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":"
                            + "[{\"name\":\"x\",\"type\":{\"type\":\"long\","
                            + "\"logicalType\":\"timestamp-millis\"}}]}}"))
                .writable())
        .as("inside a nested record")
        .isFalse();

    assertThat(
            AvroSchemaEligibility.check(
                    parse(
                        "{\"name\":\"a\",\"type\":[\"null\","
                            + "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"X\"]}]}"))
                .writable())
        .as("inside an optional union")
        .isFalse();
  }
}
