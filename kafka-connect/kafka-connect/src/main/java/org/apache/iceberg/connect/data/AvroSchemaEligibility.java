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
import java.util.Set;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * Decides whether an Avro writer schema's encoded bytes can be appended into an Iceberg Avro data
 * file unchanged.
 *
 * <p>This is a <b>hard allowlist</b>, deliberately. Iceberg's Avro reader accepts a foreign schema
 * and resolves fields by ID at read time, which makes byte-level passthrough possible — but a
 * handful of constructs read back as <em>silently wrong data</em> with no error at write time and
 * no error at read time. {@code DataFileWriter.appendEncoded} performs no validation whatsoever
 * ("Appending non-conforming data may result in an unreadable file"), so this class is the only
 * thing standing between a bad schema and corrupt table data. Anything not explicitly recognised is
 * rejected.
 *
 * <p>The whole schema is walked, not just the columns that map to table fields: unprojected fields
 * still need a working skip-reader, so an unrepresentable type anywhere in the record is fatal.
 *
 * <h2>Rejections and why</h2>
 *
 * <ul>
 *   <li>{@code enum} — encodes as a bare zigzag ordinal; Iceberg reads a string as length-prefixed
 *       UTF-8. Reading one as the other desynchronizes the stream.
 *   <li>{@code string} with {@code logicalType=uuid} — Iceberg maps the logical type to its UUID
 *       type and reads {@code readFixed(16)}, but the bytes are a length-prefixed 36-character
 *       string. Silent corruption plus desync.
 *   <li>{@code long} with {@code logicalType=timestamp-millis} — layout matches Iceberg's
 *       microsecond timestamp exactly, but readers multiply by 1000. Values come back 1000x wrong,
 *       silently. Iceberg has no millisecond timestamp type.
 *   <li>{@code int} with {@code logicalType=time-millis} and all {@code local-timestamp-*} — every
 *       Iceberg reader throws {@code Unknown logical type}. A hard crash, not a graceful skip.
 *   <li>Unions with three or more branches, or two branches where neither is {@code null} — {@code
 *       AvroWithPartnerVisitor.visitUnion} rejects them when the reader is <em>built</em>, before a
 *       byte is read, even for fields that only need skipping.
 *   <li>Recursive or self-referential records — every Iceberg Avro visitor throws {@code Cannot
 *       process recursive Avro record}.
 *   <li>A named type referenced more than once — Iceberg field IDs must be globally unique, so one
 *       set of injected IDs cannot serve two positions in the tree.
 * </ul>
 *
 * <p>Note that {@code decimal} backed by either {@code bytes} or {@code fixed} is fine (Iceberg
 * dispatches on the actual Avro type), and so is {@code fixed(16)} with {@code logicalType=uuid},
 * which is Iceberg's own canonical UUID encoding.
 *
 * <h2>Writable versus partitionable</h2>
 *
 * <p>A schema can be safe to write raw but unsafe to extract partition values from. Partition
 * extraction uses {@code InternalReader}, which maps {@code FIXED} and {@code BYTES} to the same
 * length-prefixed reader; a plain {@code fixed(N)} (no logical type) would then be skipped as
 * though it carried a length prefix, desynchronizing the stream. So {@link Result#partitionable()}
 * is reported separately from {@link Result#writable()}.
 */
public class AvroSchemaEligibility {

  private static final Set<String> ALLOWED_LOGICAL_TYPES =
      ImmutableSet.of(
          "date", "time-micros", "timestamp-micros", "timestamp-nanos", "decimal", "uuid");

  private AvroSchemaEligibility() {}

  /** The outcome of a check, carrying every reason rather than only the first. */
  public static class Result {
    private final List<String> writeBlockers;
    private final List<String> partitionBlockers;

    private Result(List<String> writeBlockers, List<String> partitionBlockers) {
      this.writeBlockers = ImmutableList.copyOf(writeBlockers);
      this.partitionBlockers = ImmutableList.copyOf(partitionBlockers);
    }

    /** True when the encoded bytes can be appended into an Iceberg Avro file unchanged. */
    public boolean writable() {
      return writeBlockers.isEmpty();
    }

    /**
     * True when partition-source values can additionally be extracted from the bytes. Implies
     * {@link #writable()}.
     */
    public boolean partitionable() {
      return writable() && partitionBlockers.isEmpty();
    }

    public List<String> writeBlockers() {
      return writeBlockers;
    }

    public List<String> partitionBlockers() {
      return partitionBlockers;
    }

    /** A single message listing every blocker, for logs and exceptions. */
    public String explain() {
      if (writable() && partitionable()) {
        return "eligible for raw passthrough and partition extraction";
      }
      StringBuilder sb = new StringBuilder();
      if (!writable()) {
        sb.append("not writable via raw passthrough: ").append(String.join("; ", writeBlockers));
      }
      if (!partitionBlockers.isEmpty()) {
        if (sb.length() > 0) {
          sb.append(" | ");
        }
        sb.append("not partitionable: ").append(String.join("; ", partitionBlockers));
      }
      return sb.toString();
    }
  }

  /**
   * Walks {@code schema} and reports every reason it cannot be used for raw passthrough.
   *
   * @param schema an Avro writer schema, expected to be a record
   */
  public static Result check(Schema schema) {
    List<String> writeBlockers = Lists.newArrayList();
    List<String> partitionBlockers = Lists.newArrayList();
    // Named types must appear exactly once, so track them across the whole walk rather than per
    // branch. `visiting` catches recursion; `seenNames` catches reuse.
    visit(schema, "", Sets.newHashSet(), Maps.newHashMap(), writeBlockers, partitionBlockers);
    return new Result(writeBlockers, partitionBlockers);
  }

  private static void visit(
      Schema schema,
      String path,
      Set<String> visiting,
      Map<String, String> seenNames,
      List<String> writeBlockers,
      List<String> partitionBlockers) {
    switch (schema.getType()) {
      case RECORD:
        visitRecord(schema, path, visiting, seenNames, writeBlockers, partitionBlockers);
        return;

      case UNION:
        visitUnion(schema, path, visiting, seenNames, writeBlockers, partitionBlockers);
        return;

      case ARRAY:
        visit(
            schema.getElementType(),
            path + "[]",
            visiting,
            seenNames,
            writeBlockers,
            partitionBlockers);
        return;

      case MAP:
        // Avro map keys are always strings, which is what Iceberg requires; only the value needs
        // checking.
        visit(
            schema.getValueType(),
            path + "{}",
            visiting,
            seenNames,
            writeBlockers,
            partitionBlockers);
        return;

      case ENUM:
        writeBlockers.add(
            at(path)
                + "enum '"
                + schema.getFullName()
                + "' encodes as a zigzag ordinal, which Iceberg reads as a length-prefixed string");
        return;

      default:
        visitPrimitive(schema, path, writeBlockers, partitionBlockers);
    }
  }

  private static void visitRecord(
      Schema record,
      String path,
      Set<String> visiting,
      Map<String, String> seenNames,
      List<String> writeBlockers,
      List<String> partitionBlockers) {
    String name = record.getFullName();

    if (visiting.contains(name)) {
      writeBlockers.add(
          at(path)
              + "record '"
              + name
              + "' is recursive; every Iceberg Avro visitor rejects recursive schemas");
      return;
    }

    String firstSeenAt = seenNames.get(name);
    if (firstSeenAt != null) {
      writeBlockers.add(
          at(path)
              + "record '"
              + name
              + "' is also used at '"
              + firstSeenAt
              + "'; Iceberg field IDs must be globally unique, so a reused named type cannot be"
              + " annotated");
      return;
    }
    seenNames.put(name, path.isEmpty() ? "<root>" : path);

    visiting.add(name);
    for (Schema.Field field : record.getFields()) {
      visit(
          field.schema(),
          path.isEmpty() ? field.name() : path + "." + field.name(),
          visiting,
          seenNames,
          writeBlockers,
          partitionBlockers);
    }
    visiting.remove(name);
  }

  private static void visitUnion(
      Schema union,
      String path,
      Set<String> visiting,
      Map<String, String> seenNames,
      List<String> writeBlockers,
      List<String> partitionBlockers) {
    List<Schema> branches = union.getTypes();
    if (branches.size() != 2) {
      writeBlockers.add(
          at(path)
              + "union has "
              + branches.size()
              + " branches; Iceberg only supports 2-branch unions containing null, and rejects"
              + " others when the reader is built -- even for fields it would only skip");
      return;
    }

    boolean nullFirst = branches.get(0).getType() == Schema.Type.NULL;
    boolean nullSecond = branches.get(1).getType() == Schema.Type.NULL;
    if (!nullFirst && !nullSecond) {
      writeBlockers.add(
          at(path)
              + "union has two branches but neither is null; Iceberg rejects non-option unions");
      return;
    }

    // Either branch order is fine: readers dispatch on the index decoded from the file, and the
    // reader array is built in file-schema order.
    visit(
        nullFirst ? branches.get(1) : branches.get(0),
        path,
        visiting,
        seenNames,
        writeBlockers,
        partitionBlockers);
  }

  private static void visitPrimitive(
      Schema primitive, String path, List<String> writeBlockers, List<String> partitionBlockers) {
    LogicalType logicalType = primitive.getLogicalType();
    String logicalName = logicalType == null ? null : logicalType.getName();

    // Avro parses only logical types it knows; anything else survives as a bare string property, so
    // check both. local-timestamp-* in particular is parsed by Avro but unknown to Iceberg.
    if (logicalName == null) {
      Object raw = primitive.getObjectProp("logicalType");
      if (raw instanceof String) {
        logicalName = (String) raw;
      }
    }

    if (logicalName != null) {
      switch (logicalName) {
        case "timestamp-millis":
          writeBlockers.add(
              at(path)
                  + "timestamp-millis has the same layout as Iceberg's timestamp but readers"
                  + " multiply by 1000; values would come back 1000x wrong with no error");
          return;

        case "time-millis":
          writeBlockers.add(
              at(path)
                  + "time-millis is not handled by any Iceberg reader; it throws at read time");
          return;

        case "local-timestamp-millis":
        case "local-timestamp-micros":
        case "local-timestamp-nanos":
          writeBlockers.add(
              at(path)
                  + logicalName
                  + " is silently degraded to a bare long by schema conversion and then throws"
                  + " 'Unknown logical type' at read time");
          return;

        case "uuid":
          if (primitive.getType() == Schema.Type.STRING) {
            writeBlockers.add(
                at(path)
                    + "string with logicalType=uuid: Iceberg reads a UUID as 16 raw bytes but the"
                    + " payload holds a length-prefixed 36-character string -- silent corruption and"
                    + " stream desync");
            return;
          }
          if (primitive.getType() == Schema.Type.FIXED && primitive.getFixedSize() != 16) {
            writeBlockers.add(
                at(path)
                    + "fixed("
                    + primitive.getFixedSize()
                    + ") with logicalType=uuid: Iceberg reads exactly 16 bytes");
            return;
          }
          break;

        default:
          if (!ALLOWED_LOGICAL_TYPES.contains(logicalName)) {
            writeBlockers.add(
                at(path) + "unrecognised logicalType '" + logicalName + "'; not on the allowlist");
            return;
          }
      }
    }

    switch (primitive.getType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
      case NULL:
        return;

      case FIXED:
        if (logicalName == null) {
          // Writable: Iceberg's GenericAvroReader reads fixed(N) as N raw bytes, correctly. But
          // InternalReader -- which partition extraction uses -- maps FIXED to the same reader as
          // BYTES, whose skip reads a length prefix that is not there.
          partitionBlockers.add(
              at(path)
                  + "plain fixed("
                  + primitive.getFixedSize()
                  + ") is skipped as length-prefixed bytes by InternalReader, which would"
                  + " desynchronize partition extraction");
        }
        return;

      default:
        writeBlockers.add(at(path) + "unsupported Avro type " + primitive.getType());
    }
  }

  private static String at(String path) {
    return path.isEmpty() ? "<root>: " : "'" + path + "': ";
  }
}
