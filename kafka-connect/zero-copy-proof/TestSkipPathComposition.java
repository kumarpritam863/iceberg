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
package org.apache.iceberg.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestScratchSkipClaim {

  /** Counting decoder that records which Decoder methods are actually invoked. */
  static class CountingDecoder extends Decoder {
    private final Decoder inner;
    final Map<String, Integer> counts = new TreeMap<>();

    CountingDecoder(Decoder inner) {
      this.inner = inner;
    }

    private void inc(String name) {
      counts.merge(name, 1, Integer::sum);
    }

    @Override
    public void readNull() throws IOException {
      inc("readNull");
      inner.readNull();
    }

    @Override
    public boolean readBoolean() throws IOException {
      inc("readBoolean");
      return inner.readBoolean();
    }

    @Override
    public int readInt() throws IOException {
      inc("readInt");
      return inner.readInt();
    }

    @Override
    public long readLong() throws IOException {
      inc("readLong");
      return inner.readLong();
    }

    @Override
    public float readFloat() throws IOException {
      inc("readFloat");
      return inner.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
      inc("readDouble");
      return inner.readDouble();
    }

    @Override
    public Utf8 readString(Utf8 old) throws IOException {
      inc("readString(Utf8)");
      return inner.readString(old);
    }

    @Override
    public String readString() throws IOException {
      inc("readString()");
      return inner.readString();
    }

    @Override
    public void skipString() throws IOException {
      inc("skipString");
      inner.skipString();
    }

    @Override
    public ByteBuffer readBytes(ByteBuffer old) throws IOException {
      inc("readBytes");
      return inner.readBytes(old);
    }

    @Override
    public void skipBytes() throws IOException {
      inc("skipBytes");
      inner.skipBytes();
    }

    @Override
    public void readFixed(byte[] bytes, int start, int length) throws IOException {
      inc("readFixed");
      inner.readFixed(bytes, start, length);
    }

    @Override
    public void skipFixed(int length) throws IOException {
      inc("skipFixed");
      inner.skipFixed(length);
    }

    @Override
    public int readEnum() throws IOException {
      inc("readEnum");
      return inner.readEnum();
    }

    @Override
    public long readArrayStart() throws IOException {
      inc("readArrayStart");
      return inner.readArrayStart();
    }

    @Override
    public long arrayNext() throws IOException {
      inc("arrayNext");
      return inner.arrayNext();
    }

    @Override
    public long skipArray() throws IOException {
      inc("skipArray");
      return inner.skipArray();
    }

    @Override
    public long readMapStart() throws IOException {
      inc("readMapStart");
      return inner.readMapStart();
    }

    @Override
    public long mapNext() throws IOException {
      inc("mapNext");
      return inner.mapNext();
    }

    @Override
    public long skipMap() throws IOException {
      inc("skipMap");
      return inner.skipMap();
    }

    @Override
    public int readIndex() throws IOException {
      inc("readIndex");
      return inner.readIndex();
    }
  }

  private static Schema.Field idField(String name, Schema type, int id) {
    Schema.Field f = new Schema.Field(name, type, null, null);
    f.addProp(AvroSchemaUtil.FIELD_ID_PROP, id);
    return f;
  }

  /** wide record: 6 "before" fields of various shapes, then the partition field. */
  private static Schema wideSchema(boolean withIds) {
    Schema optString = Schema.createUnion(Schema.create(Schema.Type.NULL), stringSchema());
    Schema nested =
        Schema.createRecord(
            "nested",
            null,
            "x",
            false,
            Lists.newArrayList(
                mk("n1", stringSchema(), 101, withIds), mk("n2", longSchema(), 102, withIds)));
    Schema arr = Schema.createArray(stringSchema());
    if (withIds) {
      arr.addProp(AvroSchemaUtil.ELEMENT_ID_PROP, 201);
    }
    Schema map = Schema.createMap(stringSchema());
    if (withIds) {
      map.addProp(AvroSchemaUtil.KEY_ID_PROP, 301);
      map.addProp(AvroSchemaUtil.VALUE_ID_PROP, 302);
    }

    List<Schema.Field> fields =
        Lists.newArrayList(
            mk("s1", stringSchema(), 1, withIds),
            mk("s2", stringSchema(), 2, withIds),
            mk("opt_s", optString, 3, withIds),
            mk("nested_rec", nested, 4, withIds),
            mk("arr", arr, 5, withIds),
            mk("map", map, 6, withIds),
            mk("part_src", stringSchema(), 7, withIds),
            mk("tail", longSchema(), 8, withIds));
    return Schema.createRecord("wide", null, "x", false, fields);
  }

  private static Schema.Field mk(String name, Schema type, int id, boolean withIds) {
    if (withIds) {
      return idField(name, type, id);
    }
    return new Schema.Field(name, type, null, null);
  }

  private static Schema stringSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  private static Schema longSchema() {
    return Schema.create(Schema.Type.LONG);
  }

  private static byte[] encode(Schema schema, GenericData.Record record) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder enc = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericData.Record>(schema).write(record, enc);
    enc.flush();
    return out.toByteArray();
  }

  private static GenericData.Record sample(Schema schema) {
    GenericData.Record rec = new GenericData.Record(schema);
    rec.put("s1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    rec.put("s2", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    rec.put("opt_s", "cccccccccccccccccccc");
    GenericData.Record nested = new GenericData.Record(schema.getField("nested_rec").schema());
    nested.put("n1", "nnnnnnnnnnnn");
    nested.put("n2", 42L);
    rec.put("nested_rec", nested);
    rec.put("arr", Lists.newArrayList("e1", "e2", "e3", "e4", "e5"));
    Map<String, String> m = new TreeMap<>();
    m.put("k1", "v1");
    m.put("k2", "v2");
    m.put("k3", "v3");
    rec.put("map", m);
    rec.put("part_src", "2024-01-01");
    rec.put("tail", 7L);
    return rec;
  }

  private static org.apache.iceberg.Schema projected() {
    return new org.apache.iceberg.Schema(
        Types.NestedField.optional(7, "part_src", Types.StringType.get()));
  }

  @Test
  public void genericAvroReaderProjection() throws IOException {
    Schema file = wideSchema(true);
    byte[] data = encode(file, sample(file));

    GenericAvroReader<GenericData.Record> reader = GenericAvroReader.create(projected());
    reader.setSchema(file);

    CountingDecoder decoder =
        new CountingDecoder(DecoderFactory.get().binaryDecoder(data, null));
    Object result = reader.read(null, decoder);
    System.out.println("### GenericAvroReader projected result = " + result);
    System.out.println("### GenericAvroReader decoder calls = " + decoder.counts);
  }

  @Test
  public void plannedDataReaderProjection() throws IOException {
    Schema file = wideSchema(true);
    byte[] data = encode(file, sample(file));

    PlannedDataReader<Object> reader = PlannedDataReader.create(projected());
    reader.setSchema(file);

    CountingDecoder decoder =
        new CountingDecoder(DecoderFactory.get().binaryDecoder(data, null));
    Object result = reader.read(null, decoder);
    System.out.println("### PlannedDataReader projected result = " + result);
    System.out.println("### PlannedDataReader decoder calls = " + decoder.counts);
  }

  @Test
  public void fullReadForComparison() throws IOException {
    Schema file = wideSchema(true);
    byte[] data = encode(file, sample(file));

    org.apache.iceberg.Schema full = AvroSchemaUtil.toIceberg(file);
    GenericAvroReader<GenericData.Record> reader = GenericAvroReader.create(full);
    reader.setSchema(file);
    CountingDecoder decoder =
        new CountingDecoder(DecoderFactory.get().binaryDecoder(data, null));
    reader.read(null, decoder);
    System.out.println("### FULL read decoder calls = " + decoder.counts);
  }

  @Test
  public void noFieldIdsInFileSchema() throws IOException {
    Schema file = wideSchema(false);
    byte[] data = encode(file, sample(file));
    try {
      GenericAvroReader<GenericData.Record> reader = GenericAvroReader.create(projected());
      reader.setSchema(file);
      Object result = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));
      System.out.println("### NO-IDS result (optional projection) = " + result);
    } catch (Exception e) {
      System.out.println("### NO-IDS optional threw: " + e);
    }

    // required projected field
    try {
      org.apache.iceberg.Schema req =
          new org.apache.iceberg.Schema(
              Types.NestedField.required(7, "part_src", Types.StringType.get()));
      GenericAvroReader<GenericData.Record> reader = GenericAvroReader.create(req);
      reader.setSchema(file);
      Object result = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));
      System.out.println("### NO-IDS result (required projection) = " + result);
    } catch (Exception e) {
      System.out.println("### NO-IDS required threw: " + e);
    }
  }

  @Test
  public void nonOptionUnionInUnprojectedField() {
    Schema multi =
        Schema.createUnion(
            Schema.create(Schema.Type.NULL), stringSchema(), Schema.create(Schema.Type.INT));
    Schema file =
        Schema.createRecord(
            "wide2",
            null,
            "x",
            false,
            Lists.newArrayList(
                idField("weird", multi, 1), idField("part_src", stringSchema(), 7)));
    try {
      GenericAvroReader<GenericData.Record> reader = GenericAvroReader.create(projected());
      reader.setSchema(file);
      System.out.println("### 3-branch union: OK (no exception)");
    } catch (Exception e) {
      System.out.println("### 3-branch union threw: " + e);
    }

    Schema noNull = Schema.createUnion(stringSchema(), Schema.create(Schema.Type.INT));
    Schema file2 =
        Schema.createRecord(
            "wide3",
            null,
            "x",
            false,
            Lists.newArrayList(
                idField("weird", noNull, 1), idField("part_src", stringSchema(), 7)));
    try {
      GenericAvroReader<GenericData.Record> reader = GenericAvroReader.create(projected());
      reader.setSchema(file2);
      System.out.println("### non-null union: OK (no exception)");
    } catch (Exception e) {
      System.out.println("### non-null union threw: " + e);
    }
  }

  @Test
  public void enumInUnprojectedField() {
    Schema enumSchema = SchemaBuilder.enumeration("color").symbols("RED", "GREEN");
    Schema file =
        Schema.createRecord(
            "wide4",
            null,
            "x",
            false,
            Lists.newArrayList(
                idField("c", enumSchema, 1), idField("part_src", stringSchema(), 7)));
    try {
      GenericAvroReader<GenericData.Record> r = GenericAvroReader.create(projected());
      r.setSchema(file);
      System.out.println("### enum + GenericAvroReader: OK");
    } catch (Exception e) {
      System.out.println("### enum + GenericAvroReader threw: " + e);
    }
    try {
      PlannedDataReader<Object> r = PlannedDataReader.create(projected());
      r.setSchema(file);
      System.out.println("### enum + PlannedDataReader: OK");
    } catch (Exception e) {
      System.out.println("### enum + PlannedDataReader threw: " + e);
    }
  }

  @Test
  public void recursiveRecord() {
    Schema rec = Schema.createRecord("node", null, "x", false);
    Schema.Field self = idField("child", Schema.createUnion(Schema.create(Schema.Type.NULL), rec), 2);
    rec.setFields(Lists.newArrayList(idField("v", longSchema(), 1), self));
    Schema file =
        Schema.createRecord(
            "wide5",
            null,
            "x",
            false,
            Lists.newArrayList(idField("n", rec, 1), idField("part_src", stringSchema(), 7)));
    try {
      GenericAvroReader<GenericData.Record> r = GenericAvroReader.create(projected());
      r.setSchema(file);
      System.out.println("### recursive: OK");
    } catch (Exception e) {
      System.out.println("### recursive threw: " + e);
    }
  }

  @Test
  public void whichReadersOverrideSkip() throws Exception {
    for (String cn :
        new String[] {
          "org.apache.iceberg.avro.ValueReaders",
          "org.apache.iceberg.data.avro.GenericReaders",
          "org.apache.iceberg.avro.InternalReaders"
        }) {
      Class<?> outer = Class.forName(cn);
      System.out.println("### " + cn);
      for (Class<?> inner : outer.getDeclaredClasses()) {
        if (Modifier.isAbstract(inner.getModifiers()) && inner.getDeclaredClasses().length == 0) {
          // still report
        }
        if (!ValueReader.class.isAssignableFrom(inner)) {
          continue;
        }
        boolean overrides = false;
        Class<?> c = inner;
        while (c != null && !c.isInterface()) {
          try {
            c.getDeclaredMethod("skip", Decoder.class);
            overrides = true;
            break;
          } catch (NoSuchMethodException e) {
            c = c.getSuperclass();
          }
        }
        System.out.println(
            String.format(
                "###   %-28s skipOverridden=%s", inner.getSimpleName(), overrides));
      }
    }
  }

  @Test
  public void timestampSkipUsesReadNotSkip() throws IOException {
    // build a file schema with a timestamp-micros field that is NOT projected
    Schema ts = Schema.create(Schema.Type.LONG);
    ts.addProp("logicalType", "timestamp-micros");
    ts.addProp("adjust-to-utc", true);
    Schema file =
        Schema.createRecord(
            "wide6",
            null,
            "x",
            false,
            Lists.newArrayList(
                idField("event_ts", ts, 1), idField("part_src", stringSchema(), 7)));
    GenericData.Record rec = new GenericData.Record(file);
    rec.put("event_ts", 1700000000000000L);
    rec.put("part_src", "p");
    byte[] data = encode(file, rec);

    PlannedDataReader<Object> reader = PlannedDataReader.create(projected());
    reader.setSchema(file);
    CountingDecoder d = new CountingDecoder(DecoderFactory.get().binaryDecoder(data, null));
    System.out.println("### ts result = " + reader.read(null, d));
    System.out.println("### ts skip decoder calls = " + d.counts);
  }

  @Test
  public void benchmark() throws IOException {
    Schema file = wideSchema(true);
    byte[] data = encode(file, sample(file));
    org.apache.iceberg.Schema full = AvroSchemaUtil.toIceberg(file);

    GenericAvroReader<GenericData.Record> fullReader = GenericAvroReader.create(full);
    fullReader.setSchema(file);
    GenericAvroReader<GenericData.Record> projReader = GenericAvroReader.create(projected());
    projReader.setSchema(file);

    int iters = 2_000_000;
    // warmup
    for (int i = 0; i < 200_000; i++) {
      fullReader.read(null, DecoderFactory.get().binaryDecoder(data, null));
      projReader.read(null, DecoderFactory.get().binaryDecoder(data, null));
    }
    long t0 = System.nanoTime();
    for (int i = 0; i < iters; i++) {
      fullReader.read(null, DecoderFactory.get().binaryDecoder(data, null));
    }
    long t1 = System.nanoTime();
    for (int i = 0; i < iters; i++) {
      projReader.read(null, DecoderFactory.get().binaryDecoder(data, null));
    }
    long t2 = System.nanoTime();
    System.out.printf(
        "### full=%.1f ns/rec  projected=%.1f ns/rec  speedup=%.2fx%n",
        (t1 - t0) / (double) iters, (t2 - t1) / (double) iters, (t1 - t0) / (double) (t2 - t1));
  }

  @Test
  public void nameMappingFixesMissingIds() throws IOException {
    Schema file = wideSchema(false);
    byte[] data = encode(file, sample(file));
    org.apache.iceberg.Schema iceberg =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "s1", Types.StringType.get()),
            Types.NestedField.required(2, "s2", Types.StringType.get()),
            Types.NestedField.optional(3, "opt_s", Types.StringType.get()),
            Types.NestedField.required(
                4,
                "nested_rec",
                Types.StructType.of(
                    Types.NestedField.required(101, "n1", Types.StringType.get()),
                    Types.NestedField.required(102, "n2", Types.LongType.get()))),
            Types.NestedField.required(
                5, "arr", Types.ListType.ofRequired(201, Types.StringType.get())),
            Types.NestedField.required(
                6,
                "map",
                Types.MapType.ofRequired(301, 302, Types.StringType.get(), Types.StringType.get())),
            Types.NestedField.required(7, "part_src", Types.StringType.get()),
            Types.NestedField.required(8, "tail", Types.LongType.get()));
    org.apache.iceberg.mapping.NameMapping mapping =
        org.apache.iceberg.mapping.MappingUtil.create(iceberg);
    org.apache.iceberg.Schema proj =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(7, "part_src", Types.StringType.get()));
    org.apache.avro.io.DatumReader<GenericData.Record> reader =
        new NameMappingDatumReader<>(mapping, GenericAvroReader.create(proj));
    reader.setSchema(file);
    CountingDecoder d = new CountingDecoder(DecoderFactory.get().binaryDecoder(data, null));
    System.out.println("### NAMEMAP result = " + reader.read(null, d));
    System.out.println("### NAMEMAP decoder calls = " + d.counts);
  }

  @Test
  public void twoOfFortyFields() throws IOException {
    List<Schema.Field> fields = Lists.newArrayList();
    for (int i = 1; i <= 40; i++) {
      fields.add(idField("f" + i, i % 2 == 0 ? stringSchema() : longSchema(), i));
    }
    Schema file = Schema.createRecord("wide40", null, "x", false, fields);
    GenericData.Record rec = new GenericData.Record(file);
    for (int i = 1; i <= 40; i++) {
      rec.put("f" + i, i % 2 == 0 ? ("v" + i) : (Object) (long) i);
    }
    byte[] data = encode(file, rec);
    org.apache.iceberg.Schema proj =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(20, "f20", Types.StringType.get()),
            Types.NestedField.required(33, "f33", Types.LongType.get()));
    GenericAvroReader<GenericData.Record> reader = GenericAvroReader.create(proj);
    reader.setSchema(file);
    CountingDecoder d = new CountingDecoder(DecoderFactory.get().binaryDecoder(data, null));
    System.out.println("### 2-of-40 result = " + reader.read(null, d));
    System.out.println("### 2-of-40 decoder calls = " + d.counts);
  }

  @Test
  public void bigArraySkipUsesBulkSkip() throws IOException {
    Schema arr = Schema.createArray(stringSchema());
    arr.addProp(AvroSchemaUtil.ELEMENT_ID_PROP, 201);
    Schema file =
        Schema.createRecord(
            "wide7",
            null,
            "x",
            false,
            Lists.newArrayList(idField("a", arr, 1), idField("part_src", stringSchema(), 7)));
    GenericData.Record rec = new GenericData.Record(file);
    List<String> items = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      items.add("item-" + i);
    }
    rec.put("a", items);
    rec.put("part_src", "p");
    // blockSize forces block byte counts to be written
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder enc = EncoderFactory.get().blockingBinaryEncoder(out, null);
    new GenericDatumWriter<GenericData.Record>(file).write(rec, enc);
    enc.flush();
    byte[] data = out.toByteArray();
    GenericAvroReader<GenericData.Record> reader = GenericAvroReader.create(projected());
    reader.setSchema(file);
    CountingDecoder d = new CountingDecoder(DecoderFactory.get().binaryDecoder(data, null));
    System.out.println("### 1000-elem blocking result = " + reader.read(null, d));
    System.out.println("### 1000-elem blocking decoder calls = " + d.counts);
  }
}
