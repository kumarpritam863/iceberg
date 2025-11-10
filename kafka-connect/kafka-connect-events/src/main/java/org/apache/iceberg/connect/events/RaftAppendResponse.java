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
package org.apache.iceberg.connect.events;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/**
 * AppendEntries Response payload for Raft consensus
 */
public class RaftAppendResponse implements Payload, IndexedRecord {

  private long term;
  private boolean success;
  private String followerId;
  private Schema avroSchema;

  static final int TERM = 10_630;
  static final int SUCCESS = 10_631;
  private static final int FOLLOWER_ID = 10_632;

  public RaftAppendResponse(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public RaftAppendResponse(long term, boolean success, String followerId) {
    this.term = term;
    this.success = success;
    this.followerId = followerId;
    this.avroSchema = AvroUtil.convert(writeSchema(), getClass());
  }

  public long term() {
    return term;
  }

  public boolean success() {
    return success;
  }

  public String followerId() {
    return followerId;
  }

  @Override
  public PayloadType type() {
    return PayloadType.RAFT_APPEND_RESPONSE;
  }

  @Override
  public StructType writeSchema() {
    return StructType.of(
        NestedField.required(TERM, "term", LongType.get()),
        NestedField.required(SUCCESS, "success", BooleanType.get()),
        NestedField.required(FOLLOWER_ID, "follower_id", StringType.get())
    );
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case TERM:
        this.term = v == null ? 0 : (Long) v;
        return;
      case SUCCESS:
        this.success = v != null && (Boolean) v;
        return;
      case FOLLOWER_ID:
        this.followerId = v == null ? null : v.toString();
        return;
      default:
        // ignore
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case TERM:
        return term;
      case SUCCESS:
        return success;
      case FOLLOWER_ID:
        return followerId;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
