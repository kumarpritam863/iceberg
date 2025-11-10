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
 * AppendEntries RPC payload for Raft consensus
 * Used for heartbeats and log replication
 */
public class RaftAppendEntries implements Payload, IndexedRecord {

  private long term;
  private String leaderId;
  private long prevLogIndex;
  private long prevLogTerm;
  private long leaderCommit;
  private boolean isHeartbeat;
  private Schema avroSchema;

  static final int TERM = 10_620;
  static final int LEADER_ID = 10_621;
  static final int PREV_LOG_INDEX = 10_622;
  static final int PREV_LOG_TERM = 10_623;
  static final int LEADER_COMMIT = 10_624;
  static final int IS_HEARTBEAT = 10_625;

  public RaftAppendEntries(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public RaftAppendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                           long leaderCommit, boolean isHeartbeat) {
    this.term = term;
    this.leaderId = leaderId;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.leaderCommit = leaderCommit;
    this.isHeartbeat = isHeartbeat;
    this.avroSchema = AvroUtil.convert(writeSchema(), getClass());
  }

  public long term() {
    return term;
  }

  public String leaderId() {
    return leaderId;
  }

  public long prevLogIndex() {
    return prevLogIndex;
  }

  public long prevLogTerm() {
    return prevLogTerm;
  }

  public long leaderCommit() {
    return leaderCommit;
  }

  public boolean isHeartbeat() {
    return isHeartbeat;
  }

  @Override
  public PayloadType type() {
    return PayloadType.RAFT_APPEND_ENTRIES;
  }

  @Override
  public StructType writeSchema() {
    return StructType.of(
        NestedField.required(TERM, "term", LongType.get()),
        NestedField.required(LEADER_ID, "leader_id", StringType.get()),
        NestedField.required(PREV_LOG_INDEX, "prev_log_index", LongType.get()),
        NestedField.required(PREV_LOG_TERM, "prev_log_term", LongType.get()),
        NestedField.required(LEADER_COMMIT, "leader_commit", LongType.get()),
        NestedField.required(IS_HEARTBEAT, "is_heartbeat", BooleanType.get())
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
      case LEADER_ID:
        this.leaderId = v == null ? null : v.toString();
        return;
      case PREV_LOG_INDEX:
        this.prevLogIndex = v == null ? 0 : (Long) v;
        return;
      case PREV_LOG_TERM:
        this.prevLogTerm = v == null ? 0 : (Long) v;
        return;
      case LEADER_COMMIT:
        this.leaderCommit = v == null ? 0 : (Long) v;
        return;
      case IS_HEARTBEAT:
        this.isHeartbeat = v != null && (Boolean) v;
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
      case LEADER_ID:
        return leaderId;
      case PREV_LOG_INDEX:
        return prevLogIndex;
      case PREV_LOG_TERM:
        return prevLogTerm;
      case LEADER_COMMIT:
        return leaderCommit;
      case IS_HEARTBEAT:
        return isHeartbeat;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
