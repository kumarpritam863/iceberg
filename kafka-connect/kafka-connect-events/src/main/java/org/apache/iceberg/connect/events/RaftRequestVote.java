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
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/**
 * RequestVote RPC payload for Raft consensus
 * Sent by candidates during leader election
 */
public class RaftRequestVote implements Payload, IndexedRecord {

  private long term;
  private String candidateId;
  private long lastLogIndex;
  private long lastLogTerm;
  private Schema avroSchema;

  static final int TERM = 10_600;
  static final int CANDIDATE_ID = 10_601;
  static final int LAST_LOG_INDEX = 10_602;
  static final int LAST_LOG_TERM = 10_603;

  // Used by Avro reflection
  public RaftRequestVote(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public RaftRequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
    this.term = term;
    this.candidateId = candidateId;
    this.lastLogIndex = lastLogIndex;
    this.lastLogTerm = lastLogTerm;
    this.avroSchema = AvroUtil.convert(writeSchema(), getClass());
  }

  public long term() {
    return term;
  }

  public String candidateId() {
    return candidateId;
  }

  public long lastLogIndex() {
    return lastLogIndex;
  }

  public long lastLogTerm() {
    return lastLogTerm;
  }

  @Override
  public PayloadType type() {
    return PayloadType.RAFT_REQUEST_VOTE;
  }

  @Override
  public StructType writeSchema() {
    return StructType.of(
        NestedField.required(TERM, "term", LongType.get()),
        NestedField.required(CANDIDATE_ID, "candidate_id", StringType.get()),
        NestedField.required(LAST_LOG_INDEX, "last_log_index", LongType.get()),
        NestedField.required(LAST_LOG_TERM, "last_log_term", LongType.get())
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
      case CANDIDATE_ID:
        this.candidateId = v == null ? null : v.toString();
        return;
      case LAST_LOG_INDEX:
        this.lastLogIndex = v == null ? 0 : (Long) v;
        return;
      case LAST_LOG_TERM:
        this.lastLogTerm = v == null ? 0 : (Long) v;
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
      case CANDIDATE_ID:
        return candidateId;
      case LAST_LOG_INDEX:
        return lastLogIndex;
      case LAST_LOG_TERM:
        return lastLogTerm;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
