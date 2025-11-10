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
 * RequestVote Response payload for Raft consensus
 */
public class RaftVoteResponse implements Payload, IndexedRecord {

  private long term;
  private boolean voteGranted;
  private String voterId;
  private Schema avroSchema;

  static final int TERM = 10_610;
  static final int VOTE_GRANTED = 10_611;
  static final int VOTER_ID = 10_612;

  public RaftVoteResponse(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public RaftVoteResponse(long term, boolean voteGranted, String voterId) {
    this.term = term;
    this.voteGranted = voteGranted;
    this.voterId = voterId;
    this.avroSchema = AvroUtil.convert(writeSchema(), getClass());
  }

  public long term() {
    return term;
  }

  public boolean voteGranted() {
    return voteGranted;
  }

  public String voterId() {
    return voterId;
  }

  @Override
  public PayloadType type() {
    return PayloadType.RAFT_VOTE_RESPONSE;
  }

  @Override
  public StructType writeSchema() {
    return StructType.of(
        NestedField.required(TERM, "term", LongType.get()),
        NestedField.required(VOTE_GRANTED, "vote_granted", BooleanType.get()),
        NestedField.required(VOTER_ID, "voter_id", StringType.get())
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
      case VOTE_GRANTED:
        this.voteGranted = v != null && (Boolean) v;
        return;
      case VOTER_ID:
        this.voterId = v == null ? null : v.toString();
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
      case VOTE_GRANTED:
        return voteGranted;
      case VOTER_ID:
        return voterId;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
