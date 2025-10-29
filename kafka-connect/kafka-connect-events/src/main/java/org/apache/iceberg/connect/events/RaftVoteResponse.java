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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/** Raft Vote Response. Reply to RequestVote RPC indicating whether vote was granted. */
public class RaftVoteResponse implements Payload {

  private String voterId;
  private long term;
  private boolean voteGranted;
  private final Schema avroSchema;

  static final int VOTER_ID = 10_610;
  static final int TERM = 10_611;
  static final int VOTE_GRANTED = 10_612;

  private static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(VOTER_ID, "voter_id", StringType.get()),
          NestedField.required(TERM, "term", LongType.get()),
          NestedField.required(VOTE_GRANTED, "vote_granted", BooleanType.get()));
  private static final Schema AVRO_SCHEMA =
      AvroUtil.convert(ICEBERG_SCHEMA, RaftVoteResponse.class);

  // Used by Avro reflection to instantiate this class when reading events
  public RaftVoteResponse(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public RaftVoteResponse(String voterId, long term, boolean voteGranted) {
    Preconditions.checkNotNull(voterId, "Voter ID cannot be null");
    Preconditions.checkArgument(term >= 0, "Term must be non-negative");
    this.voterId = voterId;
    this.term = term;
    this.voteGranted = voteGranted;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.RAFT_VOTE_RESPONSE;
  }

  public String voterId() {
    return voterId;
  }

  public long term() {
    return term;
  }

  public boolean voteGranted() {
    return voteGranted;
  }

  @Override
  public StructType writeSchema() {
    return ICEBERG_SCHEMA;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case VOTER_ID:
        this.voterId = (String) v;
        return;
      case TERM:
        this.term = (Long) v;
        return;
      case VOTE_GRANTED:
        this.voteGranted = (Boolean) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case VOTER_ID:
        return voterId;
      case TERM:
        return term;
      case VOTE_GRANTED:
        return voteGranted;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
