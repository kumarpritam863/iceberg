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
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

/**
 * Raft RequestVote RPC. Sent by candidates during leader election to request votes from other
 * nodes.
 */
public class RaftRequestVote implements Payload {

  private String candidateId;
  private long term;
  private final Schema avroSchema;

  static final int CANDIDATE_ID = 10_600;
  static final int TERM = 10_601;

  private static final StructType ICEBERG_SCHEMA =
      StructType.of(
          NestedField.required(CANDIDATE_ID, "candidate_id", StringType.get()),
          NestedField.required(TERM, "term", LongType.get()));
  private static final Schema AVRO_SCHEMA =
      AvroUtil.convert(ICEBERG_SCHEMA, RaftRequestVote.class);

  // Used by Avro reflection to instantiate this class when reading events
  public RaftRequestVote(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public RaftRequestVote(String candidateId, long term) {
    Preconditions.checkNotNull(candidateId, "Candidate ID cannot be null");
    Preconditions.checkArgument(term >= 0, "Term must be non-negative");
    this.candidateId = candidateId;
    this.term = term;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.RAFT_REQUEST_VOTE;
  }

  public String candidateId() {
    return candidateId;
  }

  public long term() {
    return term;
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
      case CANDIDATE_ID:
        this.candidateId = (String) v;
        return;
      case TERM:
        this.term = (Long) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case CANDIDATE_ID:
        return candidateId;
      case TERM:
        return term;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
