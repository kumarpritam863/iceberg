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
package org.apache.iceberg.connect.channel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Payload;

/**
 * Envelope for Raft election messages transmitted over dedicated election topic.
 *
 * <p>Contains:
 * <ul>
 *   <li>Sender node ID for message attribution
 *   <li>Election group ID for multi-tenancy support
 *   <li>Serialized Raft payload (RequestVote, AppendEntries, etc.)
 * </ul>
 */
public class RaftElectionMessage {

  private final String senderId;
  private final String groupId;
  private final byte[] payloadData;
  private final String payloadType;

  @JsonCreator
  public RaftElectionMessage(
      @JsonProperty("senderId") String senderId,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("payloadType") String payloadType,
      @JsonProperty("payloadData") byte[] payloadData) {
    this.senderId = senderId;
    this.groupId = groupId;
    this.payloadType = payloadType;
    this.payloadData = payloadData;
  }

  public static RaftElectionMessage create(String senderId, String groupId, Payload payload) {
    byte[] data = AvroUtil.encodePayload(payload);
    String type = payload.type().name();
    return new RaftElectionMessage(senderId, groupId, type, data);
  }

  @JsonProperty("senderId")
  public String senderId() {
    return senderId;
  }

  @JsonProperty("groupId")
  public String groupId() {
    return groupId;
  }

  @JsonProperty("payloadType")
  public String payloadType() {
    return payloadType;
  }

  @JsonProperty("payloadData")
  public byte[] payloadData() {
    return payloadData;
  }

  /**
   * Deserialize the Raft payload.
   *
   * @return The decoded payload
   */
  public Payload decodePayload() {
    return AvroUtil.decodePayload(payloadData);
  }
}
