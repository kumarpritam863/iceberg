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

import org.apache.iceberg.connect.events.Payload;

/**
 * Transport layer abstraction for Raft election messages.
 *
 * <p>Decouples Raft consensus logic from Kafka-specific transport details.
 * Allows for separate topic, dedicated consumer/producer, and background processing.
 */
public interface RaftElectionTransport {

  /**
   * Send a Raft message to all nodes in the cluster.
   *
   * @param payload The Raft message payload (RequestVote, VoteResponse, AppendEntries, etc.)
   * @param groupId The election group ID for message routing
   */
  void send(Payload payload, String groupId);

  /**
   * Register a listener to receive incoming Raft messages.
   *
   * @param listener The message handler
   */
  void setMessageListener(RaftMessageListener listener);

  /**
   * Start the transport layer (consumer/producer).
   */
  void start();

  /**
   * Stop the transport layer gracefully.
   */
  void stop();

  /**
   * Check if transport is running.
   *
   * @return true if running
   */
  boolean isRunning();

  /** Callback interface for incoming Raft messages */
  interface RaftMessageListener {
    /**
     * Handle an incoming Raft message.
     *
     * @param payload The message payload
     * @param senderId The sender's node ID
     * @param groupId The election group ID
     */
    void onMessage(Payload payload, String senderId, String groupId);
  }
}
