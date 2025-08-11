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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitState {
  private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

  private final List<Envelope> commitBuffer = Lists.newArrayList();
  private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
  private final IcebergSinkConfig config;
  private final AtomicBoolean isCommitting = new AtomicBoolean(false);

  CommitState(IcebergSinkConfig config) {
    this.config = config;
  }

  void addResponse(Envelope envelope) {
    commitBuffer.add(envelope);
    if (!isCommitting.get()) {
      DataWritten dataWritten = (DataWritten) envelope.event().payload();
      LOG.warn(
          "Received commit response when no commit in progress, this can happen during recovery. Commit ID: {}",
          dataWritten.commitId());
    }
  }

  boolean isCommitIntervalReached() {
    return !isCommitting.get()
        && System.currentTimeMillis() - startTime.get() >= config.commitIntervalMs();
  }

  void startNewCommit() {
    isCommitting.set(true);
    startTime.set(System.currentTimeMillis());
  }

  void endCurrentCommit() {
    isCommitting.set(false);
  }

  void clearResponses() {
    commitBuffer.clear();
  }

  boolean isCommitTimedOut() {
    if (!isCommitting.get()) {
      return false;
    }

    if (System.currentTimeMillis() - startTime.get() > config.commitTimeoutMs()) {
      LOG.info("Commit timeout reached.");
      return true;
    }

    return false;
  }

  Map<TableReference, List<Envelope>> tableCommitMap() {
    return commitBuffer.stream()
        .collect(
            Collectors.groupingBy(
                envelope -> ((DataWritten) envelope.event().payload()).tableReference()));
  }

  int bufferSize() {
    return commitBuffer.size();
  }
}
