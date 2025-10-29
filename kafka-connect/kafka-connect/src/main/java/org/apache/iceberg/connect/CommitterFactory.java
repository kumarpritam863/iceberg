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
package org.apache.iceberg.connect;

import org.apache.iceberg.connect.channel.CommitterImpl;
import org.apache.iceberg.connect.channel.RaftCommitterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterFactory.class);

  static Committer createCommitter(IcebergSinkConfig config) {
    String coordinatorImpl = config.coordinatorImpl();

    switch (coordinatorImpl) {
      case "RAFT":
        LOG.info("Creating Raft-based committer for task {}", config.taskId());
        return new RaftCommitterImpl();

      case "LEAST_PARTITION":
        LOG.info("Creating least-partition committer for task {}", config.taskId());
        return new CommitterImpl();

      default:
        throw new IllegalArgumentException(
            "Unknown coordinator implementation: "
                + coordinatorImpl
                + ". Valid options: LEAST_PARTITION, RAFT");
    }
  }

  private CommitterFactory() {}
}
