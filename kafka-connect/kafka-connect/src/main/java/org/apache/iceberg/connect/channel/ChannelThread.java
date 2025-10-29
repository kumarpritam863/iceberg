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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic thread wrapper for Channel-based coordinators.
 *
 * <p>This class can wrap any Channel subclass (Coordinator, RaftCoordinator, etc.) and run it in a
 * separate thread.
 */
class ChannelThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelThread.class);
  private static final String THREAD_NAME = "iceberg-coord";

  private final Channel channel;
  private volatile boolean terminated;

  ChannelThread(Channel channel) {
    super(THREAD_NAME);
    this.channel = channel;
  }

  @Override
  public void run() {
    try {
      channel.start();
    } catch (Exception e) {
      LOG.error("Channel error during start, exiting thread", e);
      this.terminated = true;
    }

    while (!terminated) {
      try {
        channel.process();
      } catch (Exception e) {
        LOG.error("Channel error during process, exiting thread", e);
        this.terminated = true;
      }
    }

    try {
      channel.stop();
    } catch (Exception e) {
      LOG.error("Channel error during stop, ignoring", e);
    }
  }

  boolean isTerminated() {
    return terminated;
  }

  void terminate() {
    this.terminated = true;
    channel.terminate();
  }
}
