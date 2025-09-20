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
package org.apache.iceberg.connect.coordinator;

import java.time.Duration;

/**
 * Configuration for the standalone coordinator service.
 */
public class CoordinatorConfig {

  private final String kafkaBootstrapServers;
  private final Duration commitInterval;
  private final Duration commitTimeout;
  private final int commitThreads;
  private final long keepAliveTimeoutMs;
  private final boolean enableTransactions;
  private final String transactionalId;
  private final Duration transactionTimeout;

  public CoordinatorConfig(
      String kafkaBootstrapServers,
      Duration commitInterval,
      Duration commitTimeout,
      int commitThreads,
      long keepAliveTimeoutMs,
      boolean enableTransactions,
      String transactionalId,
      Duration transactionTimeout) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
    this.commitInterval = commitInterval;
    this.commitTimeout = commitTimeout;
    this.commitThreads = commitThreads;
    this.keepAliveTimeoutMs = keepAliveTimeoutMs;
    this.enableTransactions = enableTransactions;
    this.transactionalId = transactionalId;
    this.transactionTimeout = transactionTimeout;
  }

  public String kafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public Duration commitInterval() {
    return commitInterval;
  }

  public Duration commitTimeout() {
    return commitTimeout;
  }

  public int commitThreads() {
    return commitThreads;
  }

  public long keepAliveTimeoutMs() {
    return keepAliveTimeoutMs;
  }

  public boolean enableTransactions() {
    return enableTransactions;
  }

  public String transactionalId() {
    return transactionalId;
  }

  public Duration transactionTimeout() {
    return transactionTimeout;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String kafkaBootstrapServers = "localhost:9092";
    private Duration commitInterval = Duration.ofMinutes(5);
    private Duration commitTimeout = Duration.ofMinutes(30);
    private int commitThreads = 4;
    private long keepAliveTimeoutMs = 60000L;
    private boolean enableTransactions = true;
    private String transactionalId = null;
    private Duration transactionTimeout = Duration.ofMinutes(15);

    public Builder kafkaBootstrapServers(String kafkaBootstrapServers) {
      this.kafkaBootstrapServers = kafkaBootstrapServers;
      return this;
    }

    public Builder commitInterval(Duration commitInterval) {
      this.commitInterval = commitInterval;
      return this;
    }

    public Builder commitTimeout(Duration commitTimeout) {
      this.commitTimeout = commitTimeout;
      return this;
    }

    public Builder commitThreads(int commitThreads) {
      this.commitThreads = commitThreads;
      return this;
    }

    public Builder keepAliveTimeoutMs(long keepAliveTimeoutMs) {
      this.keepAliveTimeoutMs = keepAliveTimeoutMs;
      return this;
    }

    public Builder enableTransactions(boolean enableTransactions) {
      this.enableTransactions = enableTransactions;
      return this;
    }

    public Builder transactionalId(String transactionalId) {
      this.transactionalId = transactionalId;
      return this;
    }

    public Builder transactionTimeout(Duration transactionTimeout) {
      this.transactionTimeout = transactionTimeout;
      return this;
    }

    public CoordinatorConfig build() {
      return new CoordinatorConfig(
          kafkaBootstrapServers,
          commitInterval,
          commitTimeout,
          commitThreads,
          keepAliveTimeoutMs,
          enableTransactions,
          transactionalId,
          transactionTimeout);
    }
  }
}