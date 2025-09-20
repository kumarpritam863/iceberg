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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a job configuration registered with the coordinator.
 * Contains all necessary information to manage a Kafka Connect job.
 */
public class JobConfig {

  private final String jobId;
  private final String connectGroupId;
  private final String catalogName;
  private final Map<String, String> catalogProperties;
  private final String tableName;
  private final String controlTopic;
  private final Map<String, Object> sinkConfig;
  private final OffsetDateTime createdAt;
  private final OffsetDateTime updatedAt;
  private final JobStatus status;

  @JsonCreator
  public JobConfig(
      @JsonProperty("jobId") String jobId,
      @JsonProperty("connectGroupId") String connectGroupId,
      @JsonProperty("catalogName") String catalogName,
      @JsonProperty("catalogProperties") Map<String, String> catalogProperties,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("controlTopic") String controlTopic,
      @JsonProperty("sinkConfig") Map<String, Object> sinkConfig,
      @JsonProperty("createdAt") OffsetDateTime createdAt,
      @JsonProperty("updatedAt") OffsetDateTime updatedAt,
      @JsonProperty("status") JobStatus status) {
    this.jobId = Objects.requireNonNull(jobId, "jobId cannot be null");
    this.connectGroupId = Objects.requireNonNull(connectGroupId, "connectGroupId cannot be null");
    this.catalogName = Objects.requireNonNull(catalogName, "catalogName cannot be null");
    this.catalogProperties = Objects.requireNonNull(catalogProperties, "catalogProperties cannot be null");
    this.tableName = Objects.requireNonNull(tableName, "tableName cannot be null");
    this.controlTopic = Objects.requireNonNull(controlTopic, "controlTopic cannot be null");
    this.sinkConfig = Objects.requireNonNull(sinkConfig, "sinkConfig cannot be null");
    this.createdAt = Objects.requireNonNull(createdAt, "createdAt cannot be null");
    this.updatedAt = Objects.requireNonNull(updatedAt, "updatedAt cannot be null");
    this.status = Objects.requireNonNull(status, "status cannot be null");
  }

  public String jobId() {
    return jobId;
  }

  public String connectGroupId() {
    return connectGroupId;
  }

  public String catalogName() {
    return catalogName;
  }

  public Map<String, String> catalogProperties() {
    return catalogProperties;
  }

  public String tableName() {
    return tableName;
  }

  public String controlTopic() {
    return controlTopic;
  }

  public Map<String, Object> sinkConfig() {
    return sinkConfig;
  }

  public OffsetDateTime createdAt() {
    return createdAt;
  }

  public OffsetDateTime updatedAt() {
    return updatedAt;
  }

  public JobStatus status() {
    return status;
  }

  public JobConfig withStatus(JobStatus newStatus) {
    return new JobConfig(
        jobId,
        connectGroupId,
        catalogName,
        catalogProperties,
        tableName,
        controlTopic,
        sinkConfig,
        createdAt,
        OffsetDateTime.now(),
        newStatus);
  }

  public JobConfig withUpdatedAt(OffsetDateTime newUpdatedAt) {
    return new JobConfig(
        jobId,
        connectGroupId,
        catalogName,
        catalogProperties,
        tableName,
        controlTopic,
        sinkConfig,
        createdAt,
        newUpdatedAt,
        status);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JobConfig jobConfig = (JobConfig) o;
    return Objects.equals(jobId, jobConfig.jobId) &&
        Objects.equals(connectGroupId, jobConfig.connectGroupId) &&
        Objects.equals(catalogName, jobConfig.catalogName) &&
        Objects.equals(catalogProperties, jobConfig.catalogProperties) &&
        Objects.equals(tableName, jobConfig.tableName) &&
        Objects.equals(controlTopic, jobConfig.controlTopic) &&
        Objects.equals(sinkConfig, jobConfig.sinkConfig) &&
        Objects.equals(createdAt, jobConfig.createdAt) &&
        Objects.equals(updatedAt, jobConfig.updatedAt) &&
        status == jobConfig.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobId, connectGroupId, catalogName, catalogProperties, tableName,
        controlTopic, sinkConfig, createdAt, updatedAt, status);
  }

  @Override
  public String toString() {
    return "JobConfig{" +
        "jobId='" + jobId + '\'' +
        ", connectGroupId='" + connectGroupId + '\'' +
        ", catalogName='" + catalogName + '\'' +
        ", tableName='" + tableName + '\'' +
        ", controlTopic='" + controlTopic + '\'' +
        ", status=" + status +
        ", createdAt=" + createdAt +
        ", updatedAt=" + updatedAt +
        '}';
  }

  public enum JobStatus {
    CREATED,
    RUNNING,
    PAUSED,
    FAILED,
    DELETED
  }
}