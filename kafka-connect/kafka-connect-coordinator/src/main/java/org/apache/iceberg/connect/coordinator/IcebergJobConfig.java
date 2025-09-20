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

/**
 * Configuration for a single Iceberg Kafka Connect job.
 * This represents the configuration that would typically be passed
 * to a Kafka Connect connector, but is used by the standalone coordinator.
 */
public class IcebergJobConfig {

  private final String jobId;
  private final String connectGroupId;
  private final String controlTopic;
  private final String catalogName;
  private final java.util.Map<String, String> catalogProperties;
  private final java.util.Map<String, String> tableConfigs;
  private final CoordinatorConfig coordinatorConfig;

  public IcebergJobConfig(
      String jobId,
      String connectGroupId,
      String controlTopic,
      String catalogName,
      java.util.Map<String, String> catalogProperties,
      java.util.Map<String, String> tableConfigs,
      CoordinatorConfig coordinatorConfig) {
    this.jobId = jobId;
    this.connectGroupId = connectGroupId;
    this.controlTopic = controlTopic;
    this.catalogName = catalogName;
    this.catalogProperties = catalogProperties;
    this.tableConfigs = tableConfigs;
    this.coordinatorConfig = coordinatorConfig;
  }

  public String jobId() {
    return jobId;
  }

  public String connectGroupId() {
    return connectGroupId;
  }

  public String controlTopic() {
    return controlTopic;
  }

  public String catalogName() {
    return catalogName;
  }

  public java.util.Map<String, String> catalogProperties() {
    return catalogProperties;
  }

  public java.util.Map<String, String> tableConfigs() {
    return tableConfigs;
  }

  public CoordinatorConfig coordinatorConfig() {
    return coordinatorConfig;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String jobId;
    private String connectGroupId;
    private String controlTopic;
    private String catalogName;
    private java.util.Map<String, String> catalogProperties = new java.util.HashMap<>();
    private java.util.Map<String, String> tableConfigs = new java.util.HashMap<>();
    private CoordinatorConfig coordinatorConfig;

    public Builder jobId(String jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder connectGroupId(String connectGroupId) {
      this.connectGroupId = connectGroupId;
      return this;
    }

    public Builder controlTopic(String controlTopic) {
      this.controlTopic = controlTopic;
      return this;
    }

    public Builder catalogName(String catalogName) {
      this.catalogName = catalogName;
      return this;
    }

    public Builder catalogProperties(java.util.Map<String, String> catalogProperties) {
      this.catalogProperties = catalogProperties;
      return this;
    }

    public Builder catalogProperty(String key, String value) {
      this.catalogProperties.put(key, value);
      return this;
    }

    public Builder tableConfigs(java.util.Map<String, String> tableConfigs) {
      this.tableConfigs = tableConfigs;
      return this;
    }

    public Builder tableConfig(String key, String value) {
      this.tableConfigs.put(key, value);
      return this;
    }

    public Builder coordinatorConfig(CoordinatorConfig coordinatorConfig) {
      this.coordinatorConfig = coordinatorConfig;
      return this;
    }

    public IcebergJobConfig build() {
      if (jobId == null) {
        jobId = "iceberg-job-" + java.util.UUID.randomUUID().toString();
      }
      if (coordinatorConfig == null) {
        coordinatorConfig = CoordinatorConfig.builder().build();
      }

      return new IcebergJobConfig(
          jobId,
          connectGroupId,
          controlTopic,
          catalogName,
          catalogProperties,
          tableConfigs,
          coordinatorConfig);
    }
  }
}