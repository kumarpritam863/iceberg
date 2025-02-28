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

import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);

  private IcebergSinkConfig config;
  private Catalog catalog;
  private BaseCommitter committer;

  @Override
  public String version() {
    return IcebergSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> props) {
    LOG.info("Starting IcebergSinkTask with properties: {}", props);
    try {
      this.config = new IcebergSinkConfig(props);
      this.catalog = CatalogUtils.loadCatalog(config);
      this.committer = CommitterFactory.createCommitter(catalog, config, context).configure(config.committerConfig());
    } catch (Exception e) {
      LOG.error("Error initializing IcebergSinkTask", e);
      throw new RuntimeException("Failed to start IcebergSinkTask", e);
    }
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    LOG.info("Opening IcebergSinkTask for partitions: {}", partitions);
    if (committer == null) {
      LOG.warn("Committer is not initialized, skipping partition handling.");
      return;
    }
    try {
      if (committer instanceof KafkaCommitter) {
        ((KafkaCommitter) committer).start(partitions);
        ((KafkaCommitter) committer).syncLastCommittedOffsets();
      } else {
        ((Committer) committer).start(catalog, config, context);
      }
    } catch (Exception e) {
      LOG.error("Error opening partitions in IcebergSinkTask", e);
      throw new RuntimeException("Failed to open partitions", e);
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    if (committer instanceof KafkaCommitter) {
      ((KafkaCommitter) committer).stop(partitions);
    } else if (committer instanceof Committer) {
      ((Committer) committer).stop();
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (committer == null) {
      LOG.warn("Committer is not initialized, skipping put operation.");
      return;
    }
    try {
      LOG.debug("Processing {} records", sinkRecords.size());
      committer.save(sinkRecords);
    } catch (Exception e) {
      LOG.error("Error processing SinkRecords in IcebergSinkTask", e);
      throw new RuntimeException("Failed to process records", e);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    if (committer != null) {
      committer.save(null);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return Map.of(); // Offset commit is handled by the worker
  }

  @Override
  public void stop() {
    LOG.info("Stopping IcebergSinkTask");
    try {
      if (committer != null) {
        stopCommitter();
        committer = null;
      }
      stopCatalog();
    } catch (Exception e) {
      LOG.error("Error stopping IcebergSinkTask", e);
    }
  }

  private void stopCommitter() {
    if (committer == null) return;
    try {
      if (committer instanceof KafkaCommitter) {
        ((KafkaCommitter) committer).stop(context.assignment());
      } else {
        ((Committer) committer).stop();
      }
    } catch (Exception e) {
      LOG.error("Error stopping committer", e);
    }
  }

  private void stopCatalog() {
    if (catalog instanceof AutoCloseable) {
      try {
        ((AutoCloseable) catalog).close();
      } catch (Exception e) {
        LOG.warn("Error closing catalog instance, ignoring...", e);
      }
    }
    catalog = null;
  }
}