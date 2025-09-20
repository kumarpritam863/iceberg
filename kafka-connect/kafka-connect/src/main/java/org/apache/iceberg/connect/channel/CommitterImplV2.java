/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.iceberg.connect.channel;

import java.util.Collection;
import org.apache.iceberg.connect.Committer;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterImplV2 implements Committer {

    private static final Logger LOG = LoggerFactory.getLogger(CommitterImplV2.class);

    private Worker worker;
    private IcebergSinkConfig config;
    private SinkTaskContext context;
    private KafkaClientFactory clientFactory;

    @Override
    public void open(Collection<TopicPartition> addedPartitions) {
    }

    @Override
    public void close(Collection<TopicPartition> closedPartitions) {
        // Always try to stop the worker to avoid duplicates.
        stopWorker();

        // Empty partitions → task was stopped explicitly. Stop coordinator if running.
        if (closedPartitions.isEmpty()) {
            LOG.info("Task stopped. Closing coordinator.");
            config.closeCatalog();
            return;
        }

        // Reset offsets to last committed to avoid data loss.
        LOG.info(
                "Seeking to last committed offsets for worker {}-{}.",
                config.connectorName(),
                config.taskId());
        KafkaUtils.seekToLastCommittedOffsets(context);
    }

    @Override
    public void save(Collection<SinkRecord> sinkRecords) {
        if (sinkRecords != null && !sinkRecords.isEmpty()) {
            startWorker();
            worker.save(sinkRecords);
        }
        processControlEvents();
    }

    @Override
    public void configure(IcebergSinkConfig icebergSinkConfig) {
        this.config = icebergSinkConfig;
        this.context = icebergSinkConfig.context();
        this.clientFactory = new KafkaClientFactory(icebergSinkConfig.kafkaProps());
        this.config.loadCatalog();
    }

    private void processControlEvents() {
        if (worker != null) {
            worker.process();
        }
    }

    private void startWorker() {
        if (null == this.worker) {
            LOG.info("Starting commit worker {}-{}", config.connectorName(), config.taskId());
            SinkWriter sinkWriter = new SinkWriter(config);
            worker = new Worker(config, clientFactory, sinkWriter);
            worker.start();
        }
    }

    private void stopWorker() {
        if (worker != null) {
            worker.stop();
            worker = null;
        }
    }
}
