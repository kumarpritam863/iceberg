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

package org.apache.iceberg.connect.coordinator;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.connect.IcebergSinkConfig;


import java.io.FileInputStream;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorEntryPoint {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEntryPoint.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: CoordinatorEntryPoint <config.properties>");
            System.exit(1);
        }

        // Load config from file
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(args[0])) {
            props.load(fis);
        } catch (Exception ex) {
            LOG.error("Failed while loading config", ex);
        }

        // Build IcebergSinkConfig
        Map<String, String> configMap = new HashMap<>();
        for (String name : props.stringPropertyNames()) {
            configMap.put(name, props.getProperty(name));
        }
        IcebergSinkConfig config = new IcebergSinkConfig(configMap);
        Coordinator coordinator = new Coordinator(
                config
        );

        AtomicLong coordinatorProcessCount = new AtomicLong(0);
        try {
            while (true) {
                LOG.info("Starting coordinator process no {}", coordinatorProcessCount.incrementAndGet());
                coordinator.process();
                LOG.info("Completed coordinator process no {}", coordinatorProcessCount.get());
            }
        } finally {
            coordinator.stop();
        }
    }
}

