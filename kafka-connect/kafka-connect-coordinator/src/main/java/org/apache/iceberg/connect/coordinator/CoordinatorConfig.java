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

import java.util.Map;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.config.CommonConfig;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.config.ConfigDef;

public class CoordinatorConfig extends CommonConfig {

    private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.control.commit.interval-ms";
    private static final int COMMIT_INTERVAL_MS_DEFAULT = 300_000;
    private static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.control.commit.timeout-ms";
    private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;
    private static final String COMMIT_THREADS_PROP = "iceberg.control.commit.threads";
    private static final String CONNECT_GROUP_ID_PROP = "iceberg.connect.group-id";

    private static final String COORDINATOR_EXECUTOR_KEEP_ALIVE_TIMEOUT_MS =
            "iceberg.coordinator-executor-keep-alive-timeout-ms";

    @VisibleForTesting
    static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";

    public static final ConfigDef CONFIG_DEF = newConfigDef();
    private static final String COORDINATOR_ID = "iceberg.coordinator-id";

    public static String version() {
        return IcebergBuild.version();
    }

    private static ConfigDef newConfigDef() {
        return new ConfigDef(CommonConfig.CONFIG_DEF)  // inherit from CommonConfig
                .define(
                        CONNECT_GROUP_ID_PROP,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        "Name of the Connect consumer group, should not be set under normal conditions")
                .define(
                        COMMIT_INTERVAL_MS_PROP,
                        ConfigDef.Type.INT,
                        COMMIT_INTERVAL_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Coordinator interval for performing Iceberg table commits, in millis")
                .define(
                        COMMIT_TIMEOUT_MS_PROP,
                        ConfigDef.Type.INT,
                        COMMIT_TIMEOUT_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Coordinator time to wait for worker responses before committing, in millis")
                .define(
                        COMMIT_THREADS_PROP,
                        ConfigDef.Type.INT,
                        Runtime.getRuntime().availableProcessors() * 2,
                        ConfigDef.Importance.MEDIUM,
                        "Coordinator threads to use for table commits, default is (cores * 2)")
                .define(
                        COORDINATOR_EXECUTOR_KEEP_ALIVE_TIMEOUT_MS,
                        ConfigDef.Type.LONG,
                        120000L,
                        ConfigDef.Importance.LOW,
                        "Config to control coordinator executor keep alive time")
                .define(
                        COORDINATOR_ID,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        "Id of the coordinator, if running on coordinator as service");
    }


    public CoordinatorConfig(Map<String, String> originalProps) {
        super(CONFIG_DEF, originalProps);
    }

    public long keepAliveTimeoutInMs() {
        return getLong(COORDINATOR_EXECUTOR_KEEP_ALIVE_TIMEOUT_MS);
    }

    public int commitIntervalMs() {
        return getInt(COMMIT_INTERVAL_MS_PROP);
    }

    public int commitTimeoutMs() {
        return getInt(COMMIT_TIMEOUT_MS_PROP);
    }

    public int commitThreads() {
        return getInt(COMMIT_THREADS_PROP);
    }

    public String coordinatorId() {
        return getString(COORDINATOR_ID);
    }
}
