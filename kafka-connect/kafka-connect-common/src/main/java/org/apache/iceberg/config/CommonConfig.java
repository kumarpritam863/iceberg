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

package org.apache.iceberg.config;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonConfig extends AbstractConfig {
    private static final Logger LOG = LoggerFactory.getLogger(CommonConfig.class.getName());

    private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
    private static final String HADOOP_PROP_PREFIX = "iceberg.hadoop.";
    private static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";

    private static final String CATALOG_NAME_PROP = "iceberg.catalog";
    private static final String CONTROL_TOPIC_PROP = "iceberg.control.topic";
    private static final String CONTROL_GROUP_ID_PREFIX_PROP = "iceberg.control.group-id-prefix";
    private static final String HADOOP_CONF_DIR_PROP = "iceberg.hadoop-conf-dir";

    private static final String DEFAULT_CATALOG_NAME = "iceberg";
    private static final String DEFAULT_CONTROL_TOPIC = "control-iceberg";
    public static final String DEFAULT_CONTROL_GROUP_PREFIX = "cg-control-";

    private static final String TRANSACTIONAL_PREFIX_PROP =
            "iceberg.coordinator.transactional.prefix";
    public static final String INTERNAL_TRANSACTIONAL_SUFFIX_PROP =
            "iceberg.coordinator.transactional.suffix";

    public static final ConfigDef CONFIG_DEF = newConfigDef();

    public static String version() {
        return IcebergBuild.version();
    }

    private static ConfigDef newConfigDef() {
        ConfigDef configDef = new ConfigDef();
        configDef.define(
                CATALOG_NAME_PROP,
                ConfigDef.Type.STRING,
                DEFAULT_CATALOG_NAME,
                ConfigDef.Importance.MEDIUM,
                "Iceberg catalog name");
        configDef.define(
                CONTROL_TOPIC_PROP,
                ConfigDef.Type.STRING,
                DEFAULT_CONTROL_TOPIC,
                ConfigDef.Importance.MEDIUM,
                "Name of the control topic");
        configDef.define(
                CONTROL_GROUP_ID_PREFIX_PROP,
                ConfigDef.Type.STRING,
                DEFAULT_CONTROL_GROUP_PREFIX,
                ConfigDef.Importance.LOW,
                "Prefix of the control consumer group");
        configDef.define(
                HADOOP_CONF_DIR_PROP,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                "If specified, Hadoop config files in this directory will be loaded");
        configDef.define(
                TRANSACTIONAL_PREFIX_PROP,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                "Optional prefix of the transactional id for the coordinator");
        configDef.define(
                INTERNAL_TRANSACTIONAL_SUFFIX_PROP,
                ConfigDef.Type.STRING,
                UUID.randomUUID(),
                ConfigDef.Importance.LOW,
                "Optional prefix of the transactional id for the coordinator");
        return configDef;
    }

    private final Map<String, String> catalogProps;
    private final Map<String, String> hadoopProps;
    private final Map<String, String> kafkaProps;

    public CommonConfig(ConfigDef configDef, Map<String, String> originalProps) {
        super(configDef, originalProps);

        this.catalogProps = PropertyUtil.propertiesWithPrefix(originalProps, CATALOG_PROP_PREFIX);
        this.hadoopProps = PropertyUtil.propertiesWithPrefix(originalProps, HADOOP_PROP_PREFIX);
        kafkaProps = PropertyUtil.propertiesWithPrefix(originalProps, KAFKA_PROP_PREFIX);
    }

    public String transactionalPrefix() {
        String result = getString(TRANSACTIONAL_PREFIX_PROP);
        if (result != null) {
            return result;
        }

        return "";
    }

    public String transactionalSuffix() {
        // this is for internal use and is not part of the config definition...
        return getString(INTERNAL_TRANSACTIONAL_SUFFIX_PROP);
    }

    public Map<String, String> catalogProps() {
        return catalogProps;
    }

    public Map<String, String> hadoopProps() {
        return hadoopProps;
    }

    public Map<String, String> kafkaProps() {
        return kafkaProps;
    }

    public String catalogName() {
        return getString(CATALOG_NAME_PROP);
    }

    public String controlTopic() {
        return getString(CONTROL_TOPIC_PROP);
    }

    public String controlGroupIdPrefix() {
        return getString(CONTROL_GROUP_ID_PREFIX_PROP);
    }

    public String hadoopConfDir() {
        return getString(HADOOP_CONF_DIR_PROP);
    }
}
