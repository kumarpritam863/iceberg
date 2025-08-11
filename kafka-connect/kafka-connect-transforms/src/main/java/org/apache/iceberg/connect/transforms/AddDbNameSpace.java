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

package org.apache.iceberg.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public class AddDbNameSpace<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_NAME_CONFIG = "field.name";
    public static final String PREFIX_CONFIG = "prefix";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Field to modify")
            .define(PREFIX_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Prefix to add");

    private String fieldName;
    private String prefix;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.fieldName = config.getString(FIELD_NAME_CONFIG);
        this.prefix = config.getString(PREFIX_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        if (record.value() instanceof Struct) {
            Struct valueStruct = (Struct) record.value();
            Object originalValue = valueStruct.get(fieldName);
            if (originalValue != null) {
                valueStruct.put(fieldName, prefix + "." + originalValue);
            }
            return record;
        }

        // For schemaless Map values
        if (record.value() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> valueMap = (Map<String, Object>) record.value();
            Object originalValue = valueMap.get(fieldName);
            if (originalValue != null) {
                valueMap.put(fieldName, prefix + "." + originalValue);
            }
            return record;
        }

        return record;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // Nothing to close
    }
}



