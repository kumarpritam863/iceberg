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

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.errors.DataException;

import java.util.Map;

public class AddDBPrefix<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_CONFIG = "field";
    public static final String PREFIX_CONFIG = "prefix";

    private String fieldName;
    private String prefix;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldName = config.getString(FIELD_CONFIG);
        prefix = config.getString(PREFIX_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (!(record.value() instanceof Struct)) {
            return record; // skip non-structs
        }

        Struct value = (Struct) record.value();
        Schema schema = value.schema();

        Object originalValue = value.get(fieldName);
        if (originalValue == null) {
            return record; // skip if field missing or null
        }

        if (!(originalValue instanceof String)) {
            throw new DataException("Field '" + fieldName + "' is not a String");
        }

        String newValue = prefix + "." + originalValue;
        Struct updatedValue = new Struct(schema);

        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            if (field.name().equals(fieldName)) {
                updatedValue.put(field.name(), newValue);
            } else {
                updatedValue.put(field.name(), value.get(field));
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                schema,
                updatedValue,
                record.timestamp()
        );
    }

    @Override
    public void close() {
        // No resources
    }

    public static final org.apache.kafka.common.config.ConfigDef CONFIG_DEF = new org.apache.kafka.common.config.ConfigDef()
            .define(FIELD_CONFIG, org.apache.kafka.common.config.ConfigDef.Type.STRING, org.apache.kafka.common.config.ConfigDef.Importance.HIGH,
                    "Name of the field to modify")
            .define(PREFIX_CONFIG, org.apache.kafka.common.config.ConfigDef.Type.STRING, org.apache.kafka.common.config.ConfigDef.Importance.HIGH,
                    "Prefix to add before the field value");

    @Override
    public org.apache.kafka.common.config.ConfigDef config() {
        return CONFIG_DEF;
    }
}

