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

package org.apache.iceberg.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AddFieldWithAlternatingValues<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Adds a new field to the record, cycling through 3 configured values.";

    private interface ConfigName {
        String FIELD_NAME = "field.name";
        String FIELD_VALUES = "field.values"; // comma-separated list of values
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "The name of the field to add.")
            .define(ConfigName.FIELD_VALUES, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Comma-separated list of 3 values to use alternately.");

    private String fieldName;
    private List<String> fieldValues;
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void configure(Map<String, ?> configs) {
        fieldName = (String) configs.get("field.name");
        String valuesConfig = (String) configs.get("field.values");

        fieldValues = Arrays.asList(valuesConfig.split("\\s*,\\s*"));
        if (fieldValues.size() != 3) {
            throw new IllegalArgumentException("You must specify exactly 3 values in field.values");
        }
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        // Pick value in round-robin fashion
        String chosenValue = fieldValues.get(counter.getAndIncrement() % fieldValues.size());

        if (record.valueSchema() == null) {
            // Schemaless case
            @SuppressWarnings("unchecked")
            Map<String, Object> valueMap = new HashMap<>((Map<String, Object>) record.value());
            valueMap.put(fieldName, chosenValue);
            return record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    null, valueMap,
                    record.timestamp());
        }

        // Schema-based case
        Schema valueSchema = record.valueSchema();
        Schema newSchema = makeUpdatedSchema(valueSchema);
        Struct valueStruct = (Struct) record.value();

        Struct newValueStruct = new Struct(newSchema);
        for (Field field : valueSchema.fields()) {
            newValueStruct.put(field.name(), valueStruct.get(field));
        }
        newValueStruct.put(fieldName, chosenValue);

        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                newSchema, newValueStruct,
                record.timestamp());
    }

    private Schema makeUpdatedSchema(Schema original) {
        SchemaBuilder builder = SchemaBuilder.struct().name(original.name() + "_augmented");
        for (Field field : original.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(fieldName, Schema.STRING_SCHEMA);
        if (original.isOptional()) {
            builder.optional();
        }
        builder.version(original.version());
        return builder.build();
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
