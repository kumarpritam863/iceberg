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

import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.connect.channel.CommitterImpl;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads and configures the {@link Committer} for an {@code IcebergSinkTask}.
 *
 * <p>If {@code iceberg.committer.class} is set, that fully qualified class is loaded reflectively
 * (it must implement {@link Committer} and expose a public no-arg constructor). If it is unset,
 * blank, or whitespace, the built-in {@link CommitterImpl} is used.
 *
 * <p>After instantiation, {@link Committer#configure(IcebergSinkConfig)} is invoked exactly once
 * before the committer is handed back to the task. All failure modes — class missing, wrong type,
 * no usable constructor, configure throws — surface as {@link ConfigException} with the offending
 * class name in the message so they appear cleanly in Kafka Connect's task-failure output.
 */
class CommitterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterFactory.class);

  private CommitterFactory() {}

  static Committer createCommitter(IcebergSinkConfig config) {
    Preconditions.checkNotNull(config, "IcebergSinkConfig cannot be null");

    String configured = config.committerClass();
    String className = (configured == null) ? null : configured.trim();

    Committer committer;
    if (className == null || className.isEmpty()) {
      LOG.info("No custom committer configured; using default CommitterImpl");
      committer = new CommitterImpl();
    } else {
      committer = loadCustomCommitter(className);
    }

    configureCommitter(committer, config);
    return committer;
  }

  private static Committer loadCustomCommitter(String className) {
    LOG.info("Loading custom Committer implementation: {}", className);
    DynConstructors.Ctor<Committer> ctor;
    try {
      ctor = DynConstructors.builder(Committer.class).impl(className).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new ConfigException(
          String.format(
              "Cannot initialize Committer '%s': class not found or missing a public no-arg "
                  + "constructor. Ensure the class is on the connector plugin classpath and "
                  + "declares a public no-arg constructor. Details: %s",
              className, e.getMessage()),
          e);
    }

    try {
      return ctor.newInstance();
    } catch (ClassCastException e) {
      throw new ConfigException(
          String.format(
              "Cannot initialize Committer: '%s' does not implement %s",
              className, Committer.class.getName()),
          e);
    }
  }

  private static void configureCommitter(Committer committer, IcebergSinkConfig config) {
    try {
      committer.configure(config);
    } catch (RuntimeException e) {
      throw new ConfigException(
          String.format(
              "Failed to configure Committer '%s': %s",
              committer.getClass().getName(), e.getMessage()),
          e);
    }
  }
}
