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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.channel.CommitterImpl;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCommitterFactory {

  private static final Map<String, String> BASE_PROPS =
      ImmutableMap.of(
          "iceberg.catalog.type", "rest",
          "topics", "source-topic",
          "iceberg.tables", "db.landing");

  @BeforeEach
  public void resetRecording() {
    RecordingCommitter.reset();
  }

  @Test
  public void createCommitter_defaultsWhenNotSet() {
    IcebergSinkConfig config = configWith(null);
    Committer committer = CommitterFactory.createCommitter(config);
    assertThat(committer).isInstanceOf(CommitterImpl.class);
  }

  @Test
  public void createCommitter_defaultsWhenEmpty() {
    IcebergSinkConfig config = configWith("");
    Committer committer = CommitterFactory.createCommitter(config);
    assertThat(committer).isInstanceOf(CommitterImpl.class);
  }

  @Test
  public void createCommitter_defaultsWhenWhitespace() {
    IcebergSinkConfig config = configWith("   ");
    Committer committer = CommitterFactory.createCommitter(config);
    assertThat(committer).isInstanceOf(CommitterImpl.class);
  }

  @Test
  public void createCommitter_loadsValidCustomClass() {
    IcebergSinkConfig config = configWith(NoOpCommitter.class.getName());
    Committer committer = CommitterFactory.createCommitter(config);
    assertThat(committer).isInstanceOf(NoOpCommitter.class);
  }

  @Test
  public void createCommitter_trimsWhitespaceAroundClassName() {
    IcebergSinkConfig config = configWith("  " + NoOpCommitter.class.getName() + "  ");
    Committer committer = CommitterFactory.createCommitter(config);
    assertThat(committer).isInstanceOf(NoOpCommitter.class);
  }

  @Test
  public void createCommitter_invokesConfigureExactlyOnce() {
    IcebergSinkConfig config = configWith(RecordingCommitter.class.getName());
    CommitterFactory.createCommitter(config);
    assertThat(RecordingCommitter.CONFIGURE_CALLS.get()).isEqualTo(1);
  }

  @Test
  public void createCommitter_passesSameConfigInstanceToConfigure() {
    IcebergSinkConfig config = configWith(RecordingCommitter.class.getName());
    CommitterFactory.createCommitter(config);
    assertThat(RecordingCommitter.lastConfig).isSameAs(config);
  }

  @Test
  public void createCommitter_exposesCommitterPropsWithPrefixStripped() {
    Map<String, String> props = Maps.newHashMap(BASE_PROPS);
    props.put(IcebergSinkConfig.COMMITTER_CLASS_PROP, RecordingCommitter.class.getName());
    props.put("iceberg.committer.foo", "bar");
    props.put("iceberg.committer.retry-count", "7");

    IcebergSinkConfig config = new IcebergSinkConfig(props);
    CommitterFactory.createCommitter(config);

    Map<String, String> exposed = RecordingCommitter.lastConfig.committerProps();
    assertThat(exposed).containsEntry("foo", "bar").containsEntry("retry-count", "7");
    assertThat(exposed).doesNotContainKey("iceberg.committer.foo");
  }

  @Test
  public void createCommitter_classNotFoundFailsWithConfigException() {
    IcebergSinkConfig config = configWith("com.example.NonExistentCommitter");
    assertThatThrownBy(() -> CommitterFactory.createCommitter(config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("com.example.NonExistentCommitter")
        .hasMessageContaining("public no-arg constructor");
  }

  @Test
  public void createCommitter_wrongTypeFailsWithConfigException() {
    IcebergSinkConfig config = configWith(NotACommitter.class.getName());
    assertThatThrownBy(() -> CommitterFactory.createCommitter(config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(NotACommitter.class.getName())
        .hasMessageContaining(Committer.class.getName());
  }

  @Test
  public void createCommitter_missingNoArgConstructorFailsWithConfigException() {
    IcebergSinkConfig config = configWith(BadCtorCommitter.class.getName());
    assertThatThrownBy(() -> CommitterFactory.createCommitter(config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(BadCtorCommitter.class.getName())
        .hasMessageContaining("public no-arg constructor");
  }

  @Test
  public void createCommitter_constructorThrowingPropagates() {
    IcebergSinkConfig config = configWith(ThrowingCtorCommitter.class.getName());
    // Constructor exceptions are surfaced unwrapped via DynConstructors, not caught by the
    // factory. The original RuntimeException should reach the caller so the full failure cause
    // is visible in Connect logs.
    assertThatThrownBy(() -> CommitterFactory.createCommitter(config))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ctor failed");
  }

  @Test
  public void createCommitter_configureThrowingWrappedInConfigException() {
    IcebergSinkConfig config = configWith(ThrowingConfigureCommitter.class.getName());
    assertThatThrownBy(() -> CommitterFactory.createCommitter(config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(ThrowingConfigureCommitter.class.getName())
        .hasMessageContaining("configure failed");
  }

  @Test
  public void createCommitter_nullConfigThrowsNullPointerException() {
    assertThatThrownBy(() -> CommitterFactory.createCommitter(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("IcebergSinkConfig");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static IcebergSinkConfig configWith(String committerClass) {
    Map<String, String> props = Maps.newHashMap(BASE_PROPS);
    if (committerClass != null) {
      props.put(IcebergSinkConfig.COMMITTER_CLASS_PROP, committerClass);
    }
    return new IcebergSinkConfig(props);
  }

  // ---------------------------------------------------------------------------
  // Fixture committers
  // ---------------------------------------------------------------------------

  public static class NoOpCommitter implements Committer {
    @Override
    public void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {}

    @Override
    public void stop() {}

    @Override
    public void save(Collection<SinkRecord> sinkRecords) {}
  }

  public static class RecordingCommitter implements Committer {
    static final AtomicInteger CONFIGURE_CALLS = new AtomicInteger();
    static volatile IcebergSinkConfig lastConfig;

    static void reset() {
      CONFIGURE_CALLS.set(0);
      lastConfig = null;
    }

    @Override
    public void configure(IcebergSinkConfig config) {
      CONFIGURE_CALLS.incrementAndGet();
      lastConfig = config;
    }

    @Override
    public void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {}

    @Override
    public void stop() {}

    @Override
    public void save(Collection<SinkRecord> sinkRecords) {}
  }

  public static class BadCtorCommitter implements Committer {
    @SuppressWarnings("unused")
    public BadCtorCommitter(String arg) {}

    @Override
    public void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {}

    @Override
    public void stop() {}

    @Override
    public void save(Collection<SinkRecord> sinkRecords) {}
  }

  public static class ThrowingCtorCommitter implements Committer {
    public ThrowingCtorCommitter() {
      throw new IllegalStateException("ctor failed");
    }

    @Override
    public void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {}

    @Override
    public void stop() {}

    @Override
    public void save(Collection<SinkRecord> sinkRecords) {}
  }

  public static class ThrowingConfigureCommitter implements Committer {
    @Override
    public void configure(IcebergSinkConfig config) {
      throw new IllegalStateException("configure failed");
    }

    @Override
    public void start(Catalog catalog, IcebergSinkConfig config, SinkTaskContext context) {}

    @Override
    public void stop() {}

    @Override
    public void save(Collection<SinkRecord> sinkRecords) {}
  }

  public static class NotACommitter {
    public NotACommitter() {}
  }
}
