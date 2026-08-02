# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Iceberg is an open table format for huge analytic datasets. This is a large multi-module Gradle project (Java 17+) with integrations for Spark, Flink, Hive, and various cloud storage systems.

## Build Commands

```bash
# Full build with tests
./gradlew build

# Build without tests (fast)
./gradlew build -x test -x integrationTest

# Quick mode: skip checkstyle and error-prone
./gradlew build -Pquick=true

# Build specific engine versions only
./gradlew -DsparkVersions=3.5,4.1 -DflinkVersions=2.1 build

# Build core only (no Spark/Flink/Kafka)
./gradlew check -DsparkVersions= -DflinkVersions= -DkafkaVersions= -Pquick=true
```

## Testing

```bash
# Run all tests for a module
./gradlew :iceberg-core:test

# Run a single test class
./gradlew :iceberg-core:test --tests org.apache.iceberg.TestSchema

# Run a single test method
./gradlew :iceberg-core:test --tests org.apache.iceberg.TestSchema.testMethodName

# Integration tests (require Docker)
./gradlew :iceberg-core:integrationTest
```

Tests use **JUnit 5** and **AssertJ** for assertions. Use `assertThat()` fluent assertions, not JUnit assert methods. Use **Awaitility** for async polling instead of `Thread.sleep()`.

## Code Style

- **Google Java Format** enforced via Spotless
- Check: `./gradlew spotlessCheck`
- Fix: `./gradlew spotlessApply`
- Fix all modules (including all Spark/Flink versions): `./gradlew spotlessApply -DallModules`
- Apache License header required on all source files
- Scala code uses scalafmt

## Module Structure

**Core**: `iceberg-api` (public API, strict backward compat via RevAPI), `iceberg-core`, `iceberg-common`, `iceberg-data`

**File formats**: `iceberg-parquet`, `iceberg-orc`, `iceberg-arrow`

**Engine integrations**: `iceberg-spark` (3.4, 3.5, 4.0, 4.1), `iceberg-flink` (1.20, 2.0, 2.1), `iceberg-mr`, `iceberg-kafka-connect`

**Cloud bundles**: `iceberg-aws`, `iceberg-gcp`, `iceberg-azure`, `iceberg-aliyun`, `iceberg-dell`

**Catalogs**: `iceberg-hive-metastore`, `iceberg-nessie`, `iceberg-open-api` (REST catalog), `iceberg-snowflake`, `iceberg-bigquery`

Module naming convention: Gradle uses `:iceberg-<module>` (e.g., `:iceberg-core`). Versioned engine modules nest under their parent (e.g., `spark/v4.1/spark/` with project path `:iceberg-spark:iceberg-spark-4.1`).

## API Compatibility

Changes to `iceberg-api` are checked by RevAPI (`./gradlew revapi`). Public API changes require a deprecation cycle. Deprecation annotations must include javadoc with removal version:
```java
/** @deprecated since 1.x.0, will be removed in 1.y.0; use alternative() instead. */
@Deprecated
```

## PR Conventions

- Title prefixes: `Core:`, `API:`, `Spark:`, `Flink:`, `Build:`, `Docs:`, etc.
- Include `Closes #1234` to auto-close issues
- Specification changes (under `format/`, `open-api/rest-catalog*`) require a vote
- Public API additions require 24-hour review wait

## Key Build Files

- `build.gradle` — root build configuration and dependency versions
- `settings.gradle` — module definitions and version matrix logic
- `baseline.gradle` — checkstyle, error-prone, and spotless configuration
- `gradle.properties` — default engine versions and Gradle settings

## Docker Note

Some integration tests require Docker. On macOS you may need:
```bash
sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock
```
