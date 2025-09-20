# Iceberg Kafka Connect Coordinator - Build Fixes

## Issues Resolution

### 1. Jackson Datatype JSR310 Issue
**Error:**
```
No such property: datatype for class: org.gradle.accessors.dm.LibrariesForLibs$JacksonLibraryAccessors
```

**Root Cause:** `libs.jackson.datatype.jsr310` doesn't exist in version catalog.

**✅ Fixed:** Direct dependency with BOM-managed version:
```gradle
implementation 'com.fasterxml.jackson.datatype:jackson-datatype-jsr310'
```

### 2. JUnit BOM Issue
**Error:**
```
No such property: bom for class: org.gradle.accessors.dm.LibrariesForLibs$JunitLibraryAccessors
```

**Root Cause:** `libs.junit.bom` doesn't exist in version catalog.

**✅ Fixed:** Use available JUnit libraries:
```gradle
// Instead of: platform(libs.junit.bom), libs.junit.jupiter.api
testImplementation libs.junit.jupiter         // Available: junit-jupiter
testImplementation libs.junit.jupiter.engine  // Available: junit-jupiter-engine
```

## Available Libraries in Version Catalog

### Jackson Libraries:
- ✅ `libs.jackson.bom` (platform)
- ✅ `libs.jackson.core`
- ✅ `libs.jackson.databind`
- ✅ `libs.jackson.annotations`
- ❌ `libs.jackson.datatype.jsr310` (not available)

### JUnit Libraries:
- ✅ `libs.junit.jupiter`
- ✅ `libs.junit.jupiter.engine`
- ✅ `libs.junit.platform.launcher`
- ❌ `libs.junit.bom` (not available)
- ❌ `libs.junit.jupiter.api` (not available)

### Test Libraries:
- ✅ `libs.mockito.core`
- ✅ `libs.mockito.junit.jupiter`
- ✅ `libs.assertj.core`

## Final Working Configuration

```gradle
dependencies {
  // Core dependencies
  implementation project(':iceberg-api')
  implementation project(':iceberg-core')
  implementation project(':iceberg-common')
  implementation project(':iceberg-kafka-connect:iceberg-kafka-connect-events')
  implementation project(path: ':iceberg-bundled-guava', configuration: 'shadow')

  // Jackson with JSR310 support
  implementation platform(libs.jackson.bom)
  implementation libs.jackson.core
  implementation libs.jackson.databind
  implementation 'com.fasterxml.jackson.datatype:jackson-datatype-jsr310'

  // Kafka
  implementation libs.kafka.clients
  implementation libs.slf4j.api

  // Testing
  testImplementation libs.junit.jupiter
  testImplementation libs.junit.jupiter.engine
  testImplementation libs.mockito.core
  testImplementation libs.mockito.junit.jupiter
  testImplementation libs.assertj.core
}
```

## Why These Dependencies Are Needed

1. **JSR310 Module**: Required for `OffsetDateTime` serialization in JobConfig, JobCommitState
2. **JUnit Jupiter**: Modern JUnit 5 testing framework
3. **Mockito**: Mocking framework for unit tests
4. **AssertJ**: Fluent assertion library for readable tests

The build should now work correctly! 🚀