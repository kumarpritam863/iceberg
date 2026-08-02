# Zero-copy proof artifacts

Parked here, outside any Gradle source set, so none of it runs as part of a normal build.

| file | what it is |
|---|---|
| `TestZeroCopyWriterIntegration.java` | 10 tests. Writes an OCF via `appendEncoded`, commits with partial metrics, reads back through Iceberg. Package `org.apache.iceberg.data.avro`. |
| `TestSkipPathComposition.java` | 13 tests. Probes which `ValueReaders` skips are genuine vs read-and-discard. Package `org.apache.iceberg.avro`. |
| `phase0-jmh-output.txt` | Raw JMH output for the Phase 0 benchmark (§1 of the design doc). |

The Phase 0 benchmark itself is **not** here — it is a permanent part of the build at
`kafka-connect/kafka-connect/src/jmh/java/org/apache/iceberg/connect/data/ZeroCopyAvroBenchmark.java`.

## Running the parked tests

Each declares a package, so it must sit in the matching directory to compile. Copy it in, run, remove:

```bash
cp kafka-connect/zero-copy-proof/TestZeroCopyWriterIntegration.java \
   data/src/test/java/org/apache/iceberg/data/avro/

JAVA_HOME=$(/usr/libexec/java_home -v 17) ./gradlew :iceberg-data:test \
  --tests 'org.apache.iceberg.data.avro.TestZeroCopyWriterIntegration' \
  -Pquick=true -DsparkVersions= -DflinkVersions= -DkafkaVersions=
```

`TestSkipPathComposition.java` goes to `core/src/test/java/org/apache/iceberg/avro/` and runs against
`:iceberg-core:test`.

Verified on this tree (`22714f023a`), JDK 17: `TestZeroCopyWriterIntegration` → `tests="10"
skipped="0" failures="0" errors="0"`.

## Running the Phase 0 benchmark

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) ./gradlew \
  :iceberg-kafka-connect:iceberg-kafka-connect:jmh \
  -PjmhIncludeRegex=ZeroCopyAvroBenchmark -DsparkVersions= -DflinkVersions=
```

Note the default `./gradlew` here picks up a Java 8 JVM and fails during configuration, so the
explicit `JAVA_HOME` is required in all of the above. JMH additionally refuses to run on anything
other than JDK 17 or 21.
