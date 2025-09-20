# Custom Kafka Connect Cluster Startup (No Confluent Dependencies)

## How Your Custom Images Start the Connect Cluster

The operator now starts Kafka Connect using **pure Apache Kafka components** without any Confluent dependencies.

### **1. Image Requirements**

Your S3 images must contain:
```
/app/
├── libs/                    # Kafka Connect and Kafka client JARs
│   ├── kafka-clients-*.jar
│   ├── connect-api-*.jar
│   ├── connect-runtime-*.jar
│   └── ...
├── kafka-libs/             # Additional Kafka JARs
│   └── *.jar
└── plugins/                # Connector JARs (optional, can use libs/)
    └── iceberg-kafka-connect-*.jar
```

### **2. Container Startup Command**

```yaml
containers:
- name: connect-worker
  image: <your-s3-downloaded-image>
  command: ['java']
  args: [
    '-Xmx1G', '-Xms1G',
    '-cp', '/app/libs/*:/app/kafka-libs/*',
    'org.apache.kafka.connect.cli.ConnectDistributed',
    '/app/config/connect-distributed.properties'
  ]
```

### **3. Configuration File Generation**

The operator creates `/app/config/connect-distributed.properties`:

```properties
# Kafka Connect Worker Configuration
bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092
group.id=connect-my-cluster
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

# Storage Topics
offset.storage.topic=connect-offsets
offset.storage.replication.factor=3
offset.storage.partitions=25
config.storage.topic=connect-configs
config.storage.replication.factor=3
status.storage.topic=connect-status
status.storage.replication.factor=3
status.storage.partitions=5

# Plugin Configuration
plugin.path=/app/plugins:/app/libs

# REST API
rest.port=8083
rest.advertised.port=8083

# Security
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";

# Performance Tuning
offset.flush.interval.ms=10000
offset.flush.timeout.ms=5000
task.shutdown.graceful.timeout.ms=10000

# Producer Configuration (Exactly-Once Semantics)
producer.bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092
producer.key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
producer.value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
producer.acks=all
producer.retries=2147483647
producer.enable.idempotence=true
producer.max.in.flight.requests.per.connection=1
producer.security.protocol=SASL_SSL
producer.sasl.mechanism=SCRAM-SHA-256
producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";

# Consumer Configuration (Exactly-Once Semantics)
consumer.bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092
consumer.key.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
consumer.value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
consumer.auto.offset.reset=earliest
consumer.enable.auto.commit=false
consumer.isolation.level=read_committed
consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=SCRAM-SHA-256
consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";
```

### **4. Complete Startup Flow**

```
┌─────────────────┐
│   S3 Bucket     │
│  ┌───────────┐  │
│  │Custom     │  │
│  │Worker Img │  │  <- Your image with Kafka libs
│  └───────────┘  │
└─────────┬───────┘
          │ Download & Push
          ▼
┌─────────────────┐
│    Operator     │
│ ┌─────────────┐ │
│ │Generate     │ │  <- Creates properties file
│ │ConfigMap    │ │
│ └─────────────┘ │
└─────────┬───────┘
          │ Create Deployment
          ▼
┌─────────────────┐
│   Worker Pods   │
│                 │
│ java -cp /app/  │  <- Direct Java execution
│   libs/*        │
│ ConnectDistrib. │
│ /app/config/    │
│ connect-dist.   │
│ properties      │
└─────────┬───────┘
          │ Connect to Kafka
          ▼
┌─────────────────┐
│ Kafka Connect   │
│ Distributed     │
│ Cluster         │
│ • Join cluster  │
│ • Create topics │
│ • Start REST    │
│ • Load plugins  │
└─────────────────┘
```

### **5. Key Differences from Confluent Approach**

| Aspect | Confluent | Your Custom Image |
|--------|-----------|-------------------|
| **Startup Script** | `/etc/confluent/docker/run` | Direct `java` command |
| **Configuration** | Environment variables → properties | ConfigMap → properties file |
| **Dependencies** | Confluent Platform | Pure Apache Kafka |
| **Plugin Path** | `/usr/share/confluent-hub-components` | `/app/plugins:/app/libs` |
| **JVM Options** | Confluent defaults | Your custom settings |

### **6. Building Your Custom Image**

Example Dockerfile for your worker image:
```dockerfile
FROM openjdk:11-jre-slim

# Copy Kafka Connect and dependencies
COPY kafka-libs/ /app/kafka-libs/
COPY connect-libs/ /app/libs/
COPY iceberg-connector.jar /app/plugins/

# Create config directory
RUN mkdir -p /app/config

WORKDIR /app
```

### **7. Building Your Coordinator Image**

Example Dockerfile for your coordinator:
```dockerfile
FROM openjdk:11-jre-slim

# Copy your coordinator JAR and dependencies
COPY iceberg-kafka-connect-coordinator.jar /app/
COPY libs/ /app/libs/

WORKDIR /app
```

### **8. Verification Commands**

After deployment:
```bash
# Check worker pods
kubectl get pods -l app=kafka-connect-workers

# Check generated config
kubectl get configmap kafka-connect-worker-config-my-cluster -o yaml

# Access REST API
kubectl port-forward service/kafka-connect-service-my-cluster 8083:8083
curl http://localhost:8083/

# Check connector plugins
curl http://localhost:8083/connector-plugins
```

### **9. Plugin Path Configuration**

You can customize the plugin path in your IcebergConnect spec:
```yaml
spec:
  worker:
    kafka:
      bootstrapServers: "kafka:9092"
      pluginPath: "/app/plugins:/app/libs:/custom/path"
```

This gives you **complete control** over your Connect cluster without any Confluent dependencies! 🚀

## **What Happens at Runtime**

1. **Container starts** with your custom image
2. **Java process launches** `ConnectDistributed` class
3. **Properties file loaded** from ConfigMap mount
4. **Connect worker joins** distributed cluster via `group.id`
5. **Internal topics created** (offsets, configs, status)
6. **REST API starts** on port 8083
7. **Plugins loaded** from your specified paths
8. **Ready to accept** connector configurations

**No Confluent code involved - pure Apache Kafka Connect!** ✨