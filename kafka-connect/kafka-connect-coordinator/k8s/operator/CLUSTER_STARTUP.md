# Kafka Connect Cluster Startup Process

## How the Connect Cluster is Started

The Kafka Connect cluster startup happens through the following sequence:

### 1. **Operator Downloads Images from S3**
```python
# operator.py - download_and_build_images()
s3_client.download_file(s3_config['bucket'], s3_path, str(local_file))
await self.run_command(['docker', 'load', '-i', str(local_file)])
await self.run_command(['docker', 'tag', source_image, target_image])
await self.run_command(['docker', 'push', target_image])
```

### 2. **Operator Creates ConfigMaps**
```python
# Creates ConfigMap with all Kafka Connect configuration
config_data = {
    'bootstrap.servers': kafka_config['bootstrapServers'],
    'group.id': f"connect-{cluster_name}",
    'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
    'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
    # ... all other Connect properties
}
```

### 3. **Operator Creates Worker Deployment**
```yaml
# Generated Deployment spec
containers:
- name: connect-worker
  image: <downloaded-and-pushed-image>
  command: ['/etc/confluent/docker/run']  # Confluent's Connect startup script
  ports:
  - containerPort: 8083
    name: rest-api
  env:
  - name: CONNECT_BOOTSTRAP_SERVERS
    valueFrom:
      configMapKeyRef:
        name: kafka-connect-worker-config-{cluster}
        key: bootstrap.servers
  # ... all environment variables from ConfigMap
```

### 4. **Container Startup Process**

When each worker pod starts:

```bash
# Inside the container, /etc/confluent/docker/run executes:
#!/bin/bash

# 1. Set up Kafka Connect properties from environment variables
echo "bootstrap.servers=$CONNECT_BOOTSTRAP_SERVERS" > /etc/kafka/connect-distributed.properties
echo "group.id=$CONNECT_GROUP_ID" >> /etc/kafka/connect-distributed.properties
echo "key.converter=$CONNECT_KEY_CONVERTER" >> /etc/kafka/connect-distributed.properties
# ... all other properties

# 2. Start Kafka Connect in distributed mode
exec /usr/bin/connect-distributed /etc/kafka/connect-distributed.properties
```

### 5. **Connect Worker Process**

Each worker then:
1. **Connects to Kafka** using `bootstrap.servers`
2. **Joins the Connect cluster** using `group.id`
3. **Creates internal topics** (offsets, configs, status) if they don't exist
4. **Starts REST API** on port 8083
5. **Begins task coordination** with other workers

### 6. **Coordinator Startup** (Per-Job)

The coordinator starts separately:

```bash
# Inside coordinator container
java -cp /app/libs/*:/app/iceberg-kafka-connect-coordinator.jar \
  org.apache.iceberg.connect.coordinator.IcebergJobManagerApplication
```

## Complete Startup Flow Diagram

```
┌─────────────────┐
│   S3 Bucket     │
│  ┌───────────┐  │
│  │Worker Img │  │
│  │Coord. Img │  │
│  └───────────┘  │
└─────────┬───────┘
          │ Download
          ▼
┌─────────────────┐
│    Operator     │
│ ┌─────────────┐ │
│ │Build & Push │ │
│ │ConfigMaps   │ │
│ │Deployments  │ │
│ └─────────────┘ │
└─────────┬───────┘
          │ Create K8s Resources
          ▼
┌─────────────────┐    ┌─────────────────┐
│   Worker Pods   │    │Coordinator Pods │
│                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │/etc/confluent│ │    │ │java -cp ... │ │
│ │/docker/run  │ │    │ │JobManager   │ │
│ └─────────────┘ │    │ └─────────────┘ │
│       │         │    └─────────────────┘
│       ▼         │
│ ┌─────────────┐ │
│ │connect-     │ │
│ │distributed  │ │
│ │:8083        │ │
│ └─────────────┘ │
└─────────────────┘
          │
          ▼
┌─────────────────┐
│  Kafka Cluster  │
│ ┌─────────────┐ │
│ │join cluster │ │
│ │create topics│ │
│ │start tasks  │ │
│ └─────────────┘ │
└─────────────────┘
```

## Environment Variables that Control Startup

The operator sets these critical environment variables for worker startup:

```bash
# Core Connect Configuration
CONNECT_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092
CONNECT_GROUP_ID=connect-my-cluster
CONNECT_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter
CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter

# Storage Topics
CONNECT_OFFSET_STORAGE_TOPIC=connect-offsets
CONNECT_CONFIG_STORAGE_TOPIC=connect-configs
CONNECT_STATUS_STORAGE_TOPIC=connect-status

# REST API
CONNECT_REST_PORT=8083
CONNECT_REST_ADVERTISED_HOST_NAME=<pod-ip>

# Plugin Path (where connectors are loaded from)
CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components

# Exactly-Once Producer Settings
CONNECT_PRODUCER_ACKS=all
CONNECT_PRODUCER_ENABLE_IDEMPOTENCE=true
CONNECT_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION=1

# Exactly-Once Consumer Settings
CONNECT_CONSUMER_ENABLE_AUTO_COMMIT=false
CONNECT_CONSUMER_ISOLATION_LEVEL=read_committed
```

## Verification Commands

Once deployed, you can verify the cluster is running:

```bash
# Check worker pods are running
kubectl get pods -l app=kafka-connect-workers

# Check coordinator pods are running
kubectl get pods -l app=iceberg-job-manager

# Access Connect REST API
kubectl port-forward service/kafka-connect-service-my-cluster 8083:8083

# Check cluster status
curl http://localhost:8083/
curl http://localhost:8083/connectors

# Check Connect cluster info
curl http://localhost:8083/connectors/cluster
```

## Key Points

1. **Distributed Mode**: Workers start in distributed mode, not standalone
2. **Automatic Discovery**: Workers find each other via Kafka group coordination
3. **REST API**: Each worker exposes REST API, but they coordinate internally
4. **Plugin Loading**: Connectors loaded from `/usr/share/confluent-hub-components`
5. **Per-Job Coordination**: Coordinators run separately, one per job
6. **Exactly-Once**: Configured for transactional semantics by default

The operator handles all the complexity of configuring and starting the distributed Connect cluster correctly!