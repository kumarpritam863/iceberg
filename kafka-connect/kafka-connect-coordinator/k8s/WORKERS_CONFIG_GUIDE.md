# Kafka Connect Workers Configuration Guide

## Overview

The Kafka Connect workers deployment is now fully configurable through environment variables. This allows you to customize every aspect of the worker configuration without modifying YAML files.

## Quick Start

### 1. Deploy Basic Workers

```bash
# Set required configuration
export KAFKA_BOOTSTRAP_SERVERS="kafka:9092"

# Deploy with defaults
./k8s/deploy-workers.sh
```

### 2. Deploy Production Workers

```bash
# Required
export KAFKA_BOOTSTRAP_SERVERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"

# Cluster Configuration
export WORKER_CLUSTER_NAME="production-connect"
export NAMESPACE="kafka-connect"
export REPLICAS=5

# Storage Topics (Production Settings)
export OFFSET_STORAGE_TOPIC="connect-offsets"
export OFFSET_STORAGE_REPLICATION_FACTOR=3
export OFFSET_STORAGE_PARTITIONS=25
export CONFIG_STORAGE_TOPIC="connect-configs"
export CONFIG_STORAGE_REPLICATION_FACTOR=3
export STATUS_STORAGE_TOPIC="connect-status"
export STATUS_STORAGE_REPLICATION_FACTOR=3
export STATUS_STORAGE_PARTITIONS=5

# Performance Tuning
export WORKER_MEMORY_REQUEST="2Gi"
export WORKER_MEMORY_LIMIT="4Gi"
export WORKER_CPU_REQUEST="1000m"
export WORKER_CPU_LIMIT="2000m"

# Deploy
./k8s/deploy-workers.sh
```

## Configuration Reference

### Required Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `kafka:9092` |

### Basic Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NAMESPACE` | `default` | Kubernetes namespace |
| `WORKER_CLUSTER_NAME` | `kafka-connect-cluster` | Unique cluster identifier |
| `REPLICAS` | `3` | Number of worker replicas |
| `IMAGE_TAG` | `latest` | Container image tag |
| `KAFKA_CONNECT_IMAGE` | `confluentinc/cp-kafka-connect` | Container image |

### Connect Framework Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CONNECT_GROUP_ID` | `connect-cluster` | Connect cluster group ID |
| `KEY_CONVERTER` | `JsonConverter` | Key converter class |
| `VALUE_CONVERTER` | `JsonConverter` | Value converter class |
| `KEY_CONVERTER_SCHEMAS_ENABLE` | `false` | Enable key schemas |
| `VALUE_CONVERTER_SCHEMAS_ENABLE` | `false` | Enable value schemas |

### Storage Topics Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `OFFSET_STORAGE_TOPIC` | `connect-offsets` | Offset storage topic |
| `OFFSET_STORAGE_REPLICATION_FACTOR` | `1` | Offset topic replication |
| `OFFSET_STORAGE_PARTITIONS` | `25` | Offset topic partitions |
| `CONFIG_STORAGE_TOPIC` | `connect-configs` | Config storage topic |
| `CONFIG_STORAGE_REPLICATION_FACTOR` | `1` | Config topic replication |
| `STATUS_STORAGE_TOPIC` | `connect-status` | Status storage topic |
| `STATUS_STORAGE_REPLICATION_FACTOR` | `1` | Status topic replication |
| `STATUS_STORAGE_PARTITIONS` | `5` | Status topic partitions |

### Security Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SECURITY_PROTOCOL` | `PLAINTEXT` | Security protocol |
| `SASL_MECHANISM` | - | SASL mechanism (optional) |
| `SASL_JAAS_CONFIG` | - | SASL JAAS config (optional) |

### Performance Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `OFFSET_FLUSH_INTERVAL_MS` | `10000` | Offset flush interval |
| `OFFSET_FLUSH_TIMEOUT_MS` | `5000` | Offset flush timeout |
| `TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS` | `10000` | Task shutdown timeout |

### Producer Configuration (Exactly-Once)

| Variable | Default | Description |
|----------|---------|-------------|
| `PRODUCER_ACKS` | `all` | Producer acknowledgment |
| `PRODUCER_RETRIES` | `2147483647` | Producer retries |
| `PRODUCER_ENABLE_IDEMPOTENCE` | `true` | Enable idempotence |
| `PRODUCER_MAX_IN_FLIGHT_REQUESTS` | `1` | Max in-flight requests |

### Consumer Configuration (Exactly-Once)

| Variable | Default | Description |
|----------|---------|-------------|
| `CONSUMER_AUTO_OFFSET_RESET` | `earliest` | Auto offset reset |
| `CONSUMER_ENABLE_AUTO_COMMIT` | `false` | Enable auto commit |
| `CONSUMER_ISOLATION_LEVEL` | `read_committed` | Isolation level |

### Resource Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_MEMORY_REQUEST` | `1Gi` | Memory request |
| `WORKER_MEMORY_LIMIT` | `2Gi` | Memory limit |
| `WORKER_CPU_REQUEST` | `500m` | CPU request |
| `WORKER_CPU_LIMIT` | `1000m` | CPU limit |

## Configuration Examples

### High-Throughput Setup

```bash
export KAFKA_BOOTSTRAP_SERVERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"
export WORKER_CLUSTER_NAME="high-throughput"
export REPLICAS=6

# High-performance storage
export OFFSET_STORAGE_PARTITIONS=50
export STATUS_STORAGE_PARTITIONS=10

# Aggressive flushing
export OFFSET_FLUSH_INTERVAL_MS=5000
export OFFSET_FLUSH_TIMEOUT_MS=2000

# More resources
export WORKER_MEMORY_REQUEST="4Gi"
export WORKER_MEMORY_LIMIT="8Gi"
export WORKER_CPU_REQUEST="2000m"
export WORKER_CPU_LIMIT="4000m"

./k8s/deploy-workers.sh
```

### Secure Setup with SASL

```bash
export KAFKA_BOOTSTRAP_SERVERS="secure-kafka:9093"
export WORKER_CLUSTER_NAME="secure-connect"

# Security configuration
export SECURITY_PROTOCOL="SASL_SSL"
export SASL_MECHANISM="SCRAM-SHA-256"
export SASL_JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"connect-user\" password=\"connect-password\";"

# High replication for security
export OFFSET_STORAGE_REPLICATION_FACTOR=3
export CONFIG_STORAGE_REPLICATION_FACTOR=3
export STATUS_STORAGE_REPLICATION_FACTOR=3

./k8s/deploy-workers.sh
```

### Development Setup

```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export WORKER_CLUSTER_NAME="dev-connect"
export NAMESPACE="development"
export REPLICAS=1

# Minimal resources
export WORKER_MEMORY_REQUEST="512Mi"
export WORKER_MEMORY_LIMIT="1Gi"
export WORKER_CPU_REQUEST="250m"
export WORKER_CPU_LIMIT="500m"

# Single partition topics
export OFFSET_STORAGE_PARTITIONS=1
export STATUS_STORAGE_PARTITIONS=1

./k8s/deploy-workers.sh
```

## Management Commands

### View Worker Status
```bash
kubectl get pods -n $NAMESPACE -l cluster=$WORKER_CLUSTER_NAME
kubectl logs -n $NAMESPACE -l cluster=$WORKER_CLUSTER_NAME
```

### Access REST API
```bash
kubectl port-forward -n $NAMESPACE service/kafka-connect-service-$WORKER_CLUSTER_NAME 8083:8083
curl http://localhost:8083/connectors
```

### Scale Workers
```bash
kubectl scale deployment kafka-connect-workers-$WORKER_CLUSTER_NAME --replicas=5 -n $NAMESPACE
```

### Delete Worker Cluster
```bash
kubectl delete deployment,service,configmap -n $NAMESPACE -l cluster=$WORKER_CLUSTER_NAME
```

## Integration with Job Manager

The workers are designed to work seamlessly with the per-job coordinators:

1. **Deploy Workers** with shared cluster configuration
2. **Deploy Job Managers** with specific job configurations
3. **Create Connectors** via REST API that reference both

```bash
# 1. Deploy shared workers
export KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
export WORKER_CLUSTER_NAME="shared-workers"
./k8s/deploy-workers.sh

# 2. Deploy job-specific coordinator
export CONNECT_GROUP_ID="my-iceberg-job"
export CONTROL_TOPIC="iceberg-control-topic"
./k8s/deploy-job-manager.sh

# 3. Create connector
curl -X POST http://kafka-connect-service-shared-workers:8083/connectors \\
  -H "Content-Type: application/json" \\
  -d '{
    "name": "iceberg-sink",
    "config": {
      "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
      "tasks.max": "3",
      "topics": "my-topic",
      "iceberg.control.topic": "iceberg-control-topic",
      "iceberg.control.group-id": "my-iceberg-job"
    }
  }'
```

This approach provides maximum flexibility while maintaining the exactly-once semantics and per-job coordination architecture!