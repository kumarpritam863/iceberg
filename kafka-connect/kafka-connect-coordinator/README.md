# Iceberg Kafka Connect Coordinator

A per-job coordinator for Iceberg Kafka Connect that decouples commit coordination from Kafka Connect task partition assignments, following the Flink JobManager/TaskManager architecture pattern.

## Architecture

## Features

- **Exactly-Once Semantics** - Transactional producer and consumer ensure no duplicate commits
- **One coordinator per job** - Each Iceberg job gets its own dedicated coordinator instance
- **Decoupled from partitions** - No dependency on Kafka Connect task partition assignments
- **Standalone deployment** - Runs as separate Kubernetes pods, similar to Flink JobManager
- **Event-driven coordination** - Uses existing kafka-connect-events for communication
- **Fault tolerance** - Automatic transaction abort and retry on failures

## Components

### IcebergJobManager
Per-job coordinator that manages commit cycles for a single Iceberg Kafka Connect job.

### IcebergJobConfig
Configuration for a specific job including catalog, Kafka, and coordinator settings.

### JobCommitState
Tracks commit state and manages the commit lifecycle for a job.

## Deployment

### Building the Image

```bash
# Build the project
./gradlew clean build

# Build Docker image
cd kafka-connect/kafka-connect-coordinator
docker build -t iceberg/kafka-connect-coordinator:latest .
```

### Kubernetes Deployment

#### 1. Deploy Job Manager (Coordinator)

```bash
# Set required environment variables
export KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
export CONNECT_GROUP_ID="my-iceberg-job"
export CONTROL_TOPIC="iceberg-control-topic"
export CATALOG_NAME="iceberg_catalog"
export CATALOG_CATALOG_IMPL="org.apache.iceberg.jdbc.JdbcCatalog"
export CATALOG_URI="jdbc:postgresql://postgres:5432/iceberg"
export CATALOG_JDBC_USER="iceberg"
export CATALOG_JDBC_PASSWORD="password"
export CATALOG_WAREHOUSE="s3://iceberg-warehouse/"

# Deploy job manager
./k8s/deploy-job-manager.sh
```

#### 2. Deploy Kafka Connect Workers

```bash
kubectl apply -f k8s/kafka-connect-workers.yaml
```

#### 3. Create Kafka Connect Connector

```bash
# Create the Iceberg sink connector
curl -X POST http://kafka-connect-service:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "iceberg-sink",
    "config": {
      "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
      "tasks.max": "3",
      "topics": "my-topic",
      "iceberg.control.topic": "iceberg-control-topic",
      "iceberg.control.group-id": "my-iceberg-job",
      "iceberg.catalog.name": "iceberg_catalog",
      "iceberg.catalog.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
      "iceberg.catalog.uri": "jdbc:postgresql://postgres:5432/iceberg",
      "iceberg.catalog.jdbc.user": "iceberg",
      "iceberg.catalog.jdbc.password": "password",
      "iceberg.catalog.warehouse": "s3://iceberg-warehouse/",
      "iceberg.tables": "db.table"
    }
  }'
```

## Configuration

### Environment Variables

#### Required
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka bootstrap servers
- `CONNECT_GROUP_ID` - Unique ID for this job (must match connector config)
- `CONTROL_TOPIC` - Control topic for coordination events
- `CATALOG_NAME` - Name of the Iceberg catalog

#### Catalog Configuration
Prefix catalog properties with `CATALOG_`:
- `CATALOG_CATALOG_IMPL` - Catalog implementation class
- `CATALOG_URI` - Catalog URI (for JDBC catalogs)
- `CATALOG_WAREHOUSE` - Warehouse location
- `CATALOG_JDBC_USER` - Database user (for JDBC catalogs)
- `CATALOG_JDBC_PASSWORD` - Database password (for JDBC catalogs)

#### Optional
- `JOB_ID` - Job identifier (auto-generated if not provided)
- `COMMIT_INTERVAL_MINUTES` - Commit interval in minutes (default: 5)
- `COMMIT_TIMEOUT_MINUTES` - Commit timeout in minutes (default: 30)
- `COMMIT_THREADS` - Number of commit threads (default: 4)

#### Transaction Configuration
- `ENABLE_TRANSACTIONS` - Enable transactional mode (default: true)
- `TRANSACTIONAL_ID` - Unique transactional ID (auto-generated if not provided)
- `TRANSACTION_TIMEOUT_MINUTES` - Transaction timeout in minutes (default: 15)

## Monitoring

### Health Checks
The job manager includes liveness and readiness probes that check if the application process is running.

### Logs
```bash
# View job manager logs
kubectl logs -n default -l job=<JOB_NAME> -f

# View worker logs
kubectl logs -n default -l app=kafka-connect-workers -f
```

### Cleanup
```bash
# Delete a specific job
kubectl delete deployment,service,configmap -n default -l job=<JOB_NAME>

# Delete all workers
kubectl delete -f k8s/kafka-connect-workers.yaml
```

## Comparison with Original Architecture

### Before (Partition-Based Coordinator)
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Task 0        │    │   Task 1        │    │   Task 2        │
│  (Coordinator)  │    │   (Worker)      │    │   (Worker)      │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Partition 0 │ │    │ │ Partition 1 │ │    │ │ Partition 2 │ │
│ │ Partition 3 │ │    │ │ Partition 4 │ │    │ │ Partition 5 │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### After (Per-Job Coordinator)
```
┌─────────────────────────────────────────────────────────────────┐
│                    Job Manager (Coordinator)                   │
│                     - Commit coordination                      │
│                     - Event processing                        │
│                     - Table commits                           │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Events
                                    ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Worker 0      │    │   Worker 1      │    │   Worker 2      │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Partition 0 │ │    │ │ Partition 1 │ │    │ │ Partition 2 │ │
│ │ Partition 3 │ │    │ │ Partition 4 │ │    │ │ Partition 5 │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Benefits

1. **Exactly-once semantics** - Transactional coordination prevents duplicate commits
2. **No rebalance disruption** - Coordinator doesn't restart when partitions rebalance
3. **Better resource isolation** - Coordinator has dedicated resources
4. **Simplified scaling** - Scale workers independently of coordination
5. **Enhanced reliability** - Coordinator failure doesn't affect partition processing
6. **Kubernetes native** - Follows cloud-native deployment patterns
7. **Automatic recovery** - Transaction failures are automatically retried