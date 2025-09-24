# Complete Iceberg Kafka Connect HA Coordinator Configuration Sample

This document provides complete, working configuration examples for deploying Iceberg Kafka Connect with HA coordinator.

## Prerequisites

1. **Kubernetes cluster** with Kafka Connect running
2. **RBAC permissions** for Kafka Connect service account to manage coordinator resources
3. **Iceberg catalog** (S3, ADLS, GCS, or Hadoop) properly configured
4. **Kafka topics** for data ingestion and control messages

## Required RBAC Setup

First, ensure your Kafka Connect service account has the necessary permissions:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kafka-connect-sa
  namespace: kafka-connect
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: kafka-connect
  name: iceberg-coordinator-role
rules:
# Core resources for coordinator deployment
- apiGroups: [""]
  resources: ["pods", "configmaps", "services"]
  verbs: ["get", "list", "create", "update", "patch", "delete", "watch"]
# Apps resources for deployments
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "list", "create", "update", "patch", "delete", "watch"]
# Coordination resources for leader election
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "create", "update", "patch", "delete", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: iceberg-coordinator-binding
  namespace: kafka-connect
subjects:
- kind: ServiceAccount
  name: kafka-connect-sa
  namespace: kafka-connect
roleRef:
  kind: Role
  name: iceberg-coordinator-role
  apiGroup: rbac.authorization.k8s.io
```

## Sample 1: Basic HA Configuration (S3 + Glue Catalog)

```json
{
  "name": "orders-iceberg-connector",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnectorV2",
    "tasks.max": "3",
    "topics": "orders,order_items",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "iceberg.connect.group-id": "orders-connector-group",
    "iceberg.control.topic": "iceberg-control-orders",
    "iceberg.control.commit.interval-ms": "300000",
    "iceberg.control.commit.timeout-ms": "30000",

    "iceberg.catalog": "glue",
    "iceberg.catalog.type": "glue",
    "iceberg.catalog.warehouse": "s3://my-data-lake/warehouse/",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "https://s3.us-west-2.amazonaws.com",
    "iceberg.catalog.glue.region": "us-west-2",

    "iceberg.tables": "ecommerce.orders,ecommerce.order_items",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.tables.default-partition-by": "day(created_at)",

    "iceberg.coordinator.replicas": "3",
    "iceberg.coordinator.memory.request": "512Mi",
    "iceberg.coordinator.memory.limit": "1Gi",
    "iceberg.coordinator.cpu.request": "200m",
    "iceberg.coordinator.cpu.limit": "1000m",
    "iceberg.coordinator.service-account": "kafka-connect-sa",
    "iceberg.coordinator.wait-for-readiness": "true",
    "iceberg.coordinator.readiness-timeout-seconds": "120"
  }
}
```

## Sample 2: Production Configuration (ADLS + REST Catalog)

```json
{
  "name": "analytics-iceberg-connector",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnectorV2",
    "tasks.max": "6",
    "topics": "user_events,product_views,purchases",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "iceberg.connect.group-id": "analytics-connector-group",
    "iceberg.control.topic": "iceberg-control-analytics",
    "iceberg.control.commit.interval-ms": "180000",
    "iceberg.control.commit.timeout-ms": "45000",
    "iceberg.control.commit.threads": "8",

    "iceberg.catalog": "rest",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg-rest-catalog:8181",
    "iceberg.catalog.warehouse": "abfss://data@mystorageaccount.dfs.core.windows.net/warehouse/",
    "iceberg.catalog.io-impl": "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
    "iceberg.catalog.adls.account-name": "mystorageaccount",
    "iceberg.catalog.adls.auth.type": "oauth",

    "iceberg.tables.route-field": "event_type",
    "iceberg.tables.route.user_events": "analytics.user_events",
    "iceberg.tables.route.product_views": "analytics.product_views",
    "iceberg.tables.route.purchases": "analytics.purchases",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.tables.upsert-mode-enabled": "true",
    "iceberg.tables.id-columns": "user_id,event_id",

    "iceberg.coordinator.replicas": "5",
    "iceberg.coordinator.memory.request": "1Gi",
    "iceberg.coordinator.memory.limit": "2Gi",
    "iceberg.coordinator.cpu.request": "500m",
    "iceberg.coordinator.cpu.limit": "2000m",
    "iceberg.coordinator.service-account": "kafka-connect-sa",
    "iceberg.coordinator.strict-mode": "true",
    "iceberg.coordinator.wait-for-readiness": "true",
    "iceberg.coordinator.readiness-timeout-seconds": "180"
  }
}
```

## Sample 3: Development Configuration (Local Hadoop)

```json
{
  "name": "dev-iceberg-connector",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnectorV2",
    "tasks.max": "1",
    "topics": "test-topic",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "iceberg.connect.group-id": "dev-connector-group",
    "iceberg.control.topic": "iceberg-control-dev",
    "iceberg.control.commit.interval-ms": "60000",

    "iceberg.catalog": "hadoop",
    "iceberg.catalog.type": "hadoop",
    "iceberg.catalog.warehouse": "/tmp/iceberg-warehouse",

    "iceberg.tables": "test_db.test_table",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",

    "iceberg.coordinator.replicas": "1",
    "iceberg.coordinator.memory.limit": "512Mi",
    "iceberg.coordinator.cpu.limit": "500m",
    "iceberg.coordinator.strict-mode": "false",
    "iceberg.coordinator.wait-for-readiness": "false"
  }
}
```

## Sample 4: Multi-Topic with Custom Partitioning

```json
{
  "name": "multi-topic-iceberg-connector",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnectorV2",
    "tasks.max": "4",
    "topics": "orders,customers,products,inventory",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "iceberg.connect.group-id": "multi-topic-group",
    "iceberg.control.topic": "iceberg-control-multi",
    "iceberg.control.commit.interval-ms": "240000",

    "iceberg.catalog": "glue",
    "iceberg.catalog.type": "glue",
    "iceberg.catalog.warehouse": "s3://data-lake/warehouse/",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.glue.region": "us-east-1",

    "iceberg.tables.route.orders": "retail.orders",
    "iceberg.tables.route.customers": "retail.customers",
    "iceberg.tables.route.products": "retail.products",
    "iceberg.tables.route.inventory": "retail.inventory",

    "iceberg.tables.partition.retail.orders": "month(order_date),bucket(customer_id,10)",
    "iceberg.tables.partition.retail.customers": "bucket(customer_id,20)",
    "iceberg.tables.partition.retail.products": "category",
    "iceberg.tables.partition.retail.inventory": "warehouse_id,day(updated_at)",

    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",

    "iceberg.coordinator.replicas": "3",
    "iceberg.coordinator.memory.request": "768Mi",
    "iceberg.coordinator.memory.limit": "1536Mi",
    "iceberg.coordinator.cpu.request": "300m",
    "iceberg.coordinator.cpu.limit": "1500m",
    "iceberg.coordinator.service-account": "kafka-connect-sa"
  }
}
```

## Deployment Commands

### Deploy Connector
```bash
# Save configuration to file
cat > connector-config.json << 'EOF'
{
  "name": "orders-iceberg-connector",
  "config": {
    // ... configuration from samples above
  }
}
EOF

# Deploy via Kafka Connect REST API
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector-config.json
```

### Verify Deployment
```bash
# Check connector status
curl http://kafka-connect:8083/connectors/orders-iceberg-connector/status | jq

# Check coordinator pods (will be created automatically)
kubectl get pods -l connector=orders-iceberg-connector

# Check coordinator deployment
kubectl get deployment orders-iceberg-connector-coordinator

# Check leader election
kubectl get lease orders-iceberg-connector-coordinator-leader -o yaml

# View coordinator logs
kubectl logs deployment/orders-iceberg-connector-coordinator -f
```

## Required Configuration Parameters

### Mandatory Parameters
- `connector.class`: Must be `IcebergSinkConnectorV2`
- `iceberg.connect.group-id`: Unique group ID for coordinator coordination
- `iceberg.control.topic`: Kafka topic for coordinator control messages
- `iceberg.catalog`: Catalog name
- `iceberg.catalog.type`: Catalog type (glue, rest, hadoop, etc.)
- `iceberg.catalog.warehouse`: Data warehouse location
- `iceberg.tables`: Table routing configuration

### Coordinator-Specific Parameters
- `iceberg.coordinator.replicas`: Number of coordinator replicas (1-5)
- `iceberg.coordinator.service-account`: Kubernetes service account with proper RBAC

### Optional but Recommended
- `iceberg.coordinator.memory.request/limit`: Resource management
- `iceberg.coordinator.cpu.request/limit`: Resource management
- `iceberg.coordinator.wait-for-readiness`: Wait for coordinator before starting
- `iceberg.tables.auto-create-enabled`: Auto-create missing tables
- `iceberg.tables.evolve-schema-enabled`: Handle schema evolution

## Environment Setup

### Set Environment Variables (Optional)
```bash
# Override namespace (otherwise auto-detected)
export KUBERNETES_NAMESPACE=kafka-connect

# Override coordinator image (otherwise uses Kafka Connect image)
export KAFKA_CONNECT_IMAGE=confluentinc/cp-kafka-connect:7.4.0

# Set in Kafka Connect deployment
kubectl set env deployment/kafka-connect \
  KUBERNETES_NAMESPACE=kafka-connect \
  KAFKA_CONNECT_IMAGE=confluentinc/cp-kafka-connect:7.4.0
```

## Troubleshooting

### Check Deployment Status
```bash
# Connector status
curl http://kafka-connect:8083/connectors/orders-iceberg-connector/status

# Coordinator resources
kubectl get all -l connector=orders-iceberg-connector

# Coordinator logs
kubectl logs deployment/orders-iceberg-connector-coordinator --tail=100

# Leader election status
kubectl describe lease orders-iceberg-connector-coordinator-leader
```

### Common Issues
1. **RBAC Permissions**: Ensure service account has required permissions
2. **Image Access**: Verify Kafka Connect image is accessible from coordinator pods
3. **Namespace**: Check if coordinator deploys in correct namespace
4. **Resources**: Ensure sufficient CPU/memory limits

This configuration will automatically deploy HA coordinator pods with leader election when the connector starts, providing high availability and exactly-once semantics for your Iceberg data pipeline.