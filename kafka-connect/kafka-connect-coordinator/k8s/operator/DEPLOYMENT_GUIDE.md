# Step-by-Step Deployment Guide

## Prerequisites

Before starting, ensure you have:
- ✅ Kubernetes cluster (1.19+)
- ✅ kubectl configured and connected to your cluster
- ✅ Docker installed on your machine
- ✅ AWS CLI configured (for S3 access)
- ✅ Your custom Kafka Connect worker and coordinator images ready

## Step 1: Prepare Your Custom Images

### 1.1 Build Your Worker Image

Create a Dockerfile for your worker:
```dockerfile
# worker.Dockerfile
FROM openjdk:11-jre-slim

# Install required packages
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create directories
RUN mkdir -p /app/libs /app/kafka-libs /app/plugins /app/config

# Copy Kafka Connect libraries (you need to provide these)
COPY kafka-connect-libs/* /app/libs/
COPY kafka-libs/* /app/kafka-libs/
COPY iceberg-kafka-connect-*.jar /app/plugins/

# Set working directory
WORKDIR /app

# Default command (will be overridden by operator)
CMD ["java", "-cp", "/app/libs/*:/app/kafka-libs/*", "org.apache.kafka.connect.cli.ConnectDistributed"]
```

Build and save:
```bash
# Build worker image
docker build -f worker.Dockerfile -t my-kafka-connect-worker:latest .

# Save to tar file
docker save my-kafka-connect-worker:latest > worker-image.tar
```

### 1.2 Build Your Coordinator Image

Create a Dockerfile for your coordinator:
```dockerfile
# coordinator.Dockerfile
FROM openjdk:11-jre-slim

# Install required packages
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create directories
RUN mkdir -p /app/libs

# Copy coordinator JAR and dependencies
COPY iceberg-kafka-connect-coordinator.jar /app/
COPY coordinator-libs/* /app/libs/

# Set working directory
WORKDIR /app

# Default command (will be overridden by operator)
CMD ["java", "-cp", "/app/libs/*:/app/iceberg-kafka-connect-coordinator.jar", "org.apache.iceberg.connect.coordinator.IcebergJobManagerApplication"]
```

Build and save:
```bash
# Build coordinator image
docker build -f coordinator.Dockerfile -t my-iceberg-coordinator:latest .

# Save to tar file
docker save my-iceberg-coordinator:latest > coordinator-image.tar
```

### 1.3 Upload Images to S3

```bash
# Upload to your S3 bucket
aws s3 cp worker-image.tar s3://your-bucket/images/worker-v1.0.0.tar
aws s3 cp coordinator-image.tar s3://your-bucket/images/coordinator-v1.0.0.tar

# Verify uploads
aws s3 ls s3://your-bucket/images/
```

## Step 2: Deploy the Operator

### 2.1 Clone the Repository

```bash
git clone <your-iceberg-repo>
cd iceberg/kafka-connect/kafka-connect-coordinator/k8s/operator
```

### 2.2 Build and Deploy the Operator

```bash
# Set environment variables
export OPERATOR_NAMESPACE=iceberg-system
export REGISTRY=your-registry.com  # Optional: use your registry

# Deploy the operator
./deploy-operator.sh
```

This will:
- Create the `iceberg-system` namespace
- Build the operator image
- Apply CRDs
- Deploy the operator with proper RBAC

### 2.3 Verify Operator Deployment

```bash
# Check operator status
kubectl get pods -n iceberg-system

# Check operator logs
kubectl logs -n iceberg-system -l app=iceberg-connect-operator -f

# Verify CRDs are installed
kubectl get crd icebergconnects.kafka.iceberg.apache.org
```

Expected output:
```
NAME                                               CREATED AT
icebergconnects.kafka.iceberg.apache.org          2024-01-01T10:00:00Z
```

## Step 3: Prepare Your Configuration

### 3.1 Create AWS Credentials Secret (Optional)

If using AWS S3:
```bash
kubectl create secret generic s3-credentials \
  --from-literal=access-key-id=AKIAIOSFODNN7EXAMPLE \
  --from-literal=secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  -n default
```

### 3.2 Create Your IcebergConnect Configuration

Create `my-iceberg-connect.yaml`:
```yaml
apiVersion: kafka.iceberg.apache.org/v1
kind: IcebergConnect
metadata:
  name: my-iceberg-job
  namespace: default
spec:
  # S3 Configuration for downloading images
  s3Config:
    region: "us-west-2"
    bucket: "your-bucket"
    accessKeyId: "AKIAIOSFODNN7EXAMPLE"
    secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

  # Image locations in S3
  images:
    worker:
      s3Path: "images/worker-v1.0.0.tar"
      tag: "v1.0.0"
      registry: "your-registry.com"  # Optional
    coordinator:
      s3Path: "images/coordinator-v1.0.0.tar"
      tag: "v1.0.0"
      registry: "your-registry.com"  # Optional

  # Worker Configuration
  worker:
    replicas: 3
    clusterName: "my-workers"
    namespace: "default"

    # Kafka Configuration
    kafka:
      bootstrapServers: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
      # Optional: Add security if needed
      # securityProtocol: "SASL_SSL"
      # saslMechanism: "SCRAM-SHA-256"
      # saslJaasConfig: "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"pass\";"

    # Storage Topics Configuration
    storage:
      offsetTopic: "connect-offsets"
      offsetReplicationFactor: 3
      offsetPartitions: 25
      configTopic: "connect-configs"
      configReplicationFactor: 3
      statusTopic: "connect-status"
      statusReplicationFactor: 3
      statusPartitions: 5

    # Resource Configuration
    resources:
      requests:
        memory: "1Gi"
        cpu: "500m"
      limits:
        memory: "2Gi"
        cpu: "1000m"

  # Coordinator Configuration
  coordinator:
    replicas: 1
    namespace: "default"

    # Job-specific Configuration
    job:
      controlTopic: "iceberg-control-my-job"
      groupId: "my-iceberg-job-group"
      catalogProperties:
        type: "hive"
        uri: "thrift://hive-metastore:9083"
        warehouse: "s3a://data-lake/warehouse/"
        io-impl: "org.apache.iceberg.aws.s3.S3FileIO"

    # Kafka Configuration (inherits from worker)
    kafka:
      enableTransactions: true
      transactionTimeoutMs: 300000

    # Resource Configuration
    resources:
      requests:
        memory: "512Mi"
        cpu: "250m"
      limits:
        memory: "1Gi"
        cpu: "500m"
```

## Step 4: Deploy Your IcebergConnect Resource

### 4.1 Apply the Configuration

```bash
kubectl apply -f my-iceberg-connect.yaml
```

### 4.2 Monitor the Deployment

```bash
# Check IcebergConnect status
kubectl get icebergconnects

# Describe the resource for detailed status
kubectl describe icebergconnect my-iceberg-job

# Watch the operator logs
kubectl logs -n iceberg-system -l app=iceberg-connect-operator -f
```

You should see the operator progress through these phases:
1. `Downloading` - Downloading images from S3
2. `Building` - Building and pushing images to registry
3. `Deploying` - Creating workers and coordinators
4. `Ready` - All components deployed successfully

### 4.3 Verify Deployments

```bash
# Check worker pods
kubectl get pods -l app=kafka-connect-workers

# Check coordinator pods
kubectl get pods -l app=iceberg-job-manager

# Check services
kubectl get services -l managed-by=iceberg-connect-operator

# Check ConfigMaps
kubectl get configmaps -l managed-by=iceberg-connect-operator
```

## Step 5: Test the Deployment

### 5.1 Access the Connect REST API

```bash
# Port forward to worker service
kubectl port-forward service/kafka-connect-service-my-workers 8083:8083
```

In another terminal:
```bash
# Check Connect cluster status
curl http://localhost:8083/

# List connector plugins
curl http://localhost:8083/connector-plugins

# Check worker info
curl http://localhost:8083/connectors
```

### 5.2 Create a Test Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "iceberg-sink-test",
    "config": {
      "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
      "tasks.max": "1",
      "topics": "test-topic",
      "iceberg.control.topic": "iceberg-control-my-job",
      "iceberg.control.group-id": "my-iceberg-job-group",
      "iceberg.catalog.type": "hive",
      "iceberg.catalog.uri": "thrift://hive-metastore:9083"
    }
  }'
```

### 5.3 Verify Connector Status

```bash
# List connectors
curl http://localhost:8083/connectors

# Check connector status
curl http://localhost:8083/connectors/iceberg-sink-test/status

# Check tasks
curl http://localhost:8083/connectors/iceberg-sink-test/tasks
```

## Step 6: Monitoring and Management

### 6.1 View Logs

```bash
# Worker logs
kubectl logs -l app=kafka-connect-workers -f

# Coordinator logs
kubectl logs -l app=iceberg-job-manager -f

# Operator logs
kubectl logs -n iceberg-system -l app=iceberg-connect-operator -f
```

### 6.2 Scale the Deployment

```bash
# Scale workers
kubectl patch icebergconnect my-iceberg-job --type='merge' -p='{"spec":{"worker":{"replicas":5}}}'

# Verify scaling
kubectl get pods -l app=kafka-connect-workers
```

### 6.3 Update Configuration

```bash
# Edit the resource
kubectl edit icebergconnect my-iceberg-job

# Or apply updated YAML
kubectl apply -f my-iceberg-connect-updated.yaml
```

## Step 7: Troubleshooting

### 7.1 Common Issues

**Image Download Failures:**
```bash
# Check S3 access
aws s3 ls s3://your-bucket/images/

# Check operator logs for S3 errors
kubectl logs -n iceberg-system -l app=iceberg-connect-operator | grep -i s3
```

**Worker Startup Issues:**
```bash
# Check worker pod logs
kubectl logs <worker-pod-name>

# Check ConfigMap content
kubectl get configmap kafka-connect-worker-config-my-workers -o yaml

# Describe pod for events
kubectl describe pod <worker-pod-name>
```

**Coordinator Issues:**
```bash
# Check coordinator logs
kubectl logs <coordinator-pod-name>

# Check environment variables
kubectl describe pod <coordinator-pod-name>
```

### 7.2 Debug Commands

```bash
# Check all resources created by operator
kubectl get all -l managed-by=iceberg-connect-operator

# Check IcebergConnect resource status
kubectl get icebergconnects -o wide

# Get detailed events
kubectl get events --sort-by='.lastTimestamp'
```

## Step 8: Cleanup (Optional)

### 8.1 Remove IcebergConnect Resource

```bash
kubectl delete icebergconnect my-iceberg-job
```

### 8.2 Remove Operator

```bash
kubectl delete -f manifests/operator-deployment.yaml
kubectl delete -f crds/
kubectl delete namespace iceberg-system
```

## Summary

You've successfully deployed:
1. ✅ Custom Kafka Connect operator
2. ✅ Workers with your custom images from S3
3. ✅ Per-job coordinators with transactional semantics
4. ✅ Complete exactly-once processing pipeline

Your Kafka Connect cluster is now running with **zero Confluent dependencies** using pure Apache Kafka components! 🚀