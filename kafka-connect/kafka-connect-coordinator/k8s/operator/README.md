# Iceberg Kafka Connect Operator

A Kubernetes operator that manages Iceberg Kafka Connect deployments with S3-sourced container images and comprehensive configuration management.

## Features

- **S3 Image Management**: Downloads container images from S3 and pushes to target registry
- **Per-Job Coordination**: Implements Flink-style JobManager/TaskManager architecture
- **Exactly-Once Semantics**: Transactional Kafka producers and consumers
- **Full Configuration**: Comprehensive Kafka Connect and Iceberg configuration
- **Cloud Native**: Kubernetes-native deployment and lifecycle management

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   S3 Bucket     │    │    Operator      │    │   Kubernetes    │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │Worker Image │ │───▶│ │Image Manager │ │───▶│ │   Workers   │ │
│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │Coord. Image │ │───▶│ │Config Manager│ │───▶│ │Coordinators │ │
│ └─────────────┘ │    │ └──────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Quick Start

### 1. Deploy the Operator

```bash
# Clone the repository
git clone <repository-url>
cd kafka-connect/kafka-connect-coordinator/k8s/operator

# Deploy the operator
./deploy-operator.sh
```

### 2. Prepare Your Images

Upload your Kafka Connect worker and coordinator images to S3:

```bash
# Save Docker images to tar files
docker save my-kafka-connect-worker:latest > worker-image.tar
docker save my-iceberg-coordinator:latest > coordinator-image.tar

# Upload to S3
aws s3 cp worker-image.tar s3://my-bucket/images/
aws s3 cp coordinator-image.tar s3://my-bucket/images/
```

### 3. Create an IcebergConnect Resource

```yaml
apiVersion: kafka.iceberg.apache.org/v1
kind: IcebergConnect
metadata:
  name: my-iceberg-job
  namespace: default
spec:
  s3Config:
    region: "us-west-2"
    bucket: "my-bucket"
    accessKeyId: "AKIA..."
    secretAccessKey: "secret..."

  images:
    worker:
      s3Path: "images/worker-image.tar"
      tag: "v1.0.0"
    coordinator:
      s3Path: "images/coordinator-image.tar"
      tag: "v1.0.0"

  worker:
    kafka:
      bootstrapServers: "kafka:9092"

  coordinator:
    job:
      controlTopic: "iceberg-control"
      groupId: "my-job"
      catalogProperties:
        type: "hive"
        uri: "thrift://hive:9083"
```

Apply the configuration:
```bash
kubectl apply -f my-iceberg-connect.yaml
```

## Configuration Reference

### S3 Configuration

| Field | Description | Required |
|-------|-------------|----------|
| `region` | AWS S3 region | Yes |
| `bucket` | S3 bucket name | Yes |
| `accessKeyId` | AWS Access Key ID | Yes |
| `secretAccessKey` | AWS Secret Access Key | Yes |
| `endpoint` | Custom S3 endpoint (MinIO, etc.) | No |

### Image Configuration

| Field | Description | Default |
|-------|-------------|---------|
| `s3Path` | Path to image tar in S3 | Required |
| `tag` | Target image tag | `latest` |
| `registry` | Target registry | `localhost:5000` |

### Worker Configuration

#### Basic Settings
| Field | Description | Default |
|-------|-------------|---------|
| `replicas` | Number of worker replicas | `3` |
| `clusterName` | Unique cluster identifier | Auto-generated |
| `namespace` | Kubernetes namespace | `default` |

#### Kafka Settings
| Field | Description | Required |
|-------|-------------|----------|
| `bootstrapServers` | Kafka bootstrap servers | Yes |
| `securityProtocol` | Security protocol | `PLAINTEXT` |
| `saslMechanism` | SASL mechanism | No |
| `saslJaasConfig` | SASL JAAS configuration | No |

#### Storage Topics
| Field | Description | Default |
|-------|-------------|---------|
| `offsetTopic` | Offset storage topic | `connect-offsets` |
| `offsetReplicationFactor` | Offset topic replication | `1` |
| `offsetPartitions` | Offset topic partitions | `25` |
| `configTopic` | Config storage topic | `connect-configs` |
| `configReplicationFactor` | Config topic replication | `1` |
| `statusTopic` | Status storage topic | `connect-status` |
| `statusReplicationFactor` | Status topic replication | `1` |
| `statusPartitions` | Status topic partitions | `5` |

#### Resources
| Field | Description | Default |
|-------|-------------|---------|
| `requests.memory` | Memory request | `1Gi` |
| `requests.cpu` | CPU request | `500m` |
| `limits.memory` | Memory limit | `2Gi` |
| `limits.cpu` | CPU limit | `1000m` |

#### Performance Tuning
| Field | Description | Default |
|-------|-------------|---------|
| `offsetFlushIntervalMs` | Offset flush interval | `10000` |
| `offsetFlushTimeoutMs` | Offset flush timeout | `5000` |
| `taskShutdownTimeoutMs` | Task shutdown timeout | `10000` |

### Coordinator Configuration

#### Job Settings
| Field | Description | Required |
|-------|-------------|----------|
| `controlTopic` | Control topic name | Yes |
| `groupId` | Connect group ID | Yes |
| `catalogProperties` | Iceberg catalog properties | Yes |

#### Kafka Settings
| Field | Description | Default |
|-------|-------------|---------|
| `bootstrapServers` | Kafka bootstrap servers | Inherited from worker |
| `enableTransactions` | Enable transactions | `true` |
| `transactionTimeoutMs` | Transaction timeout | `300000` |

## Examples

### Production Deployment

See [examples/iceberg-connect-production.yaml](examples/iceberg-connect-production.yaml) for a comprehensive production configuration with:
- High availability (3 worker replicas)
- Security (SASL/SSL)
- High-performance storage topics
- Resource limits appropriate for production

### Development Deployment

See [examples/iceberg-connect-development.yaml](examples/iceberg-connect-development.yaml) for a minimal development setup with:
- Single replica deployment
- Minimal resources
- Local/insecure configuration

## Monitoring and Management

### Check Deployment Status

```bash
# Check IcebergConnect resource status
kubectl get icebergconnects -A

# Describe a specific resource
kubectl describe icebergconnect my-iceberg-job

# Check operator logs
kubectl logs -n iceberg-system -l app=iceberg-connect-operator -f
```

### Monitor Workers and Coordinators

```bash
# Check worker pods
kubectl get pods -l app=kafka-connect-workers

# Check coordinator pods
kubectl get pods -l app=iceberg-job-manager

# Access worker REST API
kubectl port-forward service/kafka-connect-service-my-workers 8083:8083
curl http://localhost:8083/connectors
```

### Scaling

```bash
# Scale workers
kubectl patch icebergconnect my-iceberg-job --type='merge' -p='{"spec":{"worker":{"replicas":5}}}'

# Scale coordinators (typically keep at 1 for per-job coordination)
kubectl patch icebergconnect my-iceberg-job --type='merge' -p='{"spec":{"coordinator":{"replicas":1}}}'
```

## Troubleshooting

### Common Issues

#### 1. Image Download Failures
- Verify S3 credentials and permissions
- Check S3 bucket and path existence
- Ensure network connectivity to S3

#### 2. Worker Startup Issues
- Check Kafka connectivity
- Verify storage topic creation
- Review ConfigMap configuration

#### 3. Coordinator Issues
- Verify control topic exists
- Check catalog connectivity
- Review transaction configuration

### Debug Commands

```bash
# Check operator status
kubectl get pods -n iceberg-system -l app=iceberg-connect-operator

# View operator logs
kubectl logs -n iceberg-system -l app=iceberg-connect-operator

# Check resource status
kubectl describe icebergconnect <name>

# Check generated ConfigMaps
kubectl get configmaps -l managed-by=iceberg-connect-operator

# Check deployments and services
kubectl get all -l managed-by=iceberg-connect-operator
```

## Security Considerations

### S3 Credentials
- Use IAM roles when possible instead of access keys
- Store credentials in Kubernetes secrets
- Implement least-privilege access policies

### Kafka Security
- Use SASL/SSL for production deployments
- Implement proper topic ACLs
- Enable client authentication

### Container Security
- Scan images for vulnerabilities
- Use minimal base images
- Implement proper RBAC

## Cleanup

To remove an IcebergConnect deployment:

```bash
kubectl delete icebergconnect my-iceberg-job
```

To remove the operator:

```bash
kubectl delete -f manifests/operator-deployment.yaml
kubectl delete -f crds/
kubectl delete namespace iceberg-system
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Licensed under the Apache License 2.0. See LICENSE file for details.