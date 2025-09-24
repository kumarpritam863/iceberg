# HA Iceberg Kafka Connect Coordinator Deployment Guide

This guide explains how to deploy and configure the High Availability (HA) Iceberg Kafka Connect Coordinator with leader election.

## Overview

The HA coordinator eliminates the need for an external Kubernetes operator by directly deploying coordinator pods using the Kubernetes Java client. It provides:

- **High Availability**: Multiple coordinator replicas with leader election
- **Automatic Failover**: When the leader fails, another replica takes over
- **No Operator Dependency**: Self-contained deployment management
- **Health Monitoring**: Built-in health and readiness checks

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                       │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │               Coordinator Pods (HA)                 │   │
│  │                                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │   │
│  │  │ Coordinator │  │ Coordinator │  │ Coordinator │  │   │
│  │  │   Pod 1     │  │   Pod 2     │  │   Pod 3     │  │   │
│  │  │ (LEADER)    │  │ (STANDBY)   │  │ (STANDBY)   │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │   │
│  │        │                  │                  │      │   │
│  │        └──────────────────┼──────────────────┘      │   │
│  │                          │                         │   │
│  │                  Leader Election                    │   │
│  │                 (Kubernetes Lease)                  │   │
│  └─────────────────────────────────────────────────────┘   │
│                              │                             │
│                              │ Events                      │
│                              ▼                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Kafka Connect Workers                  │   │
│  │                                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │   │
│  │  │   Worker 1  │  │   Worker 2  │  │   Worker 3  │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Basic HA Connector Configuration

```json
{
  "name": "my-iceberg-connector",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnectorV2",
    "tasks.max": "3",
    "topics": "orders",
    "iceberg.connect.group-id": "orders-connector",
    "iceberg.control.topic": "iceberg-control-orders",
    "iceberg.catalog": "hadoop",
    "iceberg.catalog.type": "hadoop",
    "iceberg.catalog.warehouse": "s3://data-lake/warehouse",
    "iceberg.tables": "orders.orders_table",
    "iceberg.coordinator.replicas": "3"
  }
}
```

### 2. Deploy the Connector

```bash
curl -X POST http://kafka-connect:8083/connectors \\
  -H "Content-Type: application/json" \\
  -d @connector-config.json
```

The connector will automatically:
1. Create a Kubernetes lease for leader election
2. Deploy a ConfigMap with coordinator configuration
3. Create a Service for coordinator communication
4. Deploy coordinator pods with the specified replica count
5. Start leader election process

## Configuration Reference

### Basic Configuration

| Property | Description | Default | Range |
|----------|-------------|---------|-------|
| `iceberg.coordinator.replicas` | Number of coordinator replicas | `3` | `1-5` |
| `iceberg.coordinator.image` | Docker image for coordinator (optional) | Current Kafka Connect image | - |
| `iceberg.coordinator.service-account` | Kubernetes service account | `default` | - |

> **Note**: The coordinator pods use the same image as your Kafka Connect cluster by default, since all coordinator classes are included in the main Kafka Connect JAR. You only need to specify `iceberg.coordinator.image` if you want to use a different image.

### Resource Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `iceberg.coordinator.memory.request` | Memory request | `512Mi` |
| `iceberg.coordinator.memory.limit` | Memory limit | `1Gi` |
| `iceberg.coordinator.cpu.request` | CPU request | `200m` |
| `iceberg.coordinator.cpu.limit` | CPU limit | `1000m` |

### Deployment Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `iceberg.coordinator.strict-mode` | Fail connector start if coordinator deployment fails | `false` |
| `iceberg.coordinator.wait-for-readiness` | Wait for coordinator readiness | `true` |
| `iceberg.coordinator.readiness-timeout-seconds` | Readiness check timeout | `120` |

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KUBERNETES_NAMESPACE` | Target namespace for coordinator deployment | Auto-detected from current pod |
| `KAFKA_CONNECT_IMAGE` | Kafka Connect image to use for coordinators | Auto-detected from current pod |
| `POD_NAME` | Current pod name (set automatically by Kubernetes) | - |

> **Note**: The deployment manager automatically detects the current Kubernetes namespace by reading the service account token. If running outside of Kubernetes, it falls back to the `default` namespace.

## Advanced Configuration

### Production Setup

```json
{
  "name": "production-iceberg-connector",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnectorV2",
    "tasks.max": "6",
    "topics": "orders,products,customers",

    "iceberg.connect.group-id": "production-connector",
    "iceberg.control.topic": "iceberg-control-production",

    "iceberg.catalog": "glue",
    "iceberg.catalog.type": "glue",
    "iceberg.catalog.warehouse": "s3://production-data-lake/warehouse",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",

    "iceberg.tables": "production.orders,production.products,production.customers",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",

    "iceberg.coordinator.replicas": "5",
    "iceberg.coordinator.service-account": "iceberg-coordinator-sa",
    "iceberg.coordinator.memory.request": "1Gi",
    "iceberg.coordinator.memory.limit": "2Gi",
    "iceberg.coordinator.cpu.request": "500m",
    "iceberg.coordinator.cpu.limit": "2000m",
    "iceberg.coordinator.strict-mode": "true",
    "iceberg.coordinator.readiness-timeout-seconds": "180"
  }
}
```

### Custom Service Account

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: iceberg-coordinator-sa
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: default
  name: iceberg-coordinator-role
rules:
- apiGroups: [""]
  resources: ["configmaps", "services"]
  verbs: ["get", "list", "create", "update", "patch", "delete"]
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "list", "create", "update", "patch", "delete"]
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: iceberg-coordinator-binding
  namespace: default
subjects:
- kind: ServiceAccount
  name: iceberg-coordinator-sa
  namespace: default
roleRef:
  kind: Role
  name: iceberg-coordinator-role
  apiGroup: rbac.authorization.k8s.io
```

## Monitoring and Observability

### Health Check Endpoints

Each coordinator pod exposes the following endpoints:

- `GET /health` - Liveness probe (HTTP 200 = healthy)
- `GET /ready` - Readiness probe (HTTP 200 = ready for traffic)
- `GET /status` - Detailed status information

### Monitoring Commands

```bash
# Check coordinator pods (new naming pattern)
kubectl get pods -l app=iceberg-coordinator

# Check leader election lease
kubectl get lease my-connector-coordinator-leader

# View coordinator logs
kubectl logs deployment/my-connector-coordinator -f

# Check coordinator status
kubectl exec deployment/my-connector-coordinator -- \\
  curl -s http://localhost:8080/status | jq
```

### Example Status Response

```json
{
  "health": "UP",
  "readiness": "UP",
  "port": 8080,
  "running": true,
  "leader": "iceberg-coordinator-my-connector-abc123",
  "isLeader": true,
  "jobId": "my-connector"
}
```

## Troubleshooting

### Common Issues

1. **Coordinator pods not starting**
   - Check RBAC permissions for service account
   - Verify Kubernetes namespace exists
   - Check resource limits and quotas

2. **Leader election not working**
   - Verify lease resource exists: `kubectl get lease`
   - Check coordinator logs for election errors
   - Ensure network connectivity between pods

3. **Connector deployment fails**
   - Check `iceberg.coordinator.strict-mode` setting
   - Verify all required configuration is present
   - Check Kubernetes API connectivity

### Debug Commands

```bash
# Check coordinator deployment
kubectl describe deployment my-connector-coordinator

# Check coordinator service
kubectl describe service my-connector-coordinator-svc

# Check coordinator configmap
kubectl describe configmap my-connector-coordinator-config

# Check leader election lease
kubectl describe lease my-connector-coordinator-leader

# Test health endpoints
kubectl port-forward deployment/my-connector-coordinator 8080:8080
curl http://localhost:8080/health
curl http://localhost:8080/ready
curl http://localhost:8080/status
```

## Migration from Operator-based Deployment

If you're migrating from an operator-based deployment:

1. **Stop the old operator and coordinator pods**
2. **Update connector configuration** to use `IcebergSinkConnectorV2`
3. **Deploy the new connector** - it will automatically create HA coordinators
4. **Verify leader election** is working correctly
5. **Clean up old operator resources**

The new HA coordinator maintains the same Iceberg commit semantics and exactly-once guarantees as the operator-based approach.

## Performance Considerations

- **Replica Count**: 3-5 replicas provide good balance of availability and resource usage
- **Resource Limits**: Coordinator pods typically need 512Mi-2Gi memory and 200m-1000m CPU
- **Leader Election**: Uses minimal network traffic (lease renewals every 10 seconds)
- **Health Checks**: Lightweight HTTP endpoints with minimal overhead

## Security Considerations

- Use dedicated service account with minimal RBAC permissions
- Ensure coordinator image comes from trusted registry
- Configure network policies if required
- Monitor coordinator logs for security events