#!/bin/bash
set -e

# Deploy Iceberg Connect Operator with Automatic Coordinator Deployment
# This script sets up the complete system for automatic coordinator deployment

NAMESPACE=${NAMESPACE:-iceberg-system}
OPERATOR_IMAGE=${OPERATOR_IMAGE:-iceberg-connect-operator:latest}

echo "🚀 Deploying Iceberg Connect Operator with Auto-Coordinator Support"
echo "📍 Namespace: $NAMESPACE"

# Create namespace if it doesn't exist
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# Deploy the enhanced operator with admission controller support
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: iceberg-connect-operator
  namespace: $NAMESPACE
  labels:
    app: iceberg-connect-operator

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: iceberg-connect-operator
  labels:
    app: iceberg-connect-operator
rules:
# Custom Resource permissions
- apiGroups: ["kafka.iceberg.apache.org"]
  resources: ["icebergconnects"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["kafka.iceberg.apache.org"]
  resources: ["icebergconnects/status"]
  verbs: ["get", "update", "patch"]

# Core Kubernetes resource permissions
- apiGroups: [""]
  resources: ["configmaps", "services", "pods"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]

# Events permissions for logging
- apiGroups: [""]
  resources: ["events"]
  verbs: ["create", "patch"]

# Permissions to read secrets (for S3 credentials)
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list", "watch"]

# Admission webhook permissions
- apiGroups: ["admissionregistration.k8s.io"]
  resources: ["mutatingadmissionconfigurations"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: iceberg-connect-operator
  labels:
    app: iceberg-connect-operator
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: iceberg-connect-operator
subjects:
- kind: ServiceAccount
  name: iceberg-connect-operator
  namespace: $NAMESPACE

---
# Enhanced operator deployment with admission controller
apiVersion: apps/v1
kind: Deployment
metadata:
  name: iceberg-connect-operator
  namespace: $NAMESPACE
  labels:
    app: iceberg-connect-operator
    component: operator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: iceberg-connect-operator
      component: operator
  template:
    metadata:
      labels:
        app: iceberg-connect-operator
        component: operator
    spec:
      serviceAccountName: iceberg-connect-operator
      containers:
      - name: operator
        image: $OPERATOR_IMAGE
        ports:
        - containerPort: 8443
          name: webhook
        - containerPort: 8080
          name: metrics
        env:
        - name: OPERATOR_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: PYTHONUNBUFFERED
          value: "1"
        - name: COORDINATOR_S3_REGION
          value: "${COORDINATOR_S3_REGION:-us-west-2}"
        - name: COORDINATOR_S3_BUCKET
          value: "${COORDINATOR_S3_BUCKET}"
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: iceberg-s3-credentials
              key: access-key-id
              optional: true
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: iceberg-s3-credentials
              key: secret-access-key
              optional: true
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8443
            scheme: HTTPS
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8443
            scheme: HTTPS
          initialDelaySeconds: 10
          periodSeconds: 10
        volumeMounts:
        - name: docker-socket
          mountPath: /var/run/docker.sock
          readOnly: false
        - name: webhook-certs
          mountPath: /etc/certs
          readOnly: true
      volumes:
      - name: docker-socket
        hostPath:
          path: /var/run/docker.sock
          type: Socket
      - name: webhook-certs
        secret:
          secretName: iceberg-webhook-certs

---
apiVersion: v1
kind: Service
metadata:
  name: iceberg-connect-operator
  namespace: $NAMESPACE
  labels:
    app: iceberg-connect-operator
spec:
  selector:
    app: iceberg-connect-operator
    component: operator
  ports:
  - port: 443
    targetPort: 8443
    name: webhook
    protocol: TCP
  - port: 8080
    targetPort: 8080
    name: metrics
    protocol: TCP
  type: ClusterIP

---
# Admission webhook configuration
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionWebhook
metadata:
  name: iceberg-connector-admission-webhook
spec:
  clientConfig:
    service:
      name: iceberg-connect-operator
      namespace: $NAMESPACE
      path: /mutate
    caBundle: LS0tLS1CRUdJTi... # Base64 encoded CA bundle (to be generated)
  rules:
  - operations: ["CREATE", "UPDATE"]
    apiGroups: [""]
    apiVersions: ["v1"]
    resources: ["configmaps"]
  admissionReviewVersions: ["v1", "v1beta1"]
  sideEffects: None
  failurePolicy: Ignore

EOF

echo "✅ Operator deployed successfully"

# Deploy sample Connect cluster with auto-coordinator support
echo "🔧 Deploying Kafka Connect cluster with auto-coordinator support..."

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-connect-auto-config
  namespace: default
data:
  # Kafka Connect Worker Configuration with auto-coordinator support
  bootstrap.servers: "kafka:9092"
  group.id: "auto-connect-cluster"
  key.converter: "org.apache.kafka.connect.json.JsonConverter"
  value.converter: "org.apache.kafka.connect.json.JsonConverter"
  key.converter.schemas.enable: "false"
  value.converter.schemas.enable: "false"
  offset.storage.topic: "connect-offsets"
  offset.storage.replication.factor: "1"
  offset.storage.partitions: "25"
  config.storage.topic: "connect-configs"
  config.storage.replication.factor: "1"
  status.storage.topic: "connect-status"
  status.storage.replication.factor: "1"
  status.storage.partitions: "5"
  plugin.path: "/usr/share/java,/usr/share/confluent-hub-components"
  rest.port: "8083"
  rest.advertised.host.name: ""
  rest.advertised.port: "8083"

  # Auto-coordinator configuration
  iceberg.auto.coordinator.enabled: "true"
  iceberg.auto.coordinator.s3.region: "${COORDINATOR_S3_REGION:-us-west-2}"
  iceberg.auto.coordinator.s3.bucket: "${COORDINATOR_S3_BUCKET}"
  iceberg.auto.coordinator.image.registry: "${COORDINATOR_IMAGE_REGISTRY:-localhost:5000}"

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-connect-auto-workers
  namespace: default
  labels:
    app: kafka-connect-workers
    component: auto-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: kafka-connect-workers
      component: auto-worker
  template:
    metadata:
      labels:
        app: kafka-connect-workers
        component: auto-worker
    spec:
      containers:
      - name: connect-worker
        image: confluentinc/cp-kafka-connect:latest
        ports:
        - containerPort: 8083
          name: rest-api
        env:
        # Load configuration from ConfigMap
        - name: CONNECT_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: bootstrap.servers
        - name: CONNECT_GROUP_ID
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: group.id
        - name: CONNECT_KEY_CONVERTER
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: key.converter
        - name: CONNECT_VALUE_CONVERTER
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: value.converter
        - name: CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: key.converter.schemas.enable
        - name: CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: value.converter.schemas.enable
        - name: CONNECT_OFFSET_STORAGE_TOPIC
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: offset.storage.topic
        - name: CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: offset.storage.replication.factor
        - name: CONNECT_OFFSET_STORAGE_PARTITIONS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: offset.storage.partitions
        - name: CONNECT_CONFIG_STORAGE_TOPIC
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: config.storage.topic
        - name: CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: config.storage.replication.factor
        - name: CONNECT_STATUS_STORAGE_TOPIC
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: status.storage.topic
        - name: CONNECT_STATUS_STORAGE_REPLICATION_FACTOR
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: status.storage.replication.factor
        - name: CONNECT_STATUS_STORAGE_PARTITIONS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: status.storage.partitions
        - name: CONNECT_PLUGIN_PATH
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: plugin.path
        - name: CONNECT_REST_PORT
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: rest.port
        - name: CONNECT_REST_ADVERTISED_HOST_NAME
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: CONNECT_REST_ADVERTISED_PORT
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-auto-config
              key: rest.advertised.port

        volumeMounts:
        - name: iceberg-connector
          mountPath: /usr/share/confluent-hub-components/iceberg-kafka-connect
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /
            port: 8083
          initialDelaySeconds: 60
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /
            port: 8083
          initialDelaySeconds: 30
          periodSeconds: 10
      volumes:
      - name: iceberg-connector
        emptyDir: {}

---
apiVersion: v1
kind: Service
metadata:
  name: kafka-connect-auto-service
  namespace: default
  labels:
    app: kafka-connect-workers
spec:
  selector:
    app: kafka-connect-workers
    component: auto-worker
  ports:
  - port: 8083
    targetPort: 8083
    name: rest-api
    protocol: TCP
  type: ClusterIP

EOF

echo "✅ Kafka Connect cluster deployed with auto-coordinator support"

# Create sample connector deployment script
cat > create-iceberg-connector.sh << 'EOF'
#!/bin/bash
# Create Iceberg connector with automatic coordinator deployment

CONNECTOR_NAME=${1:-iceberg-sink-example}
KAFKA_CONNECT_URL=${KAFKA_CONNECT_URL:-http://kafka-connect-auto-service:8083}

echo "🔧 Creating Iceberg connector: $CONNECTOR_NAME"
echo "📍 Connect URL: $KAFKA_CONNECT_URL"

# Create connector configuration
curl -X POST $KAFKA_CONNECT_URL/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "'$CONNECTOR_NAME'",
    "config": {
      "connector.class": "org.apache.iceberg.connect.IcebergSinkConnectorV2",
      "tasks.max": "3",
      "topics": "my-topic",

      "iceberg.control.topic": "iceberg-control-'$CONNECTOR_NAME'",
      "iceberg.connect.group-id": "'$CONNECTOR_NAME'-group",
      "iceberg.catalog": "iceberg_catalog",
      "iceberg.catalog.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
      "iceberg.catalog.uri": "jdbc:postgresql://postgres:5432/iceberg",
      "iceberg.catalog.jdbc.user": "iceberg",
      "iceberg.catalog.jdbc.password": "password",
      "iceberg.catalog.warehouse": "s3://iceberg-warehouse/",
      "iceberg.tables": "db.table",

      "iceberg.coordinator.image.s3-path": "images/coordinator-latest.tar",
      "iceberg.coordinator.image.tag": "latest",
      "iceberg.coordinator.image.registry": "localhost:5000"
    }
  }'

echo ""
echo "✅ Connector created! Coordinator will be automatically deployed."
echo "🔍 Check coordinator status:"
echo "   kubectl get icebergconnects -l connector=$CONNECTOR_NAME"
echo "🔍 Check connector status:"
echo "   curl $KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status"
EOF

chmod +x create-iceberg-connector.sh

echo ""
echo "🎉 Deployment Complete!"
echo ""
echo "📋 Next Steps:"
echo "1. Ensure S3 credentials are configured:"
echo "   kubectl create secret generic iceberg-s3-credentials \\"
echo "     --from-literal=access-key-id=YOUR_ACCESS_KEY \\"
echo "     --from-literal=secret-access-key=YOUR_SECRET_KEY \\"
echo "     -n $NAMESPACE"
echo ""
echo "2. Create an Iceberg connector (coordinator will be auto-deployed):"
echo "   ./create-iceberg-connector.sh my-connector-name"
echo ""
echo "3. Monitor coordinator deployment:"
echo "   kubectl get icebergconnects -A"
echo "   kubectl get pods -l app=iceberg-job-manager"
echo ""
echo "🔧 The system now supports:"
echo "  ✅ Automatic coordinator deployment when Iceberg connectors are created"
echo "  ✅ Kubernetes operator manages coordinator lifecycle"
echo "  ✅ S3-based image distribution"
echo "  ✅ Per-job coordination with exactly-once semantics"