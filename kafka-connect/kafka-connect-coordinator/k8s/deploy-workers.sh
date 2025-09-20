#!/bin/bash
set -e

# Configuration
NAMESPACE=${NAMESPACE:-default}
WORKER_CLUSTER_NAME=${WORKER_CLUSTER_NAME:-kafka-connect-cluster}
REPLICAS=${REPLICAS:-3}
IMAGE_TAG=${IMAGE_TAG:-latest}

# Required environment variables
REQUIRED_VARS=(
    "KAFKA_BOOTSTRAP_SERVERS"
)

# Validate required environment variables
for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var}" ]]; then
        echo "Error: Required environment variable $var is not set"
        exit 1
    fi
done

echo "Deploying Kafka Connect Workers"
echo "Namespace: $NAMESPACE"
echo "Cluster Name: $WORKER_CLUSTER_NAME"
echo "Replicas: $REPLICAS"
echo "Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"

# Create namespace if it doesn't exist
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Generate unique ConfigMap for this worker cluster
cat > /tmp/worker-config-$WORKER_CLUSTER_NAME.yaml << EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
  namespace: $NAMESPACE
data:
  # Basic Kafka Connect Configuration
  bootstrap.servers: "$KAFKA_BOOTSTRAP_SERVERS"
  group.id: "${CONNECT_GROUP_ID:-connect-cluster}"
  key.converter: "${KEY_CONVERTER:-org.apache.kafka.connect.json.JsonConverter}"
  value.converter: "${VALUE_CONVERTER:-org.apache.kafka.connect.json.JsonConverter}"
  key.converter.schemas.enable: "${KEY_CONVERTER_SCHEMAS_ENABLE:-false}"
  value.converter.schemas.enable: "${VALUE_CONVERTER_SCHEMAS_ENABLE:-false}"

  # Storage Topics Configuration
  offset.storage.topic: "${OFFSET_STORAGE_TOPIC:-connect-offsets}"
  offset.storage.replication.factor: "${OFFSET_STORAGE_REPLICATION_FACTOR:-1}"
  offset.storage.partitions: "${OFFSET_STORAGE_PARTITIONS:-25}"
  config.storage.topic: "${CONFIG_STORAGE_TOPIC:-connect-configs}"
  config.storage.replication.factor: "${CONFIG_STORAGE_REPLICATION_FACTOR:-1}"
  status.storage.topic: "${STATUS_STORAGE_TOPIC:-connect-status}"
  status.storage.replication.factor: "${STATUS_STORAGE_REPLICATION_FACTOR:-1}"
  status.storage.partitions: "${STATUS_STORAGE_PARTITIONS:-5}"

  # Plugin Configuration
  plugin.path: "${PLUGIN_PATH:-/usr/share/java,/usr/share/confluent-hub-components}"

  # REST API Configuration
  rest.port: "${REST_PORT:-8083}"
  rest.advertised.host.name: "${REST_ADVERTISED_HOST_NAME:-}"
  rest.advertised.port: "${REST_ADVERTISED_PORT:-8083}"

  # Security Configuration
  security.protocol: "${SECURITY_PROTOCOL:-PLAINTEXT}"
$([ -n "$SASL_MECHANISM" ] && echo "  sasl.mechanism: \"$SASL_MECHANISM\"")
$([ -n "$SASL_JAAS_CONFIG" ] && echo "  sasl.jaas.config: \"$SASL_JAAS_CONFIG\"")

  # Performance Tuning
  offset.flush.interval.ms: "${OFFSET_FLUSH_INTERVAL_MS:-10000}"
  offset.flush.timeout.ms: "${OFFSET_FLUSH_TIMEOUT_MS:-5000}"
  task.shutdown.graceful.timeout.ms: "${TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS:-10000}"

  # Producer Configuration (Exactly-Once Semantics)
  producer.bootstrap.servers: "$KAFKA_BOOTSTRAP_SERVERS"
  producer.key.serializer: "${PRODUCER_KEY_SERIALIZER:-org.apache.kafka.common.serialization.ByteArraySerializer}"
  producer.value.serializer: "${PRODUCER_VALUE_SERIALIZER:-org.apache.kafka.common.serialization.ByteArraySerializer}"
  producer.acks: "${PRODUCER_ACKS:-all}"
  producer.retries: "${PRODUCER_RETRIES:-2147483647}"
  producer.enable.idempotence: "${PRODUCER_ENABLE_IDEMPOTENCE:-true}"
  producer.max.in.flight.requests.per.connection: "${PRODUCER_MAX_IN_FLIGHT_REQUESTS:-1}"

  # Consumer Configuration (Exactly-Once Semantics)
  consumer.bootstrap.servers: "$KAFKA_BOOTSTRAP_SERVERS"
  consumer.key.deserializer: "${CONSUMER_KEY_DESERIALIZER:-org.apache.kafka.common.serialization.ByteArrayDeserializer}"
  consumer.value.deserializer: "${CONSUMER_VALUE_DESERIALIZER:-org.apache.kafka.common.serialization.ByteArrayDeserializer}"
  consumer.auto.offset.reset: "${CONSUMER_AUTO_OFFSET_RESET:-earliest}"
  consumer.enable.auto.commit: "${CONSUMER_ENABLE_AUTO_COMMIT:-false}"
  consumer.isolation.level: "${CONSUMER_ISOLATION_LEVEL:-read_committed}"
EOF

# Generate deployment for this worker cluster
cat > /tmp/worker-deployment-$WORKER_CLUSTER_NAME.yaml << EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-connect-workers-$WORKER_CLUSTER_NAME
  namespace: $NAMESPACE
  labels:
    app: kafka-connect-workers
    cluster: $WORKER_CLUSTER_NAME
    component: worker
spec:
  replicas: $REPLICAS
  selector:
    matchLabels:
      app: kafka-connect-workers
      cluster: $WORKER_CLUSTER_NAME
      component: worker
  template:
    metadata:
      labels:
        app: kafka-connect-workers
        cluster: $WORKER_CLUSTER_NAME
        component: worker
    spec:
      containers:
      - name: connect-worker
        image: ${KAFKA_CONNECT_IMAGE:-confluentinc/cp-kafka-connect}:$IMAGE_TAG
        ports:
        - containerPort: 8083
          name: rest-api
        env:
        # Basic Configuration - all from ConfigMap
        - name: CONNECT_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: bootstrap.servers
        - name: CONNECT_GROUP_ID
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: group.id
        - name: CONNECT_KEY_CONVERTER
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: key.converter
        - name: CONNECT_VALUE_CONVERTER
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: value.converter
        - name: CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: key.converter.schemas.enable
        - name: CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: value.converter.schemas.enable

        # Storage Configuration
        - name: CONNECT_OFFSET_STORAGE_TOPIC
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: offset.storage.topic
        - name: CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: offset.storage.replication.factor
        - name: CONNECT_OFFSET_STORAGE_PARTITIONS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: offset.storage.partitions
        - name: CONNECT_CONFIG_STORAGE_TOPIC
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: config.storage.topic
        - name: CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: config.storage.replication.factor
        - name: CONNECT_STATUS_STORAGE_TOPIC
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: status.storage.topic
        - name: CONNECT_STATUS_STORAGE_REPLICATION_FACTOR
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: status.storage.replication.factor
        - name: CONNECT_STATUS_STORAGE_PARTITIONS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: status.storage.partitions

        # Plugin and REST Configuration
        - name: CONNECT_PLUGIN_PATH
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: plugin.path
        - name: CONNECT_REST_PORT
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: rest.port
        - name: CONNECT_REST_ADVERTISED_HOST_NAME
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: CONNECT_REST_ADVERTISED_PORT
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: rest.advertised.port

        # Performance Configuration
        - name: CONNECT_OFFSET_FLUSH_INTERVAL_MS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: offset.flush.interval.ms
        - name: CONNECT_OFFSET_FLUSH_TIMEOUT_MS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: offset.flush.timeout.ms
        - name: CONNECT_TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: task.shutdown.graceful.timeout.ms

        # Producer Configuration
        - name: CONNECT_PRODUCER_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: producer.bootstrap.servers
        - name: CONNECT_PRODUCER_KEY_SERIALIZER
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: producer.key.serializer
        - name: CONNECT_PRODUCER_VALUE_SERIALIZER
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: producer.value.serializer
        - name: CONNECT_PRODUCER_ACKS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: producer.acks
        - name: CONNECT_PRODUCER_RETRIES
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: producer.retries
        - name: CONNECT_PRODUCER_ENABLE_IDEMPOTENCE
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: producer.enable.idempotence
        - name: CONNECT_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: producer.max.in.flight.requests.per.connection

        # Consumer Configuration
        - name: CONNECT_CONSUMER_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: consumer.bootstrap.servers
        - name: CONNECT_CONSUMER_KEY_DESERIALIZER
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: consumer.key.deserializer
        - name: CONNECT_CONSUMER_VALUE_DESERIALIZER
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: consumer.value.deserializer
        - name: CONNECT_CONSUMER_AUTO_OFFSET_RESET
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: consumer.auto.offset.reset
        - name: CONNECT_CONSUMER_ENABLE_AUTO_COMMIT
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: consumer.enable.auto.commit
        - name: CONNECT_CONSUMER_ISOLATION_LEVEL
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: consumer.isolation.level

        # Security Configuration (if configured)
        - name: CONNECT_SECURITY_PROTOCOL
          valueFrom:
            configMapKeyRef:
              name: kafka-connect-worker-config-$WORKER_CLUSTER_NAME
              key: security.protocol

        volumeMounts:
        - name: iceberg-connector
          mountPath: /usr/share/confluent-hub-components/iceberg-kafka-connect
        resources:
          requests:
            memory: "${WORKER_MEMORY_REQUEST:-1Gi}"
            cpu: "${WORKER_CPU_REQUEST:-500m}"
          limits:
            memory: "${WORKER_MEMORY_LIMIT:-2Gi}"
            cpu: "${WORKER_CPU_LIMIT:-1000m}"
        livenessProbe:
          httpGet:
            path: /
            port: 8083
          initialDelaySeconds: 60
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /
            port: 8083
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          successThreshold: 1
          failureThreshold: 3
      volumes:
      - name: iceberg-connector
        emptyDir: {}

---
apiVersion: v1
kind: Service
metadata:
  name: kafka-connect-service-$WORKER_CLUSTER_NAME
  namespace: $NAMESPACE
  labels:
    app: kafka-connect-workers
    cluster: $WORKER_CLUSTER_NAME
spec:
  selector:
    app: kafka-connect-workers
    cluster: $WORKER_CLUSTER_NAME
    component: worker
  ports:
  - port: 8083
    targetPort: 8083
    name: rest-api
    protocol: TCP
  type: ClusterIP

---
apiVersion: v1
kind: Service
metadata:
  name: kafka-connect-headless-$WORKER_CLUSTER_NAME
  namespace: $NAMESPACE
  labels:
    app: kafka-connect-workers
    cluster: $WORKER_CLUSTER_NAME
spec:
  selector:
    app: kafka-connect-workers
    cluster: $WORKER_CLUSTER_NAME
    component: worker
  ports:
  - port: 8083
    targetPort: 8083
    name: rest-api
    protocol: TCP
  clusterIP: None
  type: ClusterIP
EOF

# Apply the configurations
echo "Applying worker configuration..."
kubectl apply -f /tmp/worker-config-$WORKER_CLUSTER_NAME.yaml

echo "Deploying workers..."
kubectl apply -f /tmp/worker-deployment-$WORKER_CLUSTER_NAME.yaml

# Wait for deployment to be ready
echo "Waiting for workers to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/kafka-connect-workers-$WORKER_CLUSTER_NAME -n $NAMESPACE

echo "Workers deployed successfully!"
echo "Cluster Name: $WORKER_CLUSTER_NAME"
echo "Namespace: $NAMESPACE"
echo "Replicas: $REPLICAS"

# Show deployment status
kubectl get pods -n $NAMESPACE -l cluster=$WORKER_CLUSTER_NAME

# Cleanup temp files
rm -f /tmp/worker-config-$WORKER_CLUSTER_NAME.yaml /tmp/worker-deployment-$WORKER_CLUSTER_NAME.yaml

echo ""
echo "To check worker logs:"
echo "  kubectl logs -n $NAMESPACE -l cluster=$WORKER_CLUSTER_NAME -f"
echo ""
echo "To access REST API:"
echo "  kubectl port-forward -n $NAMESPACE service/kafka-connect-service-$WORKER_CLUSTER_NAME 8083:8083"
echo "  curl http://localhost:8083/"
echo ""
echo "To delete this worker cluster:"
echo "  kubectl delete deployment,service,configmap -n $NAMESPACE -l cluster=$WORKER_CLUSTER_NAME"