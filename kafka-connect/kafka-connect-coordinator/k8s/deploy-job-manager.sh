#!/bin/bash
set -e

# Configuration
NAMESPACE=${NAMESPACE:-default}
JOB_NAME=${JOB_NAME:-iceberg-job-$(date +%s)}
IMAGE_TAG=${IMAGE_TAG:-latest}

# Required environment variables
REQUIRED_VARS=(
    "KAFKA_BOOTSTRAP_SERVERS"
    "CONNECT_GROUP_ID"
    "CONTROL_TOPIC"
    "CATALOG_NAME"
)

# Validate required environment variables
for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var}" ]]; then
        echo "Error: Required environment variable $var is not set"
        exit 1
    fi
done

echo "Deploying Iceberg Job Manager for job: $JOB_NAME"
echo "Namespace: $NAMESPACE"
echo "Connect Group ID: $CONNECT_GROUP_ID"
echo "Control Topic: $CONTROL_TOPIC"
echo "Catalog: $CATALOG_NAME"

# Create namespace if it doesn't exist
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Generate unique ConfigMap for this job
cat > /tmp/job-config-$JOB_NAME.yaml << EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: iceberg-job-config-$JOB_NAME
  namespace: $NAMESPACE
data:
  JOB_ID: "$JOB_NAME"
  KAFKA_BOOTSTRAP_SERVERS: "$KAFKA_BOOTSTRAP_SERVERS"
  CONNECT_GROUP_ID: "$CONNECT_GROUP_ID"
  CONTROL_TOPIC: "$CONTROL_TOPIC"
  CATALOG_NAME: "$CATALOG_NAME"
$(env | grep '^CATALOG_' | sed 's/^/  /' | sed 's/=/: "/' | sed 's/$/"/')
  COMMIT_INTERVAL_MINUTES: "${COMMIT_INTERVAL_MINUTES:-5}"
  COMMIT_TIMEOUT_MINUTES: "${COMMIT_TIMEOUT_MINUTES:-30}"
  COMMIT_THREADS: "${COMMIT_THREADS:-4}"
  KEEPALIVE_TIMEOUT_MS: "${KEEPALIVE_TIMEOUT_MS:-60000}"
  ENABLE_TRANSACTIONS: "${ENABLE_TRANSACTIONS:-true}"
  TRANSACTION_TIMEOUT_MINUTES: "${TRANSACTION_TIMEOUT_MINUTES:-15}"
$([ -n "$TRANSACTIONAL_ID" ] && echo "  TRANSACTIONAL_ID: \"$TRANSACTIONAL_ID\"")
EOF

# Generate unique deployment for this job
cat > /tmp/job-manager-$JOB_NAME.yaml << EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: iceberg-job-manager-$JOB_NAME
  namespace: $NAMESPACE
  labels:
    app: iceberg-job-manager
    job: $JOB_NAME
    component: coordinator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: iceberg-job-manager
      job: $JOB_NAME
      component: coordinator
  template:
    metadata:
      labels:
        app: iceberg-job-manager
        job: $JOB_NAME
        component: coordinator
    spec:
      containers:
      - name: job-manager
        image: iceberg/kafka-connect-coordinator:$IMAGE_TAG
        imagePullPolicy: IfNotPresent
        envFrom:
        - configMapRef:
            name: iceberg-job-config-$JOB_NAME
        ports:
        - containerPort: 8080
          name: metrics
        resources:
          requests:
            memory: "${JOB_MANAGER_MEMORY_REQUEST:-512Mi}"
            cpu: "${JOB_MANAGER_CPU_REQUEST:-200m}"
          limits:
            memory: "${JOB_MANAGER_MEMORY_LIMIT:-1Gi}"
            cpu: "${JOB_MANAGER_CPU_LIMIT:-500m}"
        livenessProbe:
          exec:
            command:
            - /bin/sh
            - -c
            - "ps aux | grep '[I]cebergJobManagerApplication' | wc -l | grep -q 1"
          initialDelaySeconds: 30
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
        readinessProbe:
          exec:
            command:
            - /bin/sh
            - -c
            - "ps aux | grep '[I]cebergJobManagerApplication' | wc -l | grep -q 1"
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3

---
apiVersion: v1
kind: Service
metadata:
  name: iceberg-job-manager-service-$JOB_NAME
  namespace: $NAMESPACE
  labels:
    app: iceberg-job-manager
    job: $JOB_NAME
spec:
  selector:
    app: iceberg-job-manager
    job: $JOB_NAME
    component: coordinator
  ports:
  - port: 8080
    targetPort: 8080
    name: metrics
  type: ClusterIP
EOF

# Apply the configurations
echo "Applying job configuration..."
kubectl apply -f /tmp/job-config-$JOB_NAME.yaml

echo "Deploying job manager..."
kubectl apply -f /tmp/job-manager-$JOB_NAME.yaml

# Wait for deployment to be ready
echo "Waiting for job manager to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/iceberg-job-manager-$JOB_NAME -n $NAMESPACE

echo "Job manager deployed successfully!"
echo "Job Name: $JOB_NAME"
echo "Namespace: $NAMESPACE"

# Show deployment status
kubectl get pods -n $NAMESPACE -l job=$JOB_NAME

# Cleanup temp files
rm -f /tmp/job-config-$JOB_NAME.yaml /tmp/job-manager-$JOB_NAME.yaml

echo ""
echo "To check job manager logs:"
echo "  kubectl logs -n $NAMESPACE -l job=$JOB_NAME -f"
echo ""
echo "To delete this job:"
echo "  kubectl delete deployment,service,configmap -n $NAMESPACE -l job=$JOB_NAME"