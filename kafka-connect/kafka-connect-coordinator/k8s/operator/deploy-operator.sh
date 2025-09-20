#!/bin/bash
set -e

# Configuration
OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE:-iceberg-system}
OPERATOR_IMAGE=${OPERATOR_IMAGE:-iceberg-connect-operator:latest}
REGISTRY=${REGISTRY:-localhost:5000}

echo "Deploying Iceberg Connect Operator"
echo "Namespace: $OPERATOR_NAMESPACE"
echo "Image: $OPERATOR_IMAGE"

# Create namespace if it doesn't exist
echo "Creating namespace $OPERATOR_NAMESPACE..."
kubectl create namespace "$OPERATOR_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Build operator image
echo "Building operator image..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Copy operator.py to build context
cp controller/operator.py .

# Build and tag image
docker build -t "$OPERATOR_IMAGE" .

# Push to registry if specified
if [[ "$REGISTRY" != "localhost:5000" ]]; then
    FULL_IMAGE="$REGISTRY/$OPERATOR_IMAGE"
    docker tag "$OPERATOR_IMAGE" "$FULL_IMAGE"
    docker push "$FULL_IMAGE"
    OPERATOR_IMAGE="$FULL_IMAGE"
    echo "Pushed image to registry: $FULL_IMAGE"
fi

# Clean up
rm -f operator.py

# Apply CRDs
echo "Applying Custom Resource Definitions..."
kubectl apply -f crds/

# Generate operator deployment with correct image
echo "Deploying operator..."
sed "s|iceberg-connect-operator:latest|$OPERATOR_IMAGE|g" manifests/operator-deployment.yaml | \
sed "s|iceberg-system|$OPERATOR_NAMESPACE|g" | \
kubectl apply -f -

# Wait for operator to be ready
echo "Waiting for operator to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/iceberg-connect-operator -n "$OPERATOR_NAMESPACE"

echo "Operator deployed successfully!"
echo ""
echo "To check operator logs:"
echo "  kubectl logs -n $OPERATOR_NAMESPACE -l app=iceberg-connect-operator -f"
echo ""
echo "To create an IcebergConnect resource, use:"
echo "  kubectl apply -f examples/iceberg-connect-example.yaml"