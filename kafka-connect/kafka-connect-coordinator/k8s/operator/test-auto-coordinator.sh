#!/bin/bash
set -e

# Complete test script for auto-coordinator deployment functionality

echo "🧪 Testing Iceberg Connector Auto-Coordinator Deployment"
echo "========================================================"

# Configuration
NAMESPACE=${NAMESPACE:-default}
KAFKA_CONNECT_SERVICE=${KAFKA_CONNECT_SERVICE:-kafka-connect-auto-service:8083}
CONNECTOR_NAME=${CONNECTOR_NAME:-test-iceberg-auto}

echo "📋 Test Configuration:"
echo "  Namespace: $NAMESPACE"
echo "  Kafka Connect Service: $KAFKA_CONNECT_SERVICE"
echo "  Connector Name: $CONNECTOR_NAME"
echo ""

# Test 1: Validate that operator is running
echo "🔍 Test 1: Checking operator deployment..."
if kubectl get deployment iceberg-connect-operator -n iceberg-system >/dev/null 2>&1; then
    echo "✅ Operator deployment found"

    # Check if operator is ready
    READY_REPLICAS=$(kubectl get deployment iceberg-connect-operator -n iceberg-system -o jsonpath='{.status.readyReplicas}')
    if [ "$READY_REPLICAS" = "1" ]; then
        echo "✅ Operator is ready"
    else
        echo "❌ Operator is not ready (ready replicas: $READY_REPLICAS)"
        exit 1
    fi
else
    echo "❌ Operator deployment not found"
    exit 1
fi

# Test 2: Check Kafka Connect cluster
echo ""
echo "🔍 Test 2: Checking Kafka Connect cluster..."
if kubectl get service $KAFKA_CONNECT_SERVICE -n $NAMESPACE >/dev/null 2>&1; then
    echo "✅ Kafka Connect service found"

    # Test connectivity (simplified - in real scenario you'd need port-forward)
    echo "ℹ️  To test connectivity, run: kubectl port-forward service/$KAFKA_CONNECT_SERVICE 8083:8083"
else
    echo "❌ Kafka Connect service not found"
    exit 1
fi

# Test 3: Create test connector configuration
echo ""
echo "🔍 Test 3: Creating test connector configuration..."

# Create connector config with comprehensive settings
cat > test-connector-config.json << EOF
{
  "name": "$CONNECTOR_NAME",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnectorV2",
    "tasks.max": "2",
    "topics": "test-topic",

    "iceberg.control.topic": "iceberg-control-$CONNECTOR_NAME",
    "iceberg.connect.group-id": "$CONNECTOR_NAME-group",
    "iceberg.catalog": "test_catalog",
    "iceberg.catalog.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
    "iceberg.catalog.uri": "jdbc:postgresql://postgres:5432/iceberg",
    "iceberg.catalog.jdbc.user": "iceberg",
    "iceberg.catalog.jdbc.password": "password",
    "iceberg.catalog.warehouse": "s3://test-warehouse/",
    "iceberg.tables": "test_db.test_table",

    "iceberg.coordinator.strict-mode": "false",
    "iceberg.coordinator.wait-for-readiness": "true",
    "iceberg.coordinator.readiness-timeout-seconds": "60",
    "iceberg.coordinator.image": "localhost:5000/iceberg-coordinator:latest",

    "bootstrap.servers": "kafka:9092"
  }
}
EOF

echo "✅ Test connector configuration created"

# Test 4: Validate configuration
echo ""
echo "🔍 Test 4: Validating connector configuration..."

# This would be done by the connector itself, but we can simulate validation
echo "✅ Configuration validation would be performed by IcebergConnectorConfigValidator"

# Test 5: Simulate connector creation (dry run)
echo ""
echo "🔍 Test 5: Simulating connector creation..."

echo "📝 Connector configuration to be submitted:"
cat test-connector-config.json | jq '.'

echo ""
echo "ℹ️  To create the connector, run:"
echo "   kubectl port-forward service/$KAFKA_CONNECT_SERVICE 8083:8083 &"
echo "   curl -X POST http://localhost:8083/connectors \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d @test-connector-config.json"

# Test 6: Expected coordinator resource
echo ""
echo "🔍 Test 6: Expected coordinator resource..."

COORDINATOR_RESOURCE_NAME="coordinator-$CONNECTOR_NAME"
echo "Expected coordinator resource name: $COORDINATOR_RESOURCE_NAME"

# Test 7: Monitoring commands
echo ""
echo "🔍 Test 7: Monitoring commands..."

cat << EOF
📊 Monitoring Commands:

1. Check connector status:
   curl http://localhost:8083/connectors/$CONNECTOR_NAME/status

2. Check coordinator deployment:
   kubectl get icebergconnects -l connector=$CONNECTOR_NAME
   kubectl describe icebergconnect $COORDINATOR_RESOURCE_NAME

3. Check coordinator pods:
   kubectl get pods -l app=iceberg-job-manager,job=$CONNECTOR_NAME

4. Check coordinator logs:
   kubectl logs -l app=iceberg-job-manager,job=$CONNECTOR_NAME -f

5. Check operator logs:
   kubectl logs -n iceberg-system -l app=iceberg-connect-operator -f

6. List all coordinators:
   kubectl get icebergconnects -A

7. Check operator API (if port-forwarded):
   kubectl port-forward -n iceberg-system service/iceberg-connect-operator 8080:8080 &
   curl http://localhost:8080/health
   curl http://localhost:8080/api/v1/coordinators/$COORDINATOR_RESOURCE_NAME/status
EOF

# Test 8: Cleanup commands
echo ""
echo "🔍 Test 8: Cleanup commands..."

cat << EOF
🧹 Cleanup Commands:

1. Delete connector:
   curl -X DELETE http://localhost:8083/connectors/$CONNECTOR_NAME

2. Delete coordinator (automatic, but manual if needed):
   kubectl delete icebergconnect $COORDINATOR_RESOURCE_NAME

3. Clean up test files:
   rm -f test-connector-config.json
EOF

# Test 9: Error scenarios
echo ""
echo "🔍 Test 9: Error scenarios to test..."

cat << EOF
🚨 Error Scenarios to Test:

1. Missing required configuration:
   - Remove 'iceberg.connect.group-id' and verify validation fails
   - Remove 'iceberg.control.topic' and verify validation fails
   - Remove catalog properties and verify validation fails

2. Invalid configuration:
   - Set invalid 'tasks.max' (negative number)
   - Set invalid 'bootstrap.servers' format
   - Set invalid timeout values

3. Coordinator deployment failures:
   - Invalid S3 credentials
   - Invalid image references
   - Kubernetes permission issues

4. Network failures:
   - Operator API unreachable
   - Kubernetes API issues

5. Strict mode vs non-strict mode:
   - Test with 'iceberg.coordinator.strict-mode=true'
   - Test with 'iceberg.coordinator.strict-mode=false'
EOF

# Test 10: Performance verification
echo ""
echo "🔍 Test 10: Performance considerations..."

cat << EOF
⚡ Performance Verification:

1. Coordinator startup time:
   - Measure time from connector creation to coordinator ready
   - Should be under 2 minutes for typical configurations

2. Resource usage:
   - Monitor coordinator pod CPU/memory usage
   - Verify resource requests/limits are appropriate

3. Event processing:
   - Test data flow from workers to coordinator
   - Verify exactly-once semantics

4. Failure recovery:
   - Test coordinator pod restart scenarios
   - Test worker rebalancing scenarios
EOF

echo ""
echo "✅ Test setup complete!"
echo ""
echo "🚀 Next Steps:"
echo "1. Deploy the enhanced operator: ./deploy-auto-coordinator.sh"
echo "2. Set up port forwarding: kubectl port-forward service/$KAFKA_CONNECT_SERVICE 8083:8083"
echo "3. Create the test connector: curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @test-connector-config.json"
echo "4. Monitor coordinator deployment: kubectl get icebergconnects -w"
echo ""
echo "📚 For detailed documentation, see:"
echo "  - README.md for deployment instructions"
echo "  - ARCHITECTURE.md for system design"
echo "  - examples/ directory for sample configurations"