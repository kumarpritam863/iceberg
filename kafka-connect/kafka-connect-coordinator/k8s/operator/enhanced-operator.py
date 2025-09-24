#!/usr/bin/env python3
"""
Enhanced Iceberg Connect Operator with HTTP API endpoints
for coordinator deployment triggered by connectors.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from flask import Flask, request, jsonify
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnhancedIcebergConnectController:
    def __init__(self):
        """Initialize the enhanced operator controller."""
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except config.ConfigException:
            config.load_kube_config()
            logger.info("Loaded local Kubernetes config")

        # Initialize Kubernetes clients
        self.k8s_apps = client.AppsV1Api()
        self.k8s_core = client.CoreV1Api()
        self.k8s_custom = client.CustomObjectsApi()

        # Operator configuration
        self.namespace = os.getenv('OPERATOR_NAMESPACE', 'default')
        self.group = 'kafka.iceberg.apache.org'
        self.version = 'v1'
        self.plural = 'icebergconnects'

        # Flask app for HTTP API
        self.app = Flask(__name__)
        self.setup_routes()

    def setup_routes(self):
        """Setup HTTP API routes."""

        @self.app.route('/health', methods=['GET'])
        def health():
            return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

        @self.app.route('/api/v1/coordinators', methods=['POST'])
        def create_coordinator():
            try:
                spec = request.get_json()
                connector_name = spec.get('connectorName')

                logger.info(f"Received coordinator deployment request for connector: {connector_name}")

                # Create IcebergConnect resource
                coordinator_resource = self.create_coordinator_resource_from_spec(spec)

                self.k8s_custom.create_namespaced_custom_object(
                    group=self.group,
                    version=self.version,
                    namespace=spec.get('namespace', self.namespace),
                    plural=self.plural,
                    body=coordinator_resource
                )

                logger.info(f"Created coordinator resource for connector: {connector_name}")

                return jsonify({
                    'status': 'success',
                    'message': f'Coordinator deployment initiated for {connector_name}',
                    'resourceName': self.get_coordinator_resource_name(connector_name)
                }), 201

            except Exception as e:
                logger.error(f"Failed to create coordinator: {e}")
                return jsonify({
                    'status': 'error',
                    'message': str(e)
                }), 500

        @self.app.route('/api/v1/coordinators/<resource_name>', methods=['DELETE'])
        def delete_coordinator(resource_name):
            try:
                logger.info(f"Received coordinator deletion request for: {resource_name}")

                self.k8s_custom.delete_namespaced_custom_object(
                    group=self.group,
                    version=self.version,
                    namespace=self.namespace,
                    plural=self.plural,
                    name=resource_name
                )

                logger.info(f"Deleted coordinator resource: {resource_name}")

                return jsonify({
                    'status': 'success',
                    'message': f'Coordinator {resource_name} deletion initiated'
                }), 200

            except ApiException as e:
                if e.status == 404:
                    return jsonify({
                        'status': 'success',
                        'message': f'Coordinator {resource_name} not found (already deleted)'
                    }), 200
                else:
                    logger.error(f"Failed to delete coordinator {resource_name}: {e}")
                    return jsonify({
                        'status': 'error',
                        'message': str(e)
                    }), 500
            except Exception as e:
                logger.error(f"Failed to delete coordinator {resource_name}: {e}")
                return jsonify({
                    'status': 'error',
                    'message': str(e)
                }), 500

        @self.app.route('/api/v1/coordinators/<resource_name>/status', methods=['GET'])
        def get_coordinator_status(resource_name):
            try:
                # Get the IcebergConnect resource
                resource = self.k8s_custom.get_namespaced_custom_object(
                    group=self.group,
                    version=self.version,
                    namespace=self.namespace,
                    plural=self.plural,
                    name=resource_name
                )

                status = resource.get('status', {})
                phase = status.get('phase', 'Unknown')

                return jsonify({
                    'status': 'success',
                    'phase': phase,
                    'message': status.get('message', ''),
                    'lastTransitionTime': status.get('lastTransitionTime', ''),
                    'ready': phase == 'Ready'
                }), 200

            except ApiException as e:
                if e.status == 404:
                    return jsonify({
                        'status': 'error',
                        'phase': 'NotFound',
                        'message': f'Coordinator {resource_name} not found',
                        'ready': False
                    }), 404
                else:
                    logger.error(f"Failed to get coordinator status {resource_name}: {e}")
                    return jsonify({
                        'status': 'error',
                        'message': str(e),
                        'ready': False
                    }), 500
            except Exception as e:
                logger.error(f"Failed to get coordinator status {resource_name}: {e}")
                return jsonify({
                    'status': 'error',
                    'message': str(e),
                    'ready': False
                }), 500

    def create_coordinator_resource_from_spec(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create IcebergConnect resource from coordinator spec."""
        connector_name = spec['connectorName']
        resource_name = self.get_coordinator_resource_name(connector_name)
        namespace = spec.get('namespace', self.namespace)

        return {
            'apiVersion': f'{self.group}/{self.version}',
            'kind': 'IcebergConnect',
            'metadata': {
                'name': resource_name,
                'namespace': namespace,
                'labels': {
                    'connector': connector_name,
                    'managed-by': 'iceberg-connect-operator',
                    'deployment-type': 'coordinator-only'
                }
            },
            'spec': {
                's3Config': {
                    'region': os.getenv('COORDINATOR_S3_REGION', 'us-west-2'),
                    'bucket': os.getenv('COORDINATOR_S3_BUCKET', ''),
                    'accessKeyId': os.getenv('AWS_ACCESS_KEY_ID', ''),
                    'secretAccessKey': os.getenv('AWS_SECRET_ACCESS_KEY', '')
                },
                'images': {
                    'coordinator': {
                        's3Path': spec.get('coordinator', {}).get('image', '').replace(':', '-') + '.tar',
                        'tag': 'latest',
                        'registry': os.getenv('COORDINATOR_IMAGE_REGISTRY', 'localhost:5000')
                    },
                    'worker': {
                        's3Path': 'images/dummy-worker.tar',  # Dummy worker for coordinator-only deployment
                        'tag': 'latest'
                    }
                },
                'worker': {
                    'replicas': 0,  # Coordinator-only deployment
                    'kafka': spec.get('kafka', {'bootstrapServers': 'kafka:9092'})
                },
                'coordinator': {
                    'replicas': spec.get('coordinator', {}).get('replicas', 1),
                    'job': spec.get('job', {}),
                    'kafka': spec.get('kafka', {})
                }
            }
        }

    def get_coordinator_resource_name(self, connector_name: str) -> str:
        """Generate coordinator resource name from connector name."""
        return f"coordinator-{connector_name.lower().replace('_', '-').replace('.', '-')}"

    def run_api_server(self):
        """Run the HTTP API server."""
        port = int(os.getenv('API_PORT', '8080'))
        logger.info(f"Starting HTTP API server on port {port}")
        self.app.run(host='0.0.0.0', port=port, debug=False)

    async def run_operator(self):
        """Run the main operator loop (existing functionality)."""
        logger.info("Starting Iceberg Connect Operator Controller")

        # This would contain the existing operator logic
        # For now, we'll just run a simple monitoring loop
        while True:
            try:
                # Monitor IcebergConnect resources and update their status
                await self.monitor_resources()
                await asyncio.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Error in operator loop: {e}")
                await asyncio.sleep(10)

    async def monitor_resources(self):
        """Monitor IcebergConnect resources and update their status."""
        try:
            resources = self.k8s_custom.list_cluster_custom_object(
                group=self.group,
                version=self.version,
                plural=self.plural
            )

            for resource in resources.get('items', []):
                await self.update_resource_status(resource)

        except Exception as e:
            logger.error(f"Error monitoring resources: {e}")

    async def update_resource_status(self, resource: Dict[str, Any]):
        """Update the status of an IcebergConnect resource based on actual deployment state."""
        name = resource['metadata']['name']
        namespace = resource['metadata']['namespace']

        try:
            # Check if coordinator deployment exists and is ready
            deployment_name = f"iceberg-job-manager-{name.replace('coordinator-', '')}"

            try:
                deployment = self.k8s_apps.read_namespaced_deployment(deployment_name, namespace)
                ready_replicas = deployment.status.ready_replicas or 0
                desired_replicas = deployment.spec.replicas or 0

                if ready_replicas >= desired_replicas and desired_replicas > 0:
                    phase = 'Ready'
                    message = f'Coordinator deployment is ready ({ready_replicas}/{desired_replicas} replicas)'
                else:
                    phase = 'Deploying'
                    message = f'Coordinator deployment in progress ({ready_replicas}/{desired_replicas} replicas ready)'

            except ApiException as e:
                if e.status == 404:
                    phase = 'Pending'
                    message = 'Coordinator deployment not found'
                else:
                    raise

            # Update resource status
            resource['status'] = {
                'phase': phase,
                'message': message,
                'lastTransitionTime': datetime.now(timezone.utc).isoformat()
            }

            self.k8s_custom.patch_namespaced_custom_object_status(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=self.plural,
                name=name,
                body=resource
            )

        except Exception as e:
            logger.error(f"Failed to update status for {namespace}/{name}: {e}")


def main():
    """Main entry point."""
    import os

    controller = EnhancedIcebergConnectController()

    # Start API server in a separate thread
    api_thread = threading.Thread(target=controller.run_api_server, daemon=True)
    api_thread.start()

    # Run the operator
    asyncio.run(controller.run_operator())


if __name__ == '__main__':
    main()