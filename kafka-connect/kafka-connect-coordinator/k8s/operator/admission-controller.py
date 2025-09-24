#!/usr/bin/env python3
"""
Kubernetes Admission Controller that monitors Kafka Connect ConfigMaps
and automatically creates coordinator deployments for Iceberg connectors.

This webhook intercepts ConfigMap creation/updates and checks if they contain
Iceberg connector configurations, then creates corresponding IcebergConnect resources.
"""

import asyncio
import base64
import json
import logging
import os
from typing import Dict, Any, Optional

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


class IcebergConnectorAdmissionController:
    def __init__(self):
        """Initialize the admission controller."""
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except config.ConfigException:
            config.load_kube_config()
            logger.info("Loaded local Kubernetes config")

        self.k8s_custom = client.CustomObjectsApi()
        self.group = 'kafka.iceberg.apache.org'
        self.version = 'v1'
        self.plural = 'icebergconnects'

    def is_iceberg_connector_config(self, config_data: Dict[str, str]) -> bool:
        """Check if ConfigMap contains Iceberg connector configuration."""
        connector_class = config_data.get('connector.class', '')
        return connector_class == 'org.apache.iceberg.connect.IcebergSinkConnector'

    def extract_connector_configs(self, configmap_data: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        """Extract individual connector configurations from ConfigMap."""
        connectors = {}

        # Look for connector configurations
        # Assumes format: connector-{name}-{property} = value
        for key, value in configmap_data.items():
            if key.startswith('connector-') and '.' in key:
                parts = key.split('-', 2)  # connector-{name}-{property}
                if len(parts) >= 3:
                    connector_name = parts[1]
                    property_name = '-'.join(parts[2:])

                    if connector_name not in connectors:
                        connectors[connector_name] = {}
                    connectors[connector_name][property_name] = value

        return {name: config for name, config in connectors.items()
                if self.is_iceberg_connector_config(config)}

    def create_coordinator_resource(self, connector_name: str, connector_config: Dict[str, str],
                                   namespace: str) -> Dict[str, Any]:
        """Create IcebergConnect resource for the connector."""
        resource_name = f"coordinator-{connector_name.lower().replace('_', '-')}"

        return {
            'apiVersion': f'{self.group}/{self.version}',
            'kind': 'IcebergConnect',
            'metadata': {
                'name': resource_name,
                'namespace': namespace,
                'labels': {
                    'connector': connector_name,
                    'managed-by': 'iceberg-admission-controller'
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
                        's3Path': connector_config.get('iceberg.coordinator.image.s3-path',
                                                     'images/coordinator.tar'),
                        'tag': connector_config.get('iceberg.coordinator.image.tag', 'latest'),
                        'registry': connector_config.get('iceberg.coordinator.image.registry',
                                                       'localhost:5000')
                    },
                    'worker': {
                        's3Path': 'images/dummy-worker.tar',  # Dummy worker
                        'tag': 'latest'
                    }
                },
                'worker': {
                    'replicas': 0,  # Coordinator-only deployment
                    'kafka': {
                        'bootstrapServers': connector_config.get('bootstrap.servers', 'kafka:9092')
                    }
                },
                'coordinator': {
                    'replicas': 1,
                    'job': {
                        'controlTopic': connector_config.get('iceberg.control.topic'),
                        'groupId': connector_config.get('iceberg.connect.group-id'),
                        'catalogProperties': {
                            key.replace('iceberg.catalog.', ''): value
                            for key, value in connector_config.items()
                            if key.startswith('iceberg.catalog.')
                        }
                    }
                }
            }
        }

    async def process_configmap(self, configmap: Dict[str, Any], operation: str):
        """Process ConfigMap and create/delete coordinators as needed."""
        name = configmap['metadata']['name']
        namespace = configmap['metadata']['namespace']

        if operation == 'DELETE':
            await self.cleanup_coordinators_for_configmap(name, namespace)
            return

        config_data = configmap.get('data', {})
        iceberg_connectors = self.extract_connector_configs(config_data)

        for connector_name, connector_config in iceberg_connectors.items():
            try:
                await self.ensure_coordinator_exists(connector_name, connector_config, namespace)
            except Exception as e:
                logger.error(f"Failed to ensure coordinator for {connector_name}: {e}")

    async def ensure_coordinator_exists(self, connector_name: str, connector_config: Dict[str, str],
                                       namespace: str):
        """Ensure coordinator exists for the given connector."""
        resource_name = f"coordinator-{connector_name.lower().replace('_', '-')}"

        try:
            # Check if coordinator already exists
            existing = self.k8s_custom.get_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=self.plural,
                name=resource_name
            )
            logger.info(f"Coordinator {resource_name} already exists")
            return
        except ApiException as e:
            if e.status != 404:
                raise
            # Resource doesn't exist, create it

        # Create new coordinator resource
        coordinator_resource = self.create_coordinator_resource(
            connector_name, connector_config, namespace
        )

        self.k8s_custom.create_namespaced_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            body=coordinator_resource
        )

        logger.info(f"Created coordinator {resource_name} for connector {connector_name}")

    async def cleanup_coordinators_for_configmap(self, configmap_name: str, namespace: str):
        """Clean up coordinators when ConfigMap is deleted."""
        try:
            # List all coordinators in the namespace
            coordinators = self.k8s_custom.list_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=self.plural,
                label_selector='managed-by=iceberg-admission-controller'
            )

            for coordinator in coordinators.get('items', []):
                coordinator_name = coordinator['metadata']['name']
                # Delete coordinator (you might want more sophisticated matching logic)
                self.k8s_custom.delete_namespaced_custom_object(
                    group=self.group,
                    version=self.version,
                    namespace=namespace,
                    plural=self.plural,
                    name=coordinator_name
                )
                logger.info(f"Deleted coordinator {coordinator_name}")

        except Exception as e:
            logger.error(f"Failed to cleanup coordinators for ConfigMap {configmap_name}: {e}")


# Global controller instance
controller = IcebergConnectorAdmissionController()


@app.route('/mutate', methods=['POST'])
def mutate():
    """Admission webhook mutate endpoint."""
    admission_request = request.get_json()

    # Extract the object being created/updated
    obj = admission_request['request']['object']
    operation = admission_request['request']['operation']

    # Only process ConfigMaps
    if obj.get('kind') != 'ConfigMap':
        return jsonify({
            'apiVersion': 'admission.k8s.io/v1',
            'kind': 'AdmissionResponse',
            'response': {
                'uid': admission_request['request']['uid'],
                'allowed': True
            }
        })

    try:
        # Process the ConfigMap asynchronously
        asyncio.create_task(controller.process_configmap(obj, operation))

        response = {
            'apiVersion': 'admission.k8s.io/v1',
            'kind': 'AdmissionResponse',
            'response': {
                'uid': admission_request['request']['uid'],
                'allowed': True
            }
        }

        return jsonify(response)

    except Exception as e:
        logger.error(f"Error in admission webhook: {e}")
        return jsonify({
            'apiVersion': 'admission.k8s.io/v1',
            'kind': 'AdmissionResponse',
            'response': {
                'uid': admission_request['request']['uid'],
                'allowed': False,
                'status': {
                    'code': 500,
                    'message': f'Error processing request: {str(e)}'
                }
            }
        }), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'})


if __name__ == '__main__':
    logger.info("Starting Iceberg Connector Admission Controller")
    app.run(host='0.0.0.0', port=8443, ssl_context='adhoc')