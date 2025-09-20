#!/usr/bin/env python3
"""
Iceberg Kafka Connect Operator Controller

This controller manages IcebergConnect custom resources by:
1. Downloading images from S3 locations
2. Building and pushing images to registry
3. Deploying workers and coordinators
4. Managing configuration and status updates
"""

import asyncio
import base64
import json
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

import boto3
import yaml
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IcebergConnectController:
    def __init__(self):
        """Initialize the operator controller."""
        # Load Kubernetes config
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

    async def run(self):
        """Main controller loop."""
        logger.info("Starting Iceberg Connect Operator Controller")

        w = watch.Watch()
        try:
            async for event in w.stream(
                self.k8s_custom.list_cluster_custom_object,
                group=self.group,
                version=self.version,
                plural=self.plural,
                _request_timeout=60
            ):
                await self.handle_event(event)
        except Exception as e:
            logger.error(f"Error in main controller loop: {e}")
            raise

    async def handle_event(self, event):
        """Handle Kubernetes events for IcebergConnect resources."""
        event_type = event['type']
        resource = event['object']

        name = resource['metadata']['name']
        namespace = resource['metadata']['namespace']
        uid = resource['metadata']['uid']

        logger.info(f"Handling {event_type} event for {namespace}/{name}")

        try:
            if event_type in ['ADDED', 'MODIFIED']:
                await self.reconcile_resource(namespace, name, resource)
            elif event_type == 'DELETED':
                await self.cleanup_resource(namespace, name, uid)
        except Exception as e:
            logger.error(f"Error handling event for {namespace}/{name}: {e}")
            await self.update_status(namespace, name, 'Failed', str(e))

    async def reconcile_resource(self, namespace: str, name: str, resource: Dict[str, Any]):
        """Reconcile an IcebergConnect resource to desired state."""
        spec = resource['spec']

        # Update status to indicate processing
        await self.update_status(namespace, name, 'Downloading', 'Downloading images from S3')

        try:
            # Step 1: Download and build images
            images = await self.download_and_build_images(spec['s3Config'], spec['images'])

            # Step 2: Update status
            await self.update_status(namespace, name, 'Deploying', 'Deploying workers and coordinators')

            # Step 3: Deploy workers
            await self.deploy_workers(namespace, name, spec['worker'], images['worker'])

            # Step 4: Deploy coordinators
            await self.deploy_coordinator(namespace, name, spec['coordinator'], images['coordinator'])

            # Step 5: Update final status
            await self.update_status(namespace, name, 'Ready', 'All components deployed successfully')

        except Exception as e:
            logger.error(f"Error reconciling {namespace}/{name}: {e}")
            await self.update_status(namespace, name, 'Failed', str(e))
            raise

    async def download_and_build_images(self, s3_config: Dict[str, Any], images_spec: Dict[str, Any]) -> Dict[str, str]:
        """Download images from S3 and build/push to registry."""
        logger.info("Starting image download and build process")

        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            region_name=s3_config['region'],
            aws_access_key_id=s3_config['accessKeyId'],
            aws_secret_access_key=s3_config['secretAccessKey'],
            endpoint_url=s3_config.get('endpoint')
        )

        built_images = {}

        for component, image_config in images_spec.items():
            logger.info(f"Processing {component} image from {image_config['s3Path']}")

            # Create temporary directory for image processing
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Download image from S3
                s3_path = image_config['s3Path']
                local_file = temp_path / f"{component}-image.tar"

                logger.info(f"Downloading {s3_path} from S3 bucket {s3_config['bucket']}")
                s3_client.download_file(s3_config['bucket'], s3_path, str(local_file))

                # Determine target image name
                registry = image_config.get('registry', 'localhost:5000')
                tag = image_config.get('tag', 'latest')
                target_image = f"{registry}/iceberg-kafka-connect-{component}:{tag}"

                # Load and tag image
                await self.run_command(['docker', 'load', '-i', str(local_file)])

                # Get loaded image ID/name
                result = await self.run_command(['docker', 'images', '--format', '{{.Repository}}:{{.Tag}}', '--filter', 'dangling=false'])
                loaded_images = result.stdout.strip().split('\n')

                # Find the most recently loaded image (this is a simplification)
                source_image = loaded_images[-1] if loaded_images else None
                if not source_image:
                    raise RuntimeError(f"Failed to identify loaded image for {component}")

                # Tag and push to target registry
                await self.run_command(['docker', 'tag', source_image, target_image])
                await self.run_command(['docker', 'push', target_image])

                built_images[component] = target_image
                logger.info(f"Successfully built and pushed {component} image: {target_image}")

        return built_images

    async def deploy_workers(self, namespace: str, name: str, worker_spec: Dict[str, Any], worker_image: str):
        """Deploy Kafka Connect workers."""
        logger.info(f"Deploying workers for {namespace}/{name}")

        cluster_name = worker_spec.get('clusterName', f"{name}-workers")
        replicas = worker_spec.get('replicas', 3)

        # Create ConfigMap for worker configuration
        config_map = self.create_worker_configmap(namespace, cluster_name, worker_spec)
        await self.apply_resource(config_map)

        # Create Worker Deployment
        deployment = self.create_worker_deployment(
            namespace, cluster_name, replicas, worker_image, worker_spec
        )
        await self.apply_resource(deployment)

        # Create Services
        service = self.create_worker_service(namespace, cluster_name)
        await self.apply_resource(service)

        headless_service = self.create_worker_headless_service(namespace, cluster_name)
        await self.apply_resource(headless_service)

    async def deploy_coordinator(self, namespace: str, name: str, coordinator_spec: Dict[str, Any], coordinator_image: str):
        """Deploy job coordinator."""
        logger.info(f"Deploying coordinator for {namespace}/{name}")

        replicas = coordinator_spec.get('replicas', 1)

        # Create ConfigMap for coordinator configuration
        config_map = self.create_coordinator_configmap(namespace, name, coordinator_spec)
        await self.apply_resource(config_map)

        # Create Coordinator Deployment
        deployment = self.create_coordinator_deployment(
            namespace, name, replicas, coordinator_image, coordinator_spec
        )
        await self.apply_resource(deployment)

    def create_worker_configmap(self, namespace: str, cluster_name: str, worker_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create ConfigMap for worker configuration."""
        kafka_config = worker_spec['kafka']
        storage_config = worker_spec.get('storage', {})
        performance_config = worker_spec.get('performance', {})

        # Build properties file content
        properties_lines = [
            "# Kafka Connect Worker Configuration",
            f"bootstrap.servers={kafka_config['bootstrapServers']}",
            f"group.id=connect-{cluster_name}",
            "key.converter=org.apache.kafka.connect.json.JsonConverter",
            "value.converter=org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable=false",
            "value.converter.schemas.enable=false",
            "",
            "# Storage Topics",
            f"offset.storage.topic={storage_config.get('offsetTopic', 'connect-offsets')}",
            f"offset.storage.replication.factor={storage_config.get('offsetReplicationFactor', 1)}",
            f"offset.storage.partitions={storage_config.get('offsetPartitions', 25)}",
            f"config.storage.topic={storage_config.get('configTopic', 'connect-configs')}",
            f"config.storage.replication.factor={storage_config.get('configReplicationFactor', 1)}",
            f"status.storage.topic={storage_config.get('statusTopic', 'connect-status')}",
            f"status.storage.replication.factor={storage_config.get('statusReplicationFactor', 1)}",
            f"status.storage.partitions={storage_config.get('statusPartitions', 5)}",
            "",
            "# Plugin Configuration",
            f"plugin.path={kafka_config.get('pluginPath', '/app/plugins:/app/libs')}",
            "",
            "# REST API",
            "rest.port=8083",
            "rest.advertised.port=8083",
            "",
            "# Security",
            f"security.protocol={kafka_config.get('securityProtocol', 'PLAINTEXT')}",
        ]

        # Add optional security configuration
        if kafka_config.get('saslMechanism'):
            properties_lines.append(f"sasl.mechanism={kafka_config['saslMechanism']}")
        if kafka_config.get('saslJaasConfig'):
            properties_lines.append(f"sasl.jaas.config={kafka_config['saslJaasConfig']}")

        properties_lines.extend([
            "",
            "# Performance Tuning",
            f"offset.flush.interval.ms={performance_config.get('offsetFlushIntervalMs', 10000)}",
            f"offset.flush.timeout.ms={performance_config.get('offsetFlushTimeoutMs', 5000)}",
            f"task.shutdown.graceful.timeout.ms={performance_config.get('taskShutdownTimeoutMs', 10000)}",
            "",
            "# Producer Configuration (Exactly-Once Semantics)",
            f"producer.bootstrap.servers={kafka_config['bootstrapServers']}",
            "producer.key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
            "producer.value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
            "producer.acks=all",
            "producer.retries=2147483647",
            "producer.enable.idempotence=true",
            "producer.max.in.flight.requests.per.connection=1",
            "",
            "# Consumer Configuration (Exactly-Once Semantics)",
            f"consumer.bootstrap.servers={kafka_config['bootstrapServers']}",
            "consumer.key.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "consumer.value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "consumer.auto.offset.reset=earliest",
            "consumer.enable.auto.commit=false",
            "consumer.isolation.level=read_committed",
        ])

        # Add security properties for producer/consumer if needed
        if kafka_config.get('securityProtocol') != 'PLAINTEXT':
            security_props = [
                f"producer.security.protocol={kafka_config.get('securityProtocol')}",
                f"consumer.security.protocol={kafka_config.get('securityProtocol')}",
            ]
            if kafka_config.get('saslMechanism'):
                security_props.extend([
                    f"producer.sasl.mechanism={kafka_config['saslMechanism']}",
                    f"consumer.sasl.mechanism={kafka_config['saslMechanism']}",
                ])
            if kafka_config.get('saslJaasConfig'):
                security_props.extend([
                    f"producer.sasl.jaas.config={kafka_config['saslJaasConfig']}",
                    f"consumer.sasl.jaas.config={kafka_config['saslJaasConfig']}",
                ])
            properties_lines.extend(security_props)

        properties_content = '\n'.join(properties_lines)

        return {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {
                'name': f'kafka-connect-worker-config-{cluster_name}',
                'namespace': namespace,
                'labels': {
                    'app': 'kafka-connect-workers',
                    'cluster': cluster_name,
                    'managed-by': 'iceberg-connect-operator'
                }
            },
            'data': {
                'connect-distributed.properties': properties_content
            }
        }

    def create_worker_deployment(self, namespace: str, cluster_name: str, replicas: int,
                                image: str, worker_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create Deployment for workers."""
        resources_spec = worker_spec.get('resources', {})
        requests = resources_spec.get('requests', {})
        limits = resources_spec.get('limits', {})

        return {
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'metadata': {
                'name': f'kafka-connect-workers-{cluster_name}',
                'namespace': namespace,
                'labels': {
                    'app': 'kafka-connect-workers',
                    'cluster': cluster_name,
                    'component': 'worker',
                    'managed-by': 'iceberg-connect-operator'
                }
            },
            'spec': {
                'replicas': replicas,
                'selector': {
                    'matchLabels': {
                        'app': 'kafka-connect-workers',
                        'cluster': cluster_name,
                        'component': 'worker'
                    }
                },
                'template': {
                    'metadata': {
                        'labels': {
                            'app': 'kafka-connect-workers',
                            'cluster': cluster_name,
                            'component': 'worker'
                        }
                    },
                    'spec': {
                        'containers': [{
                            'name': 'connect-worker',
                            'image': image,
                            'command': ['java'],
                            'args': [
                                '-Xmx1G', '-Xms1G',
                                '-cp', '/app/libs/*:/app/kafka-libs/*',
                                'org.apache.kafka.connect.cli.ConnectDistributed',
                                '/app/config/connect-distributed.properties'
                            ],
                            'ports': [{'containerPort': 8083, 'name': 'rest-api'}],
                            'env': [
                                {
                                    'name': 'JAVA_OPTS',
                                    'value': '-Xmx1G -Xms1G -server'
                                }
                            ],
                            'volumeMounts': [
                                {
                                    'name': 'connect-config',
                                    'mountPath': '/app/config'
                                }
                            ],
                            'resources': {
                                'requests': {
                                    'memory': requests.get('memory', '1Gi'),
                                    'cpu': requests.get('cpu', '500m')
                                },
                                'limits': {
                                    'memory': limits.get('memory', '2Gi'),
                                    'cpu': limits.get('cpu', '1000m')
                                }
                            },
                            'livenessProbe': {
                                'httpGet': {'path': '/', 'port': 8083},
                                'initialDelaySeconds': 60,
                                'periodSeconds': 30,
                                'timeoutSeconds': 10,
                                'failureThreshold': 3
                            },
                            'readinessProbe': {
                                'httpGet': {'path': '/', 'port': 8083},
                                'initialDelaySeconds': 30,
                                'periodSeconds': 10,
                                'timeoutSeconds': 5,
                                'successThreshold': 1,
                                'failureThreshold': 3
                            }
                        }],
                        'volumes': [
                            {
                                'name': 'connect-config',
                                'configMap': {
                                    'name': f'kafka-connect-worker-config-{cluster_name}'
                                }
                            }
                        ]
                    }
                }
            }
        }

    def _create_worker_env_vars(self, cluster_name: str) -> list:
        """Create environment variables for worker containers."""
        config_map_name = f'kafka-connect-worker-config-{cluster_name}'

        env_vars = []
        config_keys = [
            ('CONNECT_BOOTSTRAP_SERVERS', 'bootstrap.servers'),
            ('CONNECT_GROUP_ID', 'group.id'),
            ('CONNECT_KEY_CONVERTER', 'key.converter'),
            ('CONNECT_VALUE_CONVERTER', 'value.converter'),
            ('CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE', 'key.converter.schemas.enable'),
            ('CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE', 'value.converter.schemas.enable'),
            ('CONNECT_OFFSET_STORAGE_TOPIC', 'offset.storage.topic'),
            ('CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR', 'offset.storage.replication.factor'),
            ('CONNECT_OFFSET_STORAGE_PARTITIONS', 'offset.storage.partitions'),
            ('CONNECT_CONFIG_STORAGE_TOPIC', 'config.storage.topic'),
            ('CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR', 'config.storage.replication.factor'),
            ('CONNECT_STATUS_STORAGE_TOPIC', 'status.storage.topic'),
            ('CONNECT_STATUS_STORAGE_REPLICATION_FACTOR', 'status.storage.replication.factor'),
            ('CONNECT_STATUS_STORAGE_PARTITIONS', 'status.storage.partitions'),
            ('CONNECT_PLUGIN_PATH', 'plugin.path'),
            ('CONNECT_REST_PORT', 'rest.port'),
            ('CONNECT_REST_ADVERTISED_PORT', 'rest.advertised.port'),
            ('CONNECT_SECURITY_PROTOCOL', 'security.protocol'),
            ('CONNECT_OFFSET_FLUSH_INTERVAL_MS', 'offset.flush.interval.ms'),
            ('CONNECT_OFFSET_FLUSH_TIMEOUT_MS', 'offset.flush.timeout.ms'),
            ('CONNECT_TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS', 'task.shutdown.graceful.timeout.ms'),
            ('CONNECT_PRODUCER_BOOTSTRAP_SERVERS', 'producer.bootstrap.servers'),
            ('CONNECT_PRODUCER_ACKS', 'producer.acks'),
            ('CONNECT_PRODUCER_RETRIES', 'producer.retries'),
            ('CONNECT_PRODUCER_ENABLE_IDEMPOTENCE', 'producer.enable.idempotence'),
            ('CONNECT_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION', 'producer.max.in.flight.requests.per.connection'),
            ('CONNECT_CONSUMER_BOOTSTRAP_SERVERS', 'consumer.bootstrap.servers'),
            ('CONNECT_CONSUMER_AUTO_OFFSET_RESET', 'consumer.auto.offset.reset'),
            ('CONNECT_CONSUMER_ENABLE_AUTO_COMMIT', 'consumer.enable.auto.commit'),
            ('CONNECT_CONSUMER_ISOLATION_LEVEL', 'consumer.isolation.level'),
        ]

        for env_name, config_key in config_keys:
            env_vars.append({
                'name': env_name,
                'valueFrom': {
                    'configMapKeyRef': {
                        'name': config_map_name,
                        'key': config_key
                    }
                }
            })

        # Add special handling for advertised host name
        env_vars.append({
            'name': 'CONNECT_REST_ADVERTISED_HOST_NAME',
            'valueFrom': {
                'fieldRef': {
                    'fieldPath': 'status.podIP'
                }
            }
        })

        return env_vars

    def create_worker_service(self, namespace: str, cluster_name: str) -> Dict[str, Any]:
        """Create Service for workers."""
        return {
            'apiVersion': 'v1',
            'kind': 'Service',
            'metadata': {
                'name': f'kafka-connect-service-{cluster_name}',
                'namespace': namespace,
                'labels': {
                    'app': 'kafka-connect-workers',
                    'cluster': cluster_name,
                    'managed-by': 'iceberg-connect-operator'
                }
            },
            'spec': {
                'selector': {
                    'app': 'kafka-connect-workers',
                    'cluster': cluster_name,
                    'component': 'worker'
                },
                'ports': [{
                    'port': 8083,
                    'targetPort': 8083,
                    'name': 'rest-api',
                    'protocol': 'TCP'
                }],
                'type': 'ClusterIP'
            }
        }

    def create_worker_headless_service(self, namespace: str, cluster_name: str) -> Dict[str, Any]:
        """Create headless Service for workers."""
        service = self.create_worker_service(namespace, cluster_name)
        service['metadata']['name'] = f'kafka-connect-headless-{cluster_name}'
        service['spec']['clusterIP'] = 'None'
        return service

    def create_coordinator_configmap(self, namespace: str, name: str, coordinator_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create ConfigMap for coordinator configuration."""
        job_config = coordinator_spec['job']
        kafka_config = coordinator_spec.get('kafka', {})

        # Build catalog properties JSON
        catalog_properties = json.dumps(job_config['catalogProperties'])

        config_data = {
            'control.topic': job_config['controlTopic'],
            'connect.group.id': job_config['groupId'],
            'catalog.properties': catalog_properties,
            'kafka.bootstrap.servers': kafka_config.get('bootstrapServers', ''),
            'kafka.security.protocol': kafka_config.get('securityProtocol', 'PLAINTEXT'),
            'kafka.enable.transactions': str(kafka_config.get('enableTransactions', True)).lower(),
            'kafka.transaction.timeout.ms': str(kafka_config.get('transactionTimeoutMs', 300000)),
        }

        # Add optional security configuration
        if kafka_config.get('saslMechanism'):
            config_data['kafka.sasl.mechanism'] = kafka_config['saslMechanism']
        if kafka_config.get('saslJaasConfig'):
            config_data['kafka.sasl.jaas.config'] = kafka_config['saslJaasConfig']

        return {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {
                'name': f'iceberg-job-manager-config-{name}',
                'namespace': namespace,
                'labels': {
                    'app': 'iceberg-job-manager',
                    'job': name,
                    'managed-by': 'iceberg-connect-operator'
                }
            },
            'data': config_data
        }

    def create_coordinator_deployment(self, namespace: str, name: str, replicas: int,
                                     image: str, coordinator_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create Deployment for coordinator."""
        resources_spec = coordinator_spec.get('resources', {})
        requests = resources_spec.get('requests', {})
        limits = resources_spec.get('limits', {})

        return {
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'metadata': {
                'name': f'iceberg-job-manager-{name}',
                'namespace': namespace,
                'labels': {
                    'app': 'iceberg-job-manager',
                    'job': name,
                    'component': 'coordinator',
                    'managed-by': 'iceberg-connect-operator'
                }
            },
            'spec': {
                'replicas': replicas,
                'selector': {
                    'matchLabels': {
                        'app': 'iceberg-job-manager',
                        'job': name,
                        'component': 'coordinator'
                    }
                },
                'template': {
                    'metadata': {
                        'labels': {
                            'app': 'iceberg-job-manager',
                            'job': name,
                            'component': 'coordinator'
                        }
                    },
                    'spec': {
                        'containers': [{
                            'name': 'job-manager',
                            'image': image,
                            'command': ['java'],
                            'args': [
                                '-cp', '/app/libs/*:/app/iceberg-kafka-connect-coordinator.jar',
                                'org.apache.iceberg.connect.coordinator.IcebergJobManagerApplication'
                            ],
                            'env': self._create_coordinator_env_vars(name),
                            'resources': {
                                'requests': {
                                    'memory': requests.get('memory', '512Mi'),
                                    'cpu': requests.get('cpu', '250m')
                                },
                                'limits': {
                                    'memory': limits.get('memory', '1Gi'),
                                    'cpu': limits.get('cpu', '500m')
                                }
                            },
                            'livenessProbe': {
                                'exec': {
                                    'command': ['pgrep', '-f', 'IcebergJobManagerApplication']
                                },
                                'initialDelaySeconds': 30,
                                'periodSeconds': 30,
                                'timeoutSeconds': 5,
                                'failureThreshold': 3
                            }
                        }]
                    }
                }
            }
        }

    def _create_coordinator_env_vars(self, name: str) -> list:
        """Create environment variables for coordinator containers."""
        config_map_name = f'iceberg-job-manager-config-{name}'

        return [
            {
                'name': 'CONTROL_TOPIC',
                'valueFrom': {
                    'configMapKeyRef': {
                        'name': config_map_name,
                        'key': 'control.topic'
                    }
                }
            },
            {
                'name': 'CONNECT_GROUP_ID',
                'valueFrom': {
                    'configMapKeyRef': {
                        'name': config_map_name,
                        'key': 'connect.group.id'
                    }
                }
            },
            {
                'name': 'CATALOG_PROPERTIES',
                'valueFrom': {
                    'configMapKeyRef': {
                        'name': config_map_name,
                        'key': 'catalog.properties'
                    }
                }
            },
            {
                'name': 'KAFKA_BOOTSTRAP_SERVERS',
                'valueFrom': {
                    'configMapKeyRef': {
                        'name': config_map_name,
                        'key': 'kafka.bootstrap.servers'
                    }
                }
            },
            {
                'name': 'KAFKA_SECURITY_PROTOCOL',
                'valueFrom': {
                    'configMapKeyRef': {
                        'name': config_map_name,
                        'key': 'kafka.security.protocol'
                    }
                }
            },
            {
                'name': 'KAFKA_ENABLE_TRANSACTIONS',
                'valueFrom': {
                    'configMapKeyRef': {
                        'name': config_map_name,
                        'key': 'kafka.enable.transactions'
                    }
                }
            },
            {
                'name': 'KAFKA_TRANSACTION_TIMEOUT_MS',
                'valueFrom': {
                    'configMapKeyRef': {
                        'name': config_map_name,
                        'key': 'kafka.transaction.timeout.ms'
                    }
                }
            }
        ]

    async def apply_resource(self, resource: Dict[str, Any]):
        """Apply a Kubernetes resource."""
        kind = resource['kind']
        namespace = resource['metadata']['namespace']
        name = resource['metadata']['name']

        try:
            if kind == 'ConfigMap':
                try:
                    self.k8s_core.read_namespaced_config_map(name, namespace)
                    # Update existing ConfigMap
                    self.k8s_core.patch_namespaced_config_map(name, namespace, resource)
                    logger.info(f"Updated ConfigMap {namespace}/{name}")
                except ApiException as e:
                    if e.status == 404:
                        # Create new ConfigMap
                        self.k8s_core.create_namespaced_config_map(namespace, resource)
                        logger.info(f"Created ConfigMap {namespace}/{name}")
                    else:
                        raise

            elif kind == 'Deployment':
                try:
                    self.k8s_apps.read_namespaced_deployment(name, namespace)
                    # Update existing Deployment
                    self.k8s_apps.patch_namespaced_deployment(name, namespace, resource)
                    logger.info(f"Updated Deployment {namespace}/{name}")
                except ApiException as e:
                    if e.status == 404:
                        # Create new Deployment
                        self.k8s_apps.create_namespaced_deployment(namespace, resource)
                        logger.info(f"Created Deployment {namespace}/{name}")
                    else:
                        raise

            elif kind == 'Service':
                try:
                    self.k8s_core.read_namespaced_service(name, namespace)
                    # Update existing Service
                    self.k8s_core.patch_namespaced_service(name, namespace, resource)
                    logger.info(f"Updated Service {namespace}/{name}")
                except ApiException as e:
                    if e.status == 404:
                        # Create new Service
                        self.k8s_core.create_namespaced_service(namespace, resource)
                        logger.info(f"Created Service {namespace}/{name}")
                    else:
                        raise

        except Exception as e:
            logger.error(f"Failed to apply {kind} {namespace}/{name}: {e}")
            raise

    async def update_status(self, namespace: str, name: str, phase: str, message: str):
        """Update the status of an IcebergConnect resource."""
        try:
            # Get current resource
            resource = self.k8s_custom.get_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=self.plural,
                name=name
            )

            # Update status
            if 'status' not in resource:
                resource['status'] = {}

            resource['status']['phase'] = phase
            resource['status']['message'] = message
            resource['status']['lastTransitionTime'] = datetime.now(timezone.utc).isoformat()

            # Update the resource
            self.k8s_custom.patch_namespaced_custom_object_status(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=self.plural,
                name=name,
                body=resource
            )

            logger.info(f"Updated status for {namespace}/{name}: {phase} - {message}")

        except Exception as e:
            logger.error(f"Failed to update status for {namespace}/{name}: {e}")

    async def cleanup_resource(self, namespace: str, name: str, uid: str):
        """Clean up resources when IcebergConnect is deleted."""
        logger.info(f"Cleaning up resources for {namespace}/{name}")

        # Delete worker resources
        worker_labels = f"managed-by=iceberg-connect-operator"

        try:
            # Delete deployments
            deployments = self.k8s_apps.list_namespaced_deployment(
                namespace, label_selector=worker_labels
            )
            for deployment in deployments.items:
                self.k8s_apps.delete_namespaced_deployment(
                    deployment.metadata.name, namespace
                )
                logger.info(f"Deleted deployment {deployment.metadata.name}")

            # Delete services
            services = self.k8s_core.list_namespaced_service(
                namespace, label_selector=worker_labels
            )
            for service in services.items:
                self.k8s_core.delete_namespaced_service(
                    service.metadata.name, namespace
                )
                logger.info(f"Deleted service {service.metadata.name}")

            # Delete config maps
            config_maps = self.k8s_core.list_namespaced_config_map(
                namespace, label_selector=worker_labels
            )
            for config_map in config_maps.items:
                self.k8s_core.delete_namespaced_config_map(
                    config_map.metadata.name, namespace
                )
                logger.info(f"Deleted configmap {config_map.metadata.name}")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    async def run_command(self, cmd: list) -> subprocess.CompletedProcess:
        """Run a shell command asynchronously."""
        logger.info(f"Running command: {' '.join(cmd)}")

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        result = subprocess.CompletedProcess(
            cmd, process.returncode, stdout.decode(), stderr.decode()
        )

        if result.returncode != 0:
            logger.error(f"Command failed: {result.stderr}")
            raise RuntimeError(f"Command failed with exit code {result.returncode}: {result.stderr}")

        return result


async def main():
    """Main entry point."""
    controller = IcebergConnectController()
    await controller.run()


if __name__ == '__main__':
    asyncio.run(main())