#!/bin/bash

# Airflow Connections Setup Script via CLI
# This script adds all connections to Airflow using the CLI

# Container name - adjust if your container has a different name
CONTAINER_NAME="e2e-pipeline-airflow-scheduler-1"  # Change this to match your actual container name

echo "Setting up Airflow connections..."
echo "Using container: $CONTAINER_NAME"
echo ""

# PostgreSQL Connection (default)
echo "Adding postgres_default connection..."
docker exec $CONTAINER_NAME airflow connections add 'postgres_default' \
  --conn-type 'postgres' \
  --conn-host 'postgres' \
  --conn-port '5432' \
  --conn-login 'airflow' \
  --conn-password 'airflow' \
  --conn-schema 'airflow'

# MinIO Connection
echo "Adding minio_conn connection..."
docker exec $CONTAINER_NAME airflow connections add 'minio_conn' \
  --conn-type 'aws' \
  --conn-login 'minioadmin' \
  --conn-password 'minioadmin' \
  --conn-host 'minio' \
  --conn-port '9000' \
  --conn-extra '{"endpoint_url": "http://minio:9000", "addressing_style": "path"}'

# MinIO Remote Connection
echo "Adding minio_remote connection..."
docker exec $CONTAINER_NAME airflow connections add 'minio_remote' \
  --conn-type 'aws' \
  --conn-login 'minioadmin' \
  --conn-password 'minioadmin' \
  --conn-extra '{"endpoint_url": "http://minio:9000", "addressing_style": "path"}'

# Spark Connection
echo "Adding spark_default connection..."
docker exec $CONTAINER_NAME airflow connections add 'spark_default' \
  --conn-type 'spark' \
  --conn-host 'spark-master' \
  --conn-port '7077' \
  --conn-schema 'spark' \
  --conn-extra '{"deploy_mode": "cluster", "spark_binary": "/opt/spark/bin/spark-submit"}'

# Spark Submit Connection
echo "Adding spark_submit connection..."
docker exec $CONTAINER_NAME airflow connections add 'spark_submit' \
  --conn-type 'spark' \
  --conn-host 'spark-master' \
  --conn-port '7077' \
  --conn-schema 'spark' \
  --conn-extra '{"spark_home": "/opt/spark", "master_url": "spark://spark-master:7077"}'

# Kafka Internal Connection
echo "Adding kafka_default connection..."
docker exec $CONTAINER_NAME airflow connections add 'kafka_default' \
  --conn-type 'kafka' \
  --conn-host 'kafka' \
  --conn-port '9092' \
  --conn-schema 'kafka' \
  --conn-extra '{"bootstrap_servers": "kafka:9092", "group_id": "airflow-group"}'

# Kafka External Connection
echo "Adding kafka_external connection..."
docker exec $CONTAINER_NAME airflow connections add 'kafka_external' \
  --conn-type 'kafka' \
  --conn-host 'localhost' \
  --conn-port '9094' \
  --conn-schema 'kafka' \
  --conn-extra '{"bootstrap_servers": "localhost:9094", "group_id": "airflow-group"}'

# HTTP Connection
echo "Adding http_default connection..."
docker exec $CONTAINER_NAME airflow connections add 'http_default' \
  --conn-type 'http' \
  --conn-host 'http://localhost'

# Trino Connection
echo "Adding trino_default connection..."
docker exec $CONTAINER_NAME airflow connections add 'trino_default' \
  --conn-type 'trino' \
  --conn-host 'trino' \
  --conn-port '8080' \
  --conn-login 'airflow'

echo ""
echo "All connections have been added!"
echo ""
echo "To verify the connections, run:"
echo "docker exec $CONTAINER_NAME airflow connections list"
