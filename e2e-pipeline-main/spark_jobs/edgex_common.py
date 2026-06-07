"""
EdgeX Common — Shared Spark & Config Utilities
===============================================
Dùng chung cho tất cả EdgeX Spark jobs (bronze, silver, gold).
Tương tự edge_device_common.py nhưng dành riêng cho luồng EdgeX.

Kafka topic: edgex_system_metrics
Schema: EdgeX v3 Event format với array readings (25 resources)
"""

from __future__ import annotations

import os

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession


def normalize_endpoint(endpoint: str) -> str:
    if endpoint and not endpoint.startswith(("http://", "https://")):
        endpoint = f"http://{endpoint}"
    if endpoint.startswith("http://localhost") or endpoint.startswith("http://127.0.0.1"):
        return "http://minio:9000"
    return endpoint


def resolve_config() -> dict[str, str]:
    endpoint = normalize_endpoint(
        os.getenv("EDGEX_MINIO_ENDPOINT", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    )
    return {
        "endpoint": endpoint,
        "access_key": os.getenv("EDGEX_MINIO_ACCESS_KEY", os.getenv("MINIO_ACCESS_KEY", "minioadmin")),
        "secret_key": os.getenv("EDGEX_MINIO_SECRET_KEY", os.getenv("MINIO_SECRET_KEY", "minioadmin")),
        "bucket": os.getenv("EDGEX_MINIO_BUCKET", os.getenv("MINIO_BUCKET", "test-bucket")),
        "bootstrap_servers": os.getenv("EDGEX_KAFKA_BOOTSTRAP", "kafka:9092"),
        "topic": os.getenv("EDGEX_KAFKA_TOPIC", "edgex_system_metrics"),
        "checkpoint_location": os.getenv(
            "EDGEX_CHECKPOINT_LOCATION",
            f"s3a://{os.getenv('EDGEX_MINIO_BUCKET', os.getenv('MINIO_BUCKET', 'test-bucket'))}/checkpoints/edgex_system_metrics_v2",
        ),
    }


def ensure_bucket_exists(endpoint: str, access_key: str, secret_key: str, bucket: str) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)


def build_spark_session(app_name: str, config: dict[str, str]) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.local.uri", "jdbc:postgresql://postgres:5432/airflow")
        .config("spark.sql.catalog.local.jdbc.user", "airflow")
        .config("spark.sql.catalog.local.jdbc.password", "airflow")
        .config("spark.sql.catalog.local.warehouse", f"s3a://{config['bucket']}/warehouse")
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.catalog.local.s3.endpoint", config["endpoint"])
        .config("spark.sql.catalog.local.s3.access-key-id", config["access_key"])
        .config("spark.sql.catalog.local.s3.secret-access-key", config["secret_key"])
        .config("spark.hadoop.fs.s3a.endpoint", config["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", config["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", config["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession) -> None:
    """Tạo Iceberg namespace và tables cho EdgeX pipeline nếu chưa có."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.edgex")

    # Bronze: mỗi row là 1 reading từ EdgeX event
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.edgex.edgex_bronze (
            event_id        STRING NOT NULL,
            api_version     STRING,
            device_name     STRING,
            profile_name    STRING,
            source_name     STRING,
            event_origin    TIMESTAMP,
            resource_name   STRING,
            value_type      STRING,
            raw_value       STRING,
            reading_origin  TIMESTAMP,
            kafka_partition INT,
            kafka_offset    BIGINT,
            batch_id        STRING NOT NULL,
            ingested_at     TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (days(event_origin), device_name)
        """
    )

    # Silver: normalized, cast sang đúng kiểu số
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.edgex.edgex_silver (
            event_id        STRING NOT NULL,
            device_name     STRING,
            profile_name    STRING,
            resource_name   STRING,
            event_time      TIMESTAMP,
            metric_value    STRING,
            metric_value_num DOUBLE,
            value_type      STRING,
            batch_id        STRING NOT NULL,
            processed_at    TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (days(event_time), device_name)
        """
    )

    # Gold: aggregated per 5-min window / device / resource
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.edgex.edgex_gold (
            window_start    TIMESTAMP NOT NULL,
            device_name     STRING NOT NULL,
            profile_name    STRING NOT NULL,
            resource_name   STRING NOT NULL,
            sample_count    BIGINT NOT NULL,
            avg_value       DOUBLE NOT NULL,
            min_value       DOUBLE NOT NULL,
            max_value       DOUBLE NOT NULL,
            batch_id        STRING NOT NULL,
            generated_at    TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (days(window_start), device_name)
        """
    )
