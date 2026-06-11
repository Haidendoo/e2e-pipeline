"""
EdgeX System Monitoring — Airflow DAG
=======================================
Orchestrate luồng dữ liệu từ Kafka → Spark Bronze/Silver/Gold → Trino validate.

Schedule: mỗi 5 phút (*/5 * * * *)
Source:   Kafka topic 'edgex_system_metrics' (dữ liệu từ EdgeX Foundry)
Sink:     Iceberg tables local.edgex.edgex_{bronze,silver,gold}

DAG Flow:
    edgex_bronze → edgex_silver → edgex_gold → trino_validate_gold

Tags: edgex, kafka, spark, iceberg, system-monitoring
"""

from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.hook import BaseHook

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
}


def _resolve_minio_config() -> dict[str, str]:
    """Resolve MinIO settings từ Airflow connection hoặc environment variables."""
    conn_id = os.getenv("EDGEX_MINIO_AIRFLOW_CONN_ID", "minio_conn")
    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception:
        conn = BaseHook.get_connection("aws_default") if os.getenv("AWS_DEFAULT_CONN") else None

    if conn:
        extra = conn.extra_dejson or {}
        endpoint = (
            extra.get("endpoint_url")
            or (f"http://{conn.host}:{conn.port}" if conn.host and conn.port else None)
            or os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        )
        return {
            "endpoint": endpoint,
            "access_key": conn.login or os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "secret_key": conn.password or os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "bucket": extra.get("bucket_name") or os.getenv("MINIO_BUCKET", "test-bucket"),
        }

    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "bucket": os.getenv("MINIO_BUCKET", "test-bucket"),
    }


with DAG(
    dag_id="edgex_system_monitoring_5m",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    description=(
        "EdgeX System Monitoring: Kafka → Spark Bronze/Silver/Gold → Iceberg → Trino. "
        "Nguồn dữ liệu: EdgeX Foundry device RPi4-REST-v2 (25 metrics)."
    ),
    tags=["edgex", "kafka", "spark", "iceberg", "medallion", "system-monitoring"],
) as dag:

    minio = _resolve_minio_config()

    # ── Spark packages (Iceberg + Kafka + S3A + PostgreSQL JDBC) ─────────────────
    packages = ",".join(
        [
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            "org.apache.commons:commons-pool2:2.11.1",
            "org.postgresql:postgresql:42.6.0",
        ]
    )

    # ── Iceberg + S3A Spark config ────────────────────────────────────────────────
    common_conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        "spark.sql.catalog.local.uri": "jdbc:postgresql://postgres:5432/airflow",
        "spark.sql.catalog.local.jdbc.user": "airflow",
        "spark.sql.catalog.local.jdbc.password": "airflow",
        "spark.sql.catalog.local.warehouse": f"s3a://{minio['bucket']}/warehouse",
        "spark.sql.catalog.local.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
        "spark.sql.catalog.local.s3.endpoint": minio["endpoint"],
        "spark.sql.catalog.local.s3.access-key-id": minio["access_key"],
        "spark.sql.catalog.local.s3.secret-access-key": minio["secret_key"],
        "spark.hadoop.fs.s3a.endpoint": minio["endpoint"],
        "spark.hadoop.fs.s3a.access.key": minio["access_key"],
        "spark.hadoop.fs.s3a.secret.key": minio["secret_key"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
    }

    # ── Runtime env vars passed into Spark jobs ───────────────────────────────────
    common_env = {
        "EDGEX_KAFKA_BOOTSTRAP": os.getenv("EDGEX_KAFKA_BOOTSTRAP", "kafka:9092"),
        "EDGEX_KAFKA_TOPIC": os.getenv("EDGEX_KAFKA_TOPIC", "edgex_system_metrics"),
        "EDGEX_MINIO_ENDPOINT": minio["endpoint"],
        "EDGEX_MINIO_ACCESS_KEY": minio["access_key"],
        "EDGEX_MINIO_SECRET_KEY": minio["secret_key"],
        "EDGEX_MINIO_BUCKET": minio["bucket"],
    }

    spark_conn = os.getenv("EDGEX_SPARK_CONN_ID", "spark_default")

    # ── Task 1: Bronze ────────────────────────────────────────────────────────────
    bronze_task = SparkSubmitOperator(
        task_id="edgex_bronze",
        conn_id=spark_conn,
        application="/opt/spark_jobs/edgex_bronze_job.py",
        packages=packages,
        conf=common_conf,
        env_vars=common_env,
        py_files="/opt/spark_jobs/edgex_common.py",
    )

    # ── Task 2: Silver ────────────────────────────────────────────────────────────
    silver_task = SparkSubmitOperator(
        task_id="edgex_silver",
        conn_id=spark_conn,
        application="/opt/spark_jobs/edgex_silver_job.py",
        packages=packages,
        conf=common_conf,
        env_vars=common_env,
        py_files="/opt/spark_jobs/edgex_common.py",
    )

    # ── Task 3: Gold ─────────────────────────────────────────────────────────────
    gold_task = SparkSubmitOperator(
        task_id="edgex_gold",
        conn_id=spark_conn,
        application="/opt/spark_jobs/edgex_gold_job.py",
        packages=packages,
        conf=common_conf,
        env_vars=common_env,
        py_files="/opt/spark_jobs/edgex_common.py",
    )

    # ── Task 4: Trino validate ───────────────────────────────────────────────────
    trino_validate = SQLExecuteQueryOperator(
        task_id="trino_validate_gold",
        conn_id="trino_default",
        sql="""
            SELECT
                device_name,
                resource_name,
                COUNT(*) AS total_windows,
                AVG(avg_value) AS overall_avg,
                MAX(generated_at) AS last_updated
            FROM iceberg.edgex.edgex_gold
            GROUP BY device_name, resource_name
            ORDER BY resource_name
            LIMIT 50
        """,
        show_return_value_in_logs=True,
    )

    # ── DAG Dependencies ─────────────────────────────────────────────────────────
    bronze_task >> silver_task >> gold_task >> trino_validate
