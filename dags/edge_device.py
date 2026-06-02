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


def _resolve_minio_from_airflow_connection() -> dict[str, str]:
	"""Resolve MinIO settings from the Airflow connection configured in the UI."""

	conn_id = os.getenv("EDGE_DEVICE_MINIO_AIRFLOW_CONN_ID", "minio_conn")
	try:
		conn = BaseHook.get_connection(conn_id)
	except Exception:
		conn = BaseHook.get_connection("aws_default")

	extra = conn.extra_dejson or {}
	endpoint = (
		extra.get("endpoint_url")
		or extra.get("host")
		or (f"http://{conn.host}:{conn.port}" if conn.host and conn.port else None)
		or (f"http://{conn.host}" if conn.host else None)
		or os.getenv("MINIO_ENDPOINT", "http://minio:9000")
	)

	return {
		"endpoint": endpoint,
		"access_key": conn.login or os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
		"secret_key": conn.password or os.getenv("MINIO_SECRET_KEY", "minioadmin"),
		"bucket": extra.get("bucket") or extra.get("bucket_name") or extra.get("minio_bucket") or os.getenv("MINIO_BUCKET", "test-bucket"),
	}


with DAG(
	dag_id="edge_device_medallion_30m",
	start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
	schedule="*/30 * * * *",
	catchup=False,
	default_args=DEFAULT_ARGS,
	tags=["kafka", "minio", "iceberg", "spark", "medallion", "edge-device"],
) as dag:
	minio_config = _resolve_minio_from_airflow_connection()
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
	common_conf = {
		"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
		"spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
		"spark.sql.catalog.local.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
		"spark.sql.catalog.local.uri": "jdbc:postgresql://postgres:5432/airflow",
		"spark.sql.catalog.local.jdbc.user": "airflow",
		"spark.sql.catalog.local.jdbc.password": "airflow",
		"spark.sql.catalog.local.warehouse": f"s3a://{minio_config['bucket']}/warehouse",
		"spark.sql.catalog.local.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
		"spark.sql.catalog.local.s3.endpoint": minio_config["endpoint"],
		"spark.sql.catalog.local.s3.access-key-id": minio_config["access_key"],
		"spark.sql.catalog.local.s3.secret-access-key": minio_config["secret_key"],
		"spark.hadoop.fs.s3a.endpoint": minio_config["endpoint"],
		"spark.hadoop.fs.s3a.access.key": minio_config["access_key"],
		"spark.hadoop.fs.s3a.secret.key": minio_config["secret_key"],
		"spark.hadoop.fs.s3a.path.style.access": "true",
		"spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
		"spark.sql.session.timeZone": "UTC",
	}
	common_env = {
		"EDGE_DEVICE_KAFKA_BOOTSTRAP": os.getenv("EDGE_DEVICE_KAFKA_BOOTSTRAP", "kafka:9092"),
		"EDGE_DEVICE_KAFKA_TOPIC": os.getenv("EDGE_DEVICE_KAFKA_TOPIC", "edge_computer_stats"),
		"EDGE_DEVICE_MINIO_ENDPOINT": minio_config["endpoint"],
		"EDGE_DEVICE_MINIO_ACCESS_KEY": minio_config["access_key"],
		"EDGE_DEVICE_MINIO_SECRET_KEY": minio_config["secret_key"],
		"EDGE_DEVICE_MINIO_BUCKET": minio_config["bucket"],
	}

	bronze_task = SparkSubmitOperator(
		task_id="edge_device_bronze",
		conn_id=os.getenv("EDGE_DEVICE_SPARK_CONN_ID", "spark_default"),
		application="/opt/spark_jobs/edge_device_bronze_job.py",
		packages=packages,
		conf=common_conf,
		env_vars=common_env,
	)
	silver_task = SparkSubmitOperator(
		task_id="edge_device_silver",
		conn_id=os.getenv("EDGE_DEVICE_SPARK_CONN_ID", "spark_default"),
		application="/opt/spark_jobs/edge_device_silver_job.py",
		packages=packages,
		conf=common_conf,
		env_vars=common_env,
	)
	gold_task = SparkSubmitOperator(
		task_id="edge_device_gold",
		conn_id=os.getenv("EDGE_DEVICE_SPARK_CONN_ID", "spark_default"),
		application="/opt/spark_jobs/edge_device_gold_job.py",
		packages=packages,
		conf=common_conf,
		env_vars=common_env,
	)

	trino_validate_gold = SQLExecuteQueryOperator(
		task_id="trino_validate_gold",
		conn_id="trino_default",
		sql="SELECT COUNT(*) FROM iceberg.edge_device.edge_device_gold;",
		show_return_value_in_logs=True,
	)

	bronze_task >> silver_task >> gold_task >> trino_validate_gold