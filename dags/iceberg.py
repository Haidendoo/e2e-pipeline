from __future__ import annotations

import logging
import os
from datetime import datetime

import pendulum
import pyarrow as pa
from airflow import DAG
from airflow.decorators import task
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType, TimestampType

DEFAULT_ARGS = {
	"owner": "airflow",
	"retries": 1,
}


def _get_minio_config() -> dict[str, str]:
	endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
	access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
	secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
	bucket = os.getenv("MINIO_BUCKET", "test-bucket")

	if endpoint and not endpoint.startswith(("http://", "https://")):
		endpoint = f"http://{endpoint}"

	if endpoint.startswith("http://localhost") or endpoint.startswith("http://127.0.0.1"):
		endpoint = "http://minio:9000"

	return {
		"endpoint": endpoint,
		"access_key": access_key,
		"secret_key": secret_key,
		"bucket": bucket,
	}


def _ensure_bucket_exists(endpoint: str, access_key: str, secret_key: str, bucket: str) -> None:
	import boto3
	from botocore.exceptions import ClientError

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


def _load_iceberg_catalog(endpoint: str, access_key: str, secret_key: str, bucket: str):
	catalog_uri = os.getenv("ICEBERG_CATALOG_URI", "sqlite:////opt/airflow/iceberg/iceberg_catalog.db")

	if catalog_uri.startswith("sqlite:////"):
		catalog_path = catalog_uri.replace("sqlite:////", "/", 1)
		os.makedirs(os.path.dirname(catalog_path), exist_ok=True)
	elif catalog_uri.startswith("sqlite:///"):
		catalog_path = catalog_uri.replace("sqlite:///", "", 1)
		os.makedirs(os.path.dirname(catalog_path), exist_ok=True)

	return load_catalog(
		"minio_catalog",
		type="sql",
		uri=catalog_uri,
		warehouse=f"s3://{bucket}/warehouse",
		**{
			"io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
			"s3.endpoint": endpoint,
			"s3.access-key-id": access_key,
			"s3.secret-access-key": secret_key,
			"s3.path-style-access": "true",
		},
	)


def _ensure_namespace(catalog, namespace: str) -> None:
	if namespace not in {ns[0] for ns in catalog.list_namespaces()}:
		catalog.create_namespace(namespace)


def _ensure_table(catalog, table_identifier: str):
	if catalog.table_exists(table_identifier):
		return catalog.load_table(table_identifier)

	schema = Schema(
		NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
		NestedField(field_id=2, name="name", field_type=StringType(), required=True),
		NestedField(field_id=3, name="event_time", field_type=TimestampType(), required=True),
	)

	return catalog.create_table(
		identifier=table_identifier,
		schema=schema,
	)


def _append_sample_rows(table) -> None:
	now = datetime.utcnow()
	arrow_schema = pa.schema(
		[
			pa.field("id", pa.int32(), nullable=False),
			pa.field("name", pa.string(), nullable=False),
			pa.field("event_time", pa.timestamp("us"), nullable=False),
		]
	)

	payload = pa.Table.from_pydict(
		{
			"id": [1, 2, 3],
			"name": ["alpha", "beta", "gamma"],
			"event_time": [now, now, now],
		},
		schema=arrow_schema,
	)

	try:
		table.append(payload)
	except AttributeError:
		raise RuntimeError(
			"The installed pyiceberg version does not support table.append(). "
			"Upgrade pyiceberg or adjust the write path to use explicit data file appends."
		)


with DAG(
	dag_id="iceberg_minio_sample_write",
	start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
	schedule=None,
	catchup=False,
	default_args=DEFAULT_ARGS,
	tags=["sample", "iceberg", "minio"],
) as dag:

	@task
	def write_sample_iceberg_table() -> str:
		config = _get_minio_config()
		logging.info("Using MinIO endpoint %s and bucket %s", config["endpoint"], config["bucket"])

		_ensure_bucket_exists(
			endpoint=config["endpoint"],
			access_key=config["access_key"],
			secret_key=config["secret_key"],
			bucket=config["bucket"],
		)

		catalog = _load_iceberg_catalog(
			endpoint=config["endpoint"],
			access_key=config["access_key"],
			secret_key=config["secret_key"],
			bucket=config["bucket"],
		)

		namespace = "demo"
		table_identifier = f"{namespace}.sample_iceberg_table"
		_ensure_namespace(catalog, namespace)
		table = _ensure_table(catalog, table_identifier)
		_append_sample_rows(table)

		return f"s3://{config['bucket']}/warehouse/{table_identifier.replace('.', '/')}"

	write_sample_iceberg_table()
