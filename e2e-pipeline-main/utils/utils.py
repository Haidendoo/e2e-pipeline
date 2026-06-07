from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from urllib.parse import quote_plus, urlparse
import os


def _normalize_endpoint(endpoint: str) -> str:
    if endpoint and not endpoint.startswith(("http://", "https://")):
        endpoint = f"http://{endpoint}"

    if endpoint.startswith("http://localhost") or endpoint.startswith("http://127.0.0.1"):
        endpoint = "http://minio:9000"

    return endpoint


def _get_minio_config() -> dict[str, str]:
    endpoint = _normalize_endpoint(os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket = os.getenv("MINIO_BUCKET", "test-bucket")

    return {
        "endpoint": endpoint,
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket": bucket,
    }


def _build_airflow_aws_conn_uri(endpoint: str, access_key: str, secret_key: str) -> str:
    encoded_endpoint = quote_plus(endpoint, safe="")
    encoded_access_key = quote_plus(access_key, safe="")
    encoded_secret_key = quote_plus(secret_key, safe="")

    return (
        f"aws://{encoded_access_key}:{encoded_secret_key}@/?"
        f"endpoint_url={encoded_endpoint}&region_name=us-east-1&addressing_style=path"
    )


def _configure_minio_env(endpoint: str, access_key: str, secret_key: str) -> None:
    os.environ.update(
        {
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_S3_ENDPOINT_URL": endpoint,
            "AIRFLOW_CONN_AWS_DEFAULT": _build_airflow_aws_conn_uri(endpoint, access_key, secret_key),
        }
    )


def _set_airflow_http_connection(conn_id: str, conn_uri: str) -> None:
    os.environ[f"AIRFLOW_CONN_{conn_id.upper()}"] = conn_uri

def create_new_dag(dag_id, provider, url, database_name, table_list, dag, **kwargs):
    
    # Get MinIO configuration using the same method as iceberg.py
    config = _get_minio_config()
    minio_endpoint = config["endpoint"]
    minio_access_key = config["access_key"]
    minio_secret_key = config["secret_key"]
    minio_bucket = config["bucket"]
    
    _configure_minio_env(minio_endpoint, minio_access_key, minio_secret_key)
    
    # Spark connection is configured in Airflow UI or via connections
    # Connection ID: spark_default, Type: Spark, Host: spark-master, Port: 7077
    
    file_name = url.split('/')[-1]
    s3_key = f"raw/{dag_id}/{file_name}"
    
    def ensure_bucket_exists(**context):
        """Ensure MinIO bucket exists before upload"""
        import boto3
        from botocore.exceptions import ClientError
        import logging

        logger = logging.getLogger(__name__)
        logger.info(f"Creating bucket if not exists: {minio_bucket} at {minio_endpoint}")
        
        client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name="us-east-1",
        )

        try:
            client.head_bucket(Bucket=minio_bucket)
            logger.info(f"Bucket {minio_bucket} already exists")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"Bucket {minio_bucket} does not exist, creating it")
                client.create_bucket(Bucket=minio_bucket)
                logger.info(f"Bucket {minio_bucket} created successfully")
            else:
                logger.error(f"Error checking bucket: {e}")
                raise
    
    with dag:
        # Create task to ensure bucket exists first
        ensure_bucket_task = PythonOperator(
            task_id="ensure_bucket",
            python_callable=ensure_bucket_exists,
            dag=dag
        )
        
        # Chọn Operator dựa trên storageProvider
        if provider == "HTTPS":
            # Parse the URL to extract host and path for HttpToS3Operator
            parsed_url = urlparse(url)
            # Reconstruct the connection string: scheme://host:port
            if parsed_url.port:
                http_conn_uri = f"{parsed_url.scheme}://{parsed_url.hostname}:{parsed_url.port}"
            else:
                http_conn_uri = f"{parsed_url.scheme}://{parsed_url.hostname}"
            
            # Create a custom HTTP connection ID for this source
            http_conn_id = f"http_{dag_id}"
            _set_airflow_http_connection(http_conn_id, http_conn_uri)
            
            # The endpoint is just the path
            endpoint = parsed_url.path
            if parsed_url.query:
                endpoint += f"?{parsed_url.query}"
            
            # HttpToS3Operator: Download từ HTTPS và upload trực tiếp vào S3/MinIO
            step_1 = HttpToS3Operator(
                task_id="http_to_s3",
                http_conn_id=http_conn_id,
                endpoint=endpoint,
                aws_conn_id="aws_default",
                s3_bucket=minio_bucket,
                s3_key=s3_key,
                replace=True,
                dag=dag
            )
            
            # SparkSubmitOperator: Simple read from MinIO to verify the data was uploaded
            step_2 = SparkSubmitOperator(
                task_id="submit_spark_job",
                application="/opt/spark_jobs/test_job.py",
                dag=dag
            )
            
            ensure_bucket_task >> step_1 >> step_2
            
        elif provider == "FTP":
            # FTP: Dùng SFTP operator kết hợp S3 upload
            step_1 = SFTPToS3Operator(
                task_id="sftp_to_s3",
                sftp_conn_id="sftp_default",
                sftp_path=url,
                aws_conn_id="aws_default",
                s3_bucket=minio_bucket,
                s3_key=s3_key,
                dag=dag
            )
            ensure_bucket_task >> step_1
            
        elif provider == "SPARK":
            step_1 = SparkSubmitOperator(
                task_id="spark_ingest",
                application="/opt/spark_jobs/ingest.py",
                dag=dag
            )
        else:
            from airflow.operators.empty import EmptyOperator
            step_1 = EmptyOperator(task_id="provider_not_supported", dag=dag)

        step_1
    return dag