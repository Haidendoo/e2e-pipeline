"""
Simple Spark Test DAG
Tests Spark cluster connectivity and basic job submission
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import subprocess
import pandas as pd
import great_expectations as gx

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id="spark_test_dag",
    default_args=default_args,
    description="Simple DAG to test Spark cluster",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "test"],
)

# Python task to print info
def print_spark_info():
    import socket
    print("=" * 50)
    print("SPARK CLUSTER TEST")
    print("=" * 50)
    print(f"Hostname: {socket.gethostname()}")
    print(f"Timestamp: {datetime.now()}")
    print("=" * 50)

# Submit Spark job directly
def submit_spark_job():
    cmd = [
        "spark-submit",
        "--master", "local[2]",  # Local mode with 2 threads - no distributed Python serialization
        "/opt/spark_jobs/test_job.py"
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print("STDOUT:")
    print(result.stdout)
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    if result.returncode != 0:
        raise Exception(f"Spark job failed with return code {result.returncode}")
    print("Spark job completed successfully!")

# Submit Spark job to write Iceberg table to MinIO
def write_iceberg_to_minio():
    cmd = [
        "spark-submit",
        "--master", "local[2]",
        "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4",
        "/opt/spark_jobs/iceberg_write_job.py"
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print("STDOUT:")
    print(result.stdout)
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    if result.returncode != 0:
        raise Exception(f"Iceberg write job failed with return code {result.returncode}")
    print("Iceberg write job completed successfully!")

def _get_validator_from_df(df, suite_name):
    context = gx.get_context(mode="ephemeral")
    datasource = context.sources.add_pandas(name="pandas")
    data_asset = datasource.add_dataframe_asset(name="df_asset")
    batch_request = data_asset.build_batch_request(dataframe=df)
    existing_suites = {suite.expectation_suite_name for suite in context.list_expectation_suites()}
    if suite_name not in existing_suites:
        context.add_expectation_suite(expectation_suite_name=suite_name)
    return context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# Great Expectations validation (pass)
def ge_validation_pass():
    df = pd.DataFrame(
        {
            "event_id": [1, 2, 3],
            "amount": [10.0, 20.5, 30.0],
        }
    )
    validator = _get_validator_from_df(df, "spark_test_pass")
    validator.expect_column_values_to_not_be_null("event_id")
    validator.expect_column_values_to_be_between("amount", min_value=0, max_value=100)
    result = validator.validate()
    if not result["success"]:
        raise Exception("Great Expectations pass check failed")
    print("Great Expectations pass check succeeded")

# Great Expectations validation (fail)
def ge_validation_fail():
    df = pd.DataFrame(
        {
            "event_id": [1, None, 3],
            "amount": [10.0, -5.0, 30.0],
        }
    )
    validator = _get_validator_from_df(df, "spark_test_fail")
    validator.expect_column_values_to_not_be_null("event_id")
    validator.expect_column_values_to_be_between("amount", min_value=0, max_value=100)
    result = validator.validate()
    if not result["success"]:
        raise Exception("Great Expectations fail check triggered as expected")
    print("Great Expectations fail check unexpectedly succeeded")

# Task 1: Print environment info
print_info = PythonOperator(
    task_id="print_spark_info",
    python_callable=print_spark_info,
    dag=dag,
)

# Task 2: Submit a simple Spark job
submit_spark_job = PythonOperator(
    task_id="submit_spark_job",
    python_callable=submit_spark_job,
    dag=dag,
)

# Task 3: Write Iceberg table to MinIO
write_iceberg_task = PythonOperator(
    task_id="write_iceberg_to_minio",
    python_callable=write_iceberg_to_minio,
    dag=dag,
)

ge_pass_task = PythonOperator(
    task_id="ge_validation_pass",
    python_callable=ge_validation_pass,
    dag=dag,
)

ge_fail_task = PythonOperator(
    task_id="ge_validation_fail",
    python_callable=ge_validation_fail,
    dag=dag,
)

# Task dependency
print_info >> submit_spark_job >> write_iceberg_task
write_iceberg_task >> ge_pass_task
write_iceberg_task >> ge_fail_task
