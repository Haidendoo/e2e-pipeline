"""
Simple Spark Test DAG
Tests Spark cluster connectivity and basic job submission
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

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

# Task dependency
print_info >> submit_spark_job
