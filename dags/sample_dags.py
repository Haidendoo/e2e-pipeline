from __future__ import annotations

import pendulum
import pytest
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="sample_daily_hello",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["sample", "hello"],
) as dag_hello:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end


with DAG(
    dag_id="sample_taskflow_demo",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["sample", "taskflow"],
) as dag_taskflow:

    @task
    def generate_message() -> str:
        return "hello from taskflow"

    @task
    def print_message(message: str) -> None:
        print(message)

    print_message(generate_message())
