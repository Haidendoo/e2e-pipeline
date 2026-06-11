from datetime import datetime

from airflow import DAG

from utils.utils import create_new_dag


# Cấu hình mẫu để tạo DAG dùng chung utils và load dữ liệu raw lên MinIO
configs = {
    "dag_sales_data": {
        "schedule": "@daily",
        "provider": "HTTPS",
        "url": "https://gitlab.com/hoaiyenngo/vntt/-/raw/main/data_final1reco.json",
        "database_name": "analytics",
        "table_list": ["sales"],
    },
    "dag_user_data": {
        "schedule": "@hourly",
        "provider": "HTTPS",
        "url": "https://raw.githubusercontent.com/plotly/datasets/master/2014_apple_stock.csv",
        "database_name": "analytics",
        "table_list": ["users"],
    },
    "dag_user_dta": {
        "schedule": "@hourly",
        "provider": "HTTPS",
        "url": "https://raw.githubusercontent.com/plotly/datasets/master/2014_apple_stock.csv",
        "database_name": "analytics",
        "table_list": ["users"],
    },
}


for dag_id, params in configs.items():
    dag = DAG(
        dag_id=dag_id,
        schedule=params.get("schedule"),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=[params.get("provider", "HTTPS"), "master-trigger"],
    )

    globals()[dag_id] = create_new_dag(
        dag_id=dag_id,
        provider=params["provider"],
        url=params["url"],
        database_name=params["database_name"],
        table_list=params["table_list"],
        dag=dag,
    )