"""
EdgeX Silver Job — Bronze → Iceberg Silver
===========================================
Đọc Bronze table, normalize values theo value_type,
cast sang numeric (metric_value_num), ghi vào Silver.

ValueType mapping (theo EdgeX Device Profile):
  Float32  → cast DOUBLE
  Uint64   → cast DOUBLE (đủ chính xác cho counter)
  String   → giữ nguyên, metric_value_num = NULL

Incremental: chỉ xử lý Bronze batch_id chưa có trong Silver.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from edgex_common import build_spark_session, ensure_tables, resolve_config

# Mapping valueType → cast strategy
NUMERIC_TYPES = {"Float32", "Float64", "Int16", "Int32", "Int64", "Uint8", "Uint16", "Uint32", "Uint64"}


def main() -> None:
    config = resolve_config()
    spark = build_spark_session("edgex_silver_job", config)
    ensure_tables(spark)

    bronze_df = spark.table("local.edgex.edgex_bronze")
    processed_batches = (
        spark.table("local.edgex.edgex_silver").select("batch_id").distinct()
    )
    pending_bronze = bronze_df.join(processed_batches, on="batch_id", how="left_anti")

    if pending_bronze.limit(1).count() == 0:
        print("[Silver] Không có batch mới để xử lý.")
        spark.stop()
        return

    # 1. Normalize: Dữ liệu EdgeX thông thường (bao gồm cả Telegraf-Node-01)
    silver_df = (
        pending_bronze.select(
            F.col("event_id"),
            F.col("device_name"),
            F.col("profile_name"),
            F.col("resource_name"),
            F.coalesce(F.col("reading_origin"), F.col("event_origin")).alias("event_time"),
            F.col("raw_value").alias("metric_value"),
            F.when(
                F.col("value_type").isin(*NUMERIC_TYPES),
                F.col("raw_value").cast("double"),
            )
            .when(F.col("resource_name") == "cpu_data", F.get_json_object(F.col("raw_value"), "$.usage_user").cast("double"))
            .when(F.col("resource_name") == "mem_data", F.get_json_object(F.col("raw_value"), "$.used").cast("double"))
            .when(F.col("resource_name") == "sys_data", F.get_json_object(F.col("raw_value"), "$.load1").cast("double"))
            .when(F.col("resource_name") == "proc_data", F.get_json_object(F.col("raw_value"), "$.running").cast("double"))
            .when(F.col("resource_name") == "disk_data", F.get_json_object(F.col("raw_value"), "$.read_bytes").cast("double"))
            .when(F.col("resource_name") == "net_data", F.coalesce(F.get_json_object(F.col("raw_value"), "$.bytes_recv"), F.get_json_object(F.col("raw_value"), "$.ip_inreceives")).cast("double"))
            .when(F.col("resource_name") == "gpu_data", F.get_json_object(F.col("raw_value"), "$.utilization_gpu").cast("double"))
            .when(F.col("resource_name") == "gpu_process_metrics", F.get_json_object(F.col("raw_value"), "$.sm_util").cast("double"))
            .otherwise(F.lit(None).cast("double"))
            .alias("metric_value_num"),
            F.col("value_type"),
            F.col("batch_id"),
        )
        .filter(F.col("resource_name").isNotNull())
        .filter(F.col("metric_value").isNotNull())
        .withColumn("processed_at", F.current_timestamp())
    )

    # [DEBUG] Hiển thị tiến trình xử lý lên màn hình console (Terminal)
    print("\n[Silver] Bắt đầu xử lý dữ liệu. Một số dòng dữ liệu mẫu:")
    silver_df.show(20, truncate=False)

    # Ghi vào Iceberg Silver Table
    silver_df.writeTo("local.edgex.edgex_silver").append()

    # [DEBUG] Lưu một bảng JSON bằng Python thuần để tránh lỗi Permission của Spark trên WSL
    import json
    import os
    os.makedirs("/opt/spark_jobs/logs", exist_ok=True)
    debug_json_path = "/opt/spark_jobs/logs/edgex_silver_debug_output.json"
    print(f"[Silver] Lưu dữ liệu thô ra file JSON tại {debug_json_path} để kiểm chứng...")
    
    final_table = spark.table("local.edgex.edgex_silver")
    local_data_telegraf = [row.asDict() for row in final_table.filter(F.col("device_name") == "Telegraf-Node-01").orderBy(F.col("event_time").desc()).limit(200).collect()]
    local_data = local_data_telegraf if local_data_telegraf else [row.asDict() for row in final_table.orderBy(F.col("event_time").desc()).limit(100).collect()]
    with open(debug_json_path, "w", encoding="utf-8") as f:
        json.dump(local_data, f, indent=2, default=str)

    count = silver_df.count()
    print(f"[Silver] Đã ghi {count} rows vào edgex_silver.\n")
    spark.stop()


if __name__ == "__main__":
    main()
