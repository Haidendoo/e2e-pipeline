"""
EdgeX Gold Job — Silver → Iceberg Gold
========================================
Aggregate Silver readings theo 5-minute time windows,
nhóm theo device_name và resource_name.

Output Gold schema:
  window_start    — bắt đầu window 5 phút
  device_name     — tên thiết bị (RPi4-REST-v2)
  profile_name    — tên profile
  resource_name   — tên metric (CPUUsage, MemUsed, ...)
  sample_count    — số readings trong window
  avg_value       — giá trị trung bình
  min_value       — giá trị nhỏ nhất
  max_value       — giá trị lớn nhất
  batch_id        — batch reference
  generated_at    — thời điểm tạo

Incremental: chỉ xử lý Silver batch_id chưa có trong Gold.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, DoubleType

from edgex_common import build_spark_session, ensure_tables, resolve_config


def five_min_window(column_name: str) -> F.Column:
    """Tạo 5-minute window bucket từ timestamp column."""
    return F.expr(
        f"to_timestamp(from_unixtime(CAST(floor(unix_timestamp({column_name}) / 300) * 300 AS BIGINT)))"
    )


def main() -> None:
    config = resolve_config()
    spark = build_spark_session("edgex_gold_job", config)
    ensure_tables(spark)

    silver_df = spark.table("local.edgex.edgex_silver")
    processed_batches = (
        spark.table("local.edgex.edgex_gold").select("batch_id").distinct()
    )
    pending_silver = silver_df.join(processed_batches, on="batch_id", how="left_anti")

    if pending_silver.limit(1).count() == 0:
        print("[Gold] Không có batch mới để xử lý.")
        spark.stop()
        return

    NUMERIC_TYPES = {"Float32", "Float64", "Int16", "Int32", "Int64", "Uint8", "Uint16", "Uint32", "Uint64"}

    gold_df = (
        pending_silver
        .withColumn(
            "parsed_map",
            F.when(
                F.col("value_type").isin(*NUMERIC_TYPES),
                F.create_map(F.lit("value"), F.col("metric_value").cast("double"))
            ).otherwise(
                F.from_json(F.col("metric_value"), MapType(StringType(), DoubleType()))
            )
        )
        .select(
            "*", F.explode_outer("parsed_map").alias("metric_key", "metric_val")
        )
        .filter(F.col("metric_val").isNotNull())
        .withColumn("device_name_nn", F.coalesce(F.col("device_name"), F.lit("unknown")))
        .withColumn("profile_name_nn", F.coalesce(F.col("profile_name"), F.lit("unknown")))
        .withColumn(
            "resource_name_nn", 
            F.when(
                F.col("metric_key") == "value", 
                F.coalesce(F.col("resource_name"), F.lit("unknown"))
            ).otherwise(
                F.concat_ws("_", F.coalesce(F.col("resource_name"), F.lit("unknown")), F.col("metric_key"))
            )
        )
        .withColumn("window_start_nn", F.coalesce(five_min_window("event_time"), F.current_timestamp()))
        .groupBy(
            "window_start_nn",
            "device_name_nn",
            "profile_name_nn",
            "resource_name_nn",
            "batch_id",
        )
        .agg(
            F.count(F.lit(1)).alias("sample_count"),
            F.coalesce(F.avg("metric_val"), F.lit(0.0)).alias("avg_value"),
            F.coalesce(F.min("metric_val"), F.lit(0.0)).alias("min_value"),
            F.coalesce(F.max("metric_val"), F.lit(0.0)).alias("max_value"),
        )
        .select(
            F.col("window_start_nn").alias("window_start"),
            F.col("device_name_nn").alias("device_name"),
            F.col("profile_name_nn").alias("profile_name"),
            F.col("resource_name_nn").alias("resource_name"),
            F.col("sample_count"),
            F.col("avg_value"),
            F.col("min_value"),
            F.col("max_value"),
            F.col("batch_id"),
        )
        .withColumn("generated_at", F.current_timestamp())
    )

    gold_df.writeTo("local.edgex.edgex_gold").append()
    count = gold_df.count()
    print(f"[Gold] Đã ghi {count} aggregated rows vào edgex_gold.")

    # DUMP DEBUG FILE JSON
    print("[Gold] Đang xuất file JSON để debug...")
    try:
        gold_dump = (
            spark.table("local.edgex.edgex_gold")
            .orderBy(F.col("window_start").desc())
            .limit(100)
            .withColumn("window_start", F.col("window_start").cast("string"))
            .withColumn("generated_at", F.col("generated_at").cast("string"))
        )
        pandas_df = gold_dump.toPandas()
                
        import os
        os.makedirs("/opt/spark_jobs/logs", exist_ok=True)
        pandas_df.to_json("/opt/spark_jobs/logs/edgex_gold_debug_output.json", orient="records", indent=2)
        print("[Gold] Đã xuất file JSON tại: /opt/spark_jobs/logs/edgex_gold_debug_output.json")
    except Exception as e:
        print(f"[Gold] Lỗi khi xuất JSON: {e}")

    spark.stop()



if __name__ == "__main__":
    main()
