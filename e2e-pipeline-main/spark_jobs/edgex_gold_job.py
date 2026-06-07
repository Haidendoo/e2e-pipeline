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

    gold_df = (
        pending_silver
        .filter(F.col("metric_value_num").isNotNull())
        .withColumn("device_name_nn", F.coalesce(F.col("device_name"), F.lit("unknown")))
        .withColumn("profile_name_nn", F.coalesce(F.col("profile_name"), F.lit("unknown")))
        .withColumn("resource_name_nn", F.coalesce(F.col("resource_name"), F.lit("unknown")))
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
            F.coalesce(F.avg("metric_value_num"), F.lit(0.0)).alias("avg_value"),
            F.coalesce(F.min("metric_value_num"), F.lit(0.0)).alias("min_value"),
            F.coalesce(F.max("metric_value_num"), F.lit(0.0)).alias("max_value"),
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
    spark.stop()


if __name__ == "__main__":
    main()
