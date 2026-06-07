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

    # Normalize: cast value sang số nếu valueType là numeric
    silver_df = (
        pending_bronze.select(
            F.col("event_id"),
            F.col("device_name"),
            F.col("profile_name"),
            F.col("resource_name"),
            # Dùng reading_origin làm event_time chính
            F.coalesce(F.col("reading_origin"), F.col("event_origin")).alias("event_time"),
            F.col("raw_value").alias("metric_value"),
            # Cast sang DOUBLE cho numeric types
            F.when(
                F.col("value_type").isin(*NUMERIC_TYPES),
                F.col("raw_value").cast("double"),
            )
            .otherwise(F.lit(None).cast("double"))
            .alias("metric_value_num"),
            F.col("value_type"),
            F.col("batch_id"),
        )
        .filter(F.col("resource_name").isNotNull())
        .filter(F.col("metric_value").isNotNull())
        .withColumn("processed_at", F.current_timestamp())
    )

    silver_df.writeTo("local.edgex.edgex_silver").append()
    count = silver_df.count()
    print(f"[Silver] Đã ghi {count} rows vào edgex_silver.")
    spark.stop()


if __name__ == "__main__":
    main()
