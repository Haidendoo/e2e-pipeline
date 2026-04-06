from __future__ import annotations

from pyspark.sql import functions as F

from edge_device_common import build_spark_session, ensure_tables, resolve_config


def half_hour_window(column_name: str) -> F.Column:
    return F.expr(
        f"to_timestamp(from_unixtime(CAST(floor(unix_timestamp({column_name}) / 1800) * 1800 AS BIGINT)))"
    )


def main() -> None:
    config = resolve_config()
    spark = build_spark_session("edge_device_gold_job", config)
    ensure_tables(spark)

    silver_df = spark.table("local.edge_device.edge_device_silver")
    processed_batches = spark.table("local.edge_device.edge_device_gold").select("batch_id").distinct()
    pending_silver = silver_df.join(processed_batches, on="batch_id", how="left_anti")

    if pending_silver.limit(1).count() == 0:
        spark.stop()
        return

    gold_base_df = (
        pending_silver.filter(F.col("metric_value_num").isNotNull())
        .withColumn("source_name_nn", F.coalesce(F.col("source_name"), F.lit("unknown")))
        .withColumn("window_start_nn", F.coalesce(half_hour_window("event_time"), F.current_timestamp()))
    )

    gold_df = (
        gold_base_df.groupBy("window_start_nn", "source_name_nn", "metric_key", "batch_id")
        .agg(
            F.count(F.lit(1)).alias("sample_count"),
            F.coalesce(F.avg("metric_value_num"), F.lit(0.0)).alias("avg_value"),
            F.coalesce(F.min("metric_value_num"), F.lit(0.0)).alias("min_value"),
            F.coalesce(F.max("metric_value_num"), F.lit(0.0)).alias("max_value"),
        )
        .select(
            F.col("window_start_nn").alias("window_start"),
            F.col("source_name_nn").alias("source_name"),
            F.col("metric_key"),
            F.col("sample_count"),
            F.col("avg_value"),
            F.col("min_value"),
            F.col("max_value"),
            F.col("batch_id"),
        )
        .filter(F.col("metric_key").isNotNull())
        .withColumn("generated_at", F.current_timestamp())
    )

    gold_df.writeTo("local.edge_device.edge_device_gold").append()
    spark.stop()


if __name__ == "__main__":
    main()
