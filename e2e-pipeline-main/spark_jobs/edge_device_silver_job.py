from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from edge_device_common import build_spark_session, ensure_tables, resolve_config


def main() -> None:
    config = resolve_config()
    spark = build_spark_session("edge_device_silver_job", config)
    ensure_tables(spark)

    bronze_df = spark.table("local.edge_device.edge_device_bronze")
    processed_batches = spark.table("local.edge_device.edge_device_silver").select("batch_id").distinct()
    pending_bronze = bronze_df.join(processed_batches, on="batch_id", how="left_anti")

    if pending_bronze.limit(1).count() == 0:
        spark.stop()
        return

    outer_map = F.from_json(F.col("raw_value"), MapType(StringType(), StringType(), True))
    inner_json = F.element_at(outer_map, F.col("source_name"))
    inner_map = F.from_json(inner_json, MapType(StringType(), StringType(), True))

    silver_df = (
        pending_bronze.withColumn("inner_map", inner_map)
        .withColumn("metric", F.explode(F.map_entries(F.col("inner_map"))))
        .select(
            "event_id",
            "device_name",
            "profile_name",
            "source_name",
            F.col("event_origin").alias("event_time"),
            F.col("metric.key").alias("metric_key"),
            F.col("metric.value").alias("metric_value"),
            F.col("metric.value").cast("double").alias("metric_value_num"),
            "batch_id",
            F.current_timestamp().alias("processed_at"),
        )
        .filter(F.col("metric_key").isNotNull())
    )

    silver_df.writeTo("local.edge_device.edge_device_silver").append()
    spark.stop()


if __name__ == "__main__":
    main()
