"""
EdgeX Bronze Job — Kafka → Iceberg Bronze
==========================================
Đọc EdgeX Event JSON từ Kafka topic 'edgex_system_metrics'
và ghi vào Iceberg table local.edgex.edgex_bronze.

Schema EdgeX v3 Event:
{
  "apiVersion": "v3",
  "id": "<uuid>",
  "deviceName": "RPi4-REST-v2",
  "profileName": "RPi-REST-Profile-v2",
  "sourceName": "ReadAll",
  "origin": <nanoseconds>,
  "readings": [
    {
      "origin": <nanoseconds>,
      "deviceName": "RPi4-REST-v2",
      "resourceName": "CPUUsage",
      "profileName": "RPi-REST-Profile-v2",
      "valueType": "Float32",
      "value": "45.2"
    },
    ... (25 readings tổng)
  ]
}

Output: 1 row per reading (explode readings array)
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from edgex_common import (
    build_spark_session,
    ensure_bucket_exists,
    ensure_tables,
    resolve_config,
)


def edgex_event_schema() -> StructType:
    """Schema cho EdgeX v3 Event envelope."""
    reading_schema = StructType(
        [
            StructField("origin", LongType(), True),
            StructField("deviceName", StringType(), True),
            StructField("resourceName", StringType(), True),
            StructField("profileName", StringType(), True),
            StructField("valueType", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    return StructType(
        [
            StructField("apiVersion", StringType(), True),
            StructField("id", StringType(), True),
            StructField("deviceName", StringType(), True),
            StructField("profileName", StringType(), True),
            StructField("sourceName", StringType(), True),
            StructField("origin", LongType(), True),
            StructField("readings", ArrayType(reading_schema), True),
        ]
    )


def process_batch(batch_df: DataFrame, micro_batch_id: int) -> None:
    """Xử lý micro-batch: parse JSON → explode readings → ghi Bronze."""
    if batch_df.limit(1).count() == 0:
        return

    run_batch_id = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S%fZ')}_{micro_batch_id}"

    # Parse JSON envelope
    parsed = batch_df.select(
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.from_json(F.col("value").cast("string"), edgex_event_schema()).alias("event"),
    )

    # Explode readings array: 1 reading → 1 row
    exploded = parsed.withColumn("reading", F.explode(F.col("event.readings")))

    bronze_df = (
        exploded.select(
            F.coalesce(
                F.col("event.id"),
                F.concat_ws(
                    "-",
                    F.col("kafka_partition").cast("string"),
                    F.col("kafka_offset").cast("string"),
                    F.col("reading.resourceName"),
                ),
            ).alias("event_id"),
            F.col("event.apiVersion").alias("api_version"),
            F.col("event.deviceName").alias("device_name"),
            F.col("event.profileName").alias("profile_name"),
            F.col("event.sourceName").alias("source_name"),
            # Event timestamp (nanoseconds → TIMESTAMP)
            F.to_timestamp(
                F.from_unixtime((F.col("event.origin") / F.lit(1_000_000_000)).cast("double"))
            ).alias("event_origin"),
            # Reading fields
            F.col("reading.resourceName").alias("resource_name"),
            F.col("reading.valueType").alias("value_type"),
            F.col("reading.value").alias("raw_value"),
            F.to_timestamp(
                F.from_unixtime((F.col("reading.origin") / F.lit(1_000_000_000)).cast("double"))
            ).alias("reading_origin"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
        )
        .withColumn("batch_id", F.lit(run_batch_id))
        .withColumn("ingested_at", F.current_timestamp())
        # Filter out rows với event_origin null (malformed events)
        .filter(F.col("event_origin").isNotNull())
    )

    bronze_df.writeTo("local.edgex.edgex_bronze").append()
    print(f"[Bronze] Batch {run_batch_id}: {bronze_df.count()} readings written")


def main() -> None:
    config = resolve_config()
    ensure_bucket_exists(config["endpoint"], config["access_key"], config["secret_key"], config["bucket"])
    spark = build_spark_session("edgex_bronze_job", config)
    ensure_tables(spark)

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config["bootstrap_servers"])
        .option("subscribe", config["topic"])
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    query = (
        stream_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{config['checkpoint_location']}/bronze")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
