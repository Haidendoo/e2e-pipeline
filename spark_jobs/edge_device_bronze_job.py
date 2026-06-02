from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from edge_device_common import build_spark_session, ensure_bucket_exists, ensure_tables, resolve_config


def event_schema() -> StructType:
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
    if batch_df.limit(1).count() == 0:
        return

    run_batch_id = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S%fZ')}_{micro_batch_id}"
    parsed = batch_df.select(
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.from_json(F.col("value").cast("string"), event_schema()).alias("event"),
    )

    first_reading = F.element_at(F.col("event.readings"), F.lit(1))
    bronze_df = (
        parsed.select(
            F.coalesce(
                F.col("event.id"),
                F.concat_ws("-", F.col("kafka_partition").cast("string"), F.col("kafka_offset").cast("string")),
            ).alias("event_id"),
            F.col("event.apiVersion").alias("api_version"),
            F.col("event.deviceName").alias("device_name"),
            F.col("event.profileName").alias("profile_name"),
            F.col("event.sourceName").alias("source_name"),
            F.to_timestamp(F.from_unixtime((F.col("event.origin") / F.lit(1000000000)).cast("double"))).alias(
                "event_origin"
            ),
            F.to_timestamp(F.from_unixtime((first_reading.getField("origin") / F.lit(1000000000)).cast("double"))).alias(
                "reading_origin"
            ),
            first_reading.getField("value").alias("raw_value"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
        )
        .withColumn("batch_id", F.lit(run_batch_id))
        .withColumn("ingested_at", F.current_timestamp())
    )

    bronze_df.writeTo("local.edge_device.edge_device_bronze").append()


def main() -> None:
    config = resolve_config()
    ensure_bucket_exists(config["endpoint"], config["access_key"], config["secret_key"], config["bucket"])
    spark = build_spark_session("edge_device_bronze_job", config)
    ensure_tables(spark)

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config["bootstrap_servers"])
        .option("subscribe", config["topic"])
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        stream_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{config['checkpoint_location']}_v2/bronze")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
