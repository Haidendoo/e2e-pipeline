from __future__ import annotations

import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession

from edge_device_common import build_spark_session, resolve_config


def print_table(spark: SparkSession, table_name: str, limit_rows: int = 20) -> None:
    print(f"\n=== {table_name} ===")
    try:
        df = spark.table(table_name)
    except Exception:
        print("Table not found")
        return

    row_count = df.count()
    print(f"Rows: {row_count}")
    if row_count == 0:
        print("(empty)")
        return

    df.limit(limit_rows).show(truncate=False)


def export_table_csv(spark: SparkSession, table_name: str, output_dir: str) -> None:
    table_suffix = table_name.split(".")[-1]
    tmp_dir = Path(output_dir) / f"{table_suffix}_tmp"
    final_csv = Path(output_dir) / f"{table_suffix}.csv"

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    if final_csv.exists():
        final_csv.unlink()

    df = spark.table(table_name)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(tmp_dir))

    part_files = list(tmp_dir.glob("part-*.csv"))
    if not part_files:
        print(f"No CSV part file generated for {table_name}")
        return

    part_files[0].rename(final_csv)
    for leftover in tmp_dir.glob("*"):
        if leftover.exists() and leftover.is_file():
            leftover.unlink()
    tmp_dir.rmdir()

    print(f"CSV exported: {final_csv}")


def main() -> None:
    config = resolve_config()
    spark = build_spark_session("edge_device_reader_job", config)

    namespace = os.getenv("EDGE_DEVICE_ICEBERG_NAMESPACE", "edge_device")
    output_dir = os.getenv("EDGE_DEVICE_CSV_OUTPUT_DIR", "/tmp/edge_device_csv")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    bronze_table = f"local.{namespace}.edge_device_bronze"
    silver_table = f"local.{namespace}.edge_device_silver"
    gold_table = f"local.{namespace}.edge_device_gold"

    print_table(spark, bronze_table)
    print_table(spark, silver_table)
    print_table(spark, gold_table)

    export_table_csv(spark, bronze_table, output_dir)
    export_table_csv(spark, silver_table, output_dir)
    export_table_csv(spark, gold_table, output_dir)

    spark.stop()


if __name__ == "__main__":
    main()
