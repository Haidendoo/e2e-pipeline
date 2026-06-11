from __future__ import annotations

from edgex_common import build_spark_session, resolve_config
from pyspark.sql import SparkSession


def main() -> None:
    config = resolve_config()
    # spark = build_spark_session("reset_data_job", config)
    # Rebuild spark with packages
    spark = (
        SparkSession.builder.appName("reset_data_job")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0")
        .config("spark.jars.ivy", "/tmp/ivy_reset")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.local.uri", "jdbc:postgresql://postgres:5432/airflow")
        .config("spark.sql.catalog.local.jdbc.user", "airflow")
        .config("spark.sql.catalog.local.jdbc.password", "airflow")
        .config("spark.sql.catalog.local.warehouse", f"s3a://{config['bucket']}/warehouse")
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.catalog.local.s3.endpoint", config["endpoint"])
        .config("spark.sql.catalog.local.s3.access-key-id", config["access_key"])
        .config("spark.sql.catalog.local.s3.secret-access-key", config["secret_key"])
        .config("spark.hadoop.fs.s3a.endpoint", config["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", config["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", config["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


    print("\n[RESET] Đang tiến hành xóa dữ liệu cũ trong bảng Bronze...")
    try:
        spark.sql("DELETE FROM local.edgex.edgex_bronze")
        print("[RESET] Xóa dữ liệu Bronze thành công.")
    except Exception as e:
        print(f"[RESET] Lỗi khi xóa Bronze: {e}")

    print("\n[RESET] Đang tiến hành xóa dữ liệu cũ trong bảng Silver...")
    try:
        spark.sql("DELETE FROM local.edgex.edgex_silver")
        print("[RESET] Xóa dữ liệu Silver thành công.")
    except Exception as e:
        print(f"[RESET] Lỗi khi xóa Silver: {e}")

    print("\n[RESET] Đang tiến hành xóa dữ liệu cũ trong bảng Gold...")
    try:
        spark.sql("DELETE FROM local.edgex.edgex_gold")
        print("[RESET] Xóa dữ liệu Gold thành công.")
    except Exception as e:
        print(f"[RESET] Lỗi khi xóa Gold: {e}")

    # Clear file log output json (Tùy chọn)
    import os
    debug_json_path = "/opt/spark_jobs/logs/edgex_silver_debug_output.json"
    if os.path.exists(debug_json_path):
        try:
            os.remove(debug_json_path)
            print(f"\n[RESET] Đã xóa file log {debug_json_path}")
        except Exception as e:
            pass

    print("\n[RESET] Hoàn tất! Dữ liệu cũ đã được dọn dẹp.\n")
    spark.stop()


if __name__ == "__main__":
    main()
