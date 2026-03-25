from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

# Khởi tạo SparkSession kèm các package cần thiết.
PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
]

spark = SparkSession.builder \
    .appName("EdgeX_Streaming_Kafka_to_Iceberg") \
    .config("spark.jars.packages", ",".join(PACKAGES)) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://datalake/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.ui.host", "0.0    docker-compose up -d.0.0") \
    .getOrCreate()

# Schema chuẩn theo định dạng EdgeX (Telegraf-Node)
reading_schema = StructType([
    StructField("origin", LongType(), True),
    StructField("deviceName", StringType(), True),
    StructField("resourceName", StringType(), True),
    StructField("profileName", StringType(), True),
    StructField("valueType", StringType(), True),
    StructField("value", StringType(), True)
])

schema = StructType([
    StructField("apiVersion", StringType(), True),
    StructField("id", StringType(), True),
    StructField("deviceName", StringType(), True),
    StructField("profileName", StringType(), True),
    StructField("sourceName", StringType(), True),
    StructField("origin", LongType(), True),
    StructField("readings", ArrayType(reading_schema), True)
])

# 1. Spark Streaming - Đọc dữ liệu từ Kafka Topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "edgex_telegraf_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Parse dữ liệu theo cấu trúc EdgeX JSON
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Explode mảng readings để làm phẳng schema (mỗi reading là 1 dòng)
flattened_df = parsed_df.withColumn("reading", explode(col("readings"))) \
    .select(
        col("id").alias("event_id"),
        col("apiVersion"),
        col("deviceName"),
        col("profileName"),
        col("sourceName"),
        col("origin").alias("event_origin"),
        col("reading.origin").alias("reading_origin"),
        col("reading.resourceName").alias("resourceName"),
        col("reading.valueType").alias("valueType"),
        col("reading.value").alias("reading_value")
    )

# 3. Khởi tạo Database và Bảng Iceberg trước khi Stream nếu chưa có
spark.sql("CREATE DATABASE IF NOT EXISTS local.db")
try:
    if not spark.catalog.tableExists("local.db.edgex_telegraf"):
        print("Đang khởi tạo bảng Iceberg: local.db.edgex_telegraf...")
        empty_df = spark.createDataFrame([], flattened_df.schema)
        empty_df.writeTo("local.db.edgex_telegraf").using("iceberg").create()
except Exception as e:
    print("Thông báo khi khởi tạo bảng: ", e)

# 4. Ghi dữ liệu trực tiếp dưới định dạng Iceberg ra MinIO Storage
query = flattened_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime='2 seconds') \
    .option("checkpointLocation", "s3a://datalake/checkpoints/edgex_telegraf") \
    .option("path", "local.db.edgex_telegraf") \
    .start()

print("Bắt đầu Spark Streaming (EdgeX Kafka -> MinIO/Iceberg)... Nhấn Ctrl+C để kết thúc.")

# Block main thread chờ kết quả
query.awaitTermination()
