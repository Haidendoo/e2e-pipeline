"""
Spark Job to Write Data to MinIO with Iceberg Table Format
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime
import os

# Get MinIO configuration from environment
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
minio_bucket = os.getenv("MINIO_BUCKET", "test-bucket")

# Ensure endpoint has proper protocol
if minio_endpoint and not minio_endpoint.startswith(("http://", "https://")):
    minio_endpoint = f"http://{minio_endpoint}"

# Create bucket if it doesn't exist
print("=" * 80)
print("ENSURING MINIO BUCKET EXISTS")
print("=" * 80)
try:
    import boto3
    from botocore.exceptions import ClientError
    
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name="us-east-1",
    )
    
    try:
        s3_client.head_bucket(Bucket=minio_bucket)
        print(f"✓ Bucket '{minio_bucket}' already exists")
    except ClientError:
        s3_client.create_bucket(Bucket=minio_bucket)
        print(f"✓ Bucket '{minio_bucket}' created")
except Exception as e:
    print(f"Warning: Could not verify/create bucket: {e}")
    print("Proceeding anyway...")
print("=" * 80)

# Initialize Spark Session with Iceberg and S3 configurations
spark = SparkSession.builder \
    .appName("IcebergWriteJob") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", f"s3a://{minio_bucket}/spark-iceberg") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("=" * 80)
print("ICEBERG WRITE JOB STARTED")
print("=" * 80)
print(f"Spark Version: {spark.version}")
print(f"App Name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")
print(f"MinIO Endpoint: {minio_endpoint}")
print(f"MinIO Bucket: {minio_bucket}")
print(f"Warehouse Path: s3a://{minio_bucket}/warehouse")
print(f"Timestamp: {datetime.now()}")
print("=" * 80)

# Verify catalog configuration
print("\n✓ Checking catalog configuration:")
print("-" * 80)
try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    for catalog in catalogs:
        print(f"  Catalog: {catalog}")
except Exception as e:
    print(f"  Warning: Could not list catalogs: {e}")

# Define schema
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=True),
    StructField("value", IntegerType(), nullable=True),
    StructField("event_time", TimestampType(), nullable=False),
])

# Create sample data
now = datetime.now()
data = [
    (1, "Product A", "Electronics", 100, now),
    (2, "Product B", "Clothing", 50, now),
    (3, "Product C", "Electronics", 150, now),
    (4, "Product D", "Food", 30, now),
    (5, "Product E", "Clothing", 75, now),
]

try:
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    
    print("\n✓ Sample DataFrame created:")
    print("-" * 80)
    df.show(truncate=False)
    
    print("\n✓ DataFrame Schema:")
    print("-" * 80)
    df.printSchema()
    
    # Define Iceberg table name
    namespace = "spark_demo"
    table_name = "products"
    full_table_name = f"local.{namespace}.{table_name}"
    
    print(f"\n✓ Writing data to Iceberg table: {full_table_name}")
    print("-" * 80)
    
    # Create namespace if it doesn't exist
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{namespace}")
        print(f"✓ Namespace 'local.{namespace}' created or already exists")
    except Exception as ns_error:
        print(f"Warning creating namespace: {ns_error}")
    
    # Write DataFrame to Iceberg table
    print(f"Attempting to write to {full_table_name}...")
    df.writeTo(full_table_name) \
        .using("iceberg") \
        .createOrReplace()
    
    print(f"✓ Data successfully written to {full_table_name}")
    
    # Verify the write by reading back the data
    print("\n✓ Reading back data from Iceberg table:")
    print("-" * 80)
    result_df = spark.table(full_table_name)
    result_df.show(truncate=False)
    
    # Show table metadata
    print("\n✓ Table Metadata:")
    print("-" * 80)
    try:
        spark.sql(f"DESCRIBE EXTENDED {full_table_name}").show(20, truncate=False)
    except Exception as desc_error:
        print(f"Could not describe table: {desc_error}")
    
    print("\n✓ Table History:")
    print("-" * 80)
    try:
        spark.sql(f"SELECT * FROM {full_table_name}.history").show(truncate=False)
    except Exception as hist_error:
        print(f"Could not get table history: {hist_error}")
    
    print("\n" + "=" * 80)
    print("ICEBERG WRITE JOB COMPLETED SUCCESSFULLY")
    print("=" * 80)
    
except Exception as e:
    print(f"\n✗ Error occurred: {str(e)}")
    import traceback
    traceback.print_exc()
    raise
finally:
    spark.stop()
