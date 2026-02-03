"""
Simple Spark Test Job
Creates a DataFrame, processes it, and prints results
"""

from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkTestJob") \
    .getOrCreate()

print("=" * 60)
print("SPARK TEST JOB STARTED")
print("=" * 60)
print(f"Spark Version: {spark.version}")
print(f"App Name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")
print(f"Timestamp: {datetime.now()}")
print("=" * 60)

# Create a sample DataFrame
data = [
    ("Alice", 25, 50000),
    ("Bob", 30, 60000),
    ("Charlie", 28, 55000),
    ("David", 35, 70000),
    ("Eve", 27, 52000),
]

columns = ["Name", "Age", "Salary"]

try:
    df = spark.createDataFrame(data, columns)
    
    print("\n✓ Sample DataFrame created:")
    print("-" * 60)
    
    # Collect to driver to avoid worker issues
    rows = df.collect()
    for row in rows:
        print(row)
    
    print("\n✓ DataFrame Schema:")
    print("-" * 60)
    df.printSchema()
    
    print("\n✓ Row Count:")
    print("-" * 60)
    print(f"Total rows: {len(rows)}")
    
    print("\n" + "=" * 60)
    print("SPARK TEST JOB COMPLETED SUCCESSFULLY")
    print("=" * 60)
    
except Exception as e:
    print(f"\n✗ Error during job execution:")
    print(f"{type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
