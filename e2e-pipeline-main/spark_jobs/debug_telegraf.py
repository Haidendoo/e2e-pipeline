from edgex_common import build_spark_session, resolve_config

config = resolve_config()
spark = build_spark_session("debug_bronze", config)

bronze = spark.table("local.edgex.edgex_bronze")
print("--- BRONZE PROFILES ---")
bronze.groupBy("profile_name").count().show(truncate=False)

print("--- BRONZE TELEGRAF SAMPLE ---")
bronze.filter("profile_name = 'Telegraf-Full-Node-Profile'").show(5, truncate=False)

silver = spark.table("local.edgex.edgex_silver")
print("--- SILVER PROFILES ---")
silver.groupBy("profile_name").count().show(truncate=False)
