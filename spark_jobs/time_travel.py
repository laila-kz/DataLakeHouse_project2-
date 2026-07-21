from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Time Travel") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

table = "delta.`s3a://bronze/ecommerce_events/`"

print("🚀 TIME TRAVEL DEMO")
print("=" * 60)

# Current version count
current = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
print(f"\n📊 Current version count: {current:,}")

# Version 0 count
version0 = spark.sql(f"SELECT COUNT(*) FROM {table} VERSION AS OF 0").collect()[0][0]
print(f"📊 Version 0 count: {version0:,}")

# Calculate difference
diff = current - version0
print(f"📈 Difference: {diff:,} more rows in current version")

if diff > 0:
    print("✅ Time travel works! Version 0 has fewer rows (before second run)")

# Show sample from Version 0
print("\n📊 Sample data from Version 0 (first 2 rows):")
spark.sql(f"SELECT * FROM {table} VERSION AS OF 0 LIMIT 2").show(truncate=False)

spark.stop()
