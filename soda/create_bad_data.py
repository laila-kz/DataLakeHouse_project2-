from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder \
    .appName("Create Bad Data") \
    .config("spark.jars", "/opt/spark/jars-extra/delta-spark_2.12-3.1.0.jar,/opt/spark/jars-extra/delta-storage-3.1.0.jar,/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

bad_data = [
    (datetime.now(), "view", 12345, 67890, "electronics.phone", "apple", 999.99, 111111, "session_123"),
    (None, "view", 12345, 67890, "electronics.phone", "apple", 999.99, 222222, "session_456"),
    (datetime.now(), "view", None, 67890, "electronics.phone", "apple", 999.99, 333333, "session_789"),
]

schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])

df = spark.createDataFrame(bad_data, schema)
df_with_lineage = (
    df.withColumn("ingested_at", current_timestamp())
      .withColumn("source_file", lit("test_bad_data.csv"))
      .withColumn("pipeline_run_id", lit("test_run_123"))
      .withColumn("batch_id", lit("test_batch_456"))
      .withColumn("event_date", df.event_time)
)

df_with_lineage.write.format("delta").mode("append").save("s3a://bronze/test_bad_events/")
print("✅ Bad data written to s3a://bronze/test_bad_events/")
spark.stop()
