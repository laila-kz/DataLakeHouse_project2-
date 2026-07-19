# Design Decisions - Data Lakehouse Project

## ✅ Spark-MinIO Connectivity - WORKING CONFIGURATION

**Date Tested:** 2026-07-19

### Spark Configuration for S3A (MinIO)

```python
spark = SparkSession.builder \
    .appName("MinIO Connectivity Test") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()