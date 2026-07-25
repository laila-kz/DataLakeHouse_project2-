#!/usr/bin/env python3
"""
Silver Transform - Incremental processing with watermarking and deduplication
"""

import os
import sys
import uuid
import json
import logging
import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws,
    split, when, size, to_utc_timestamp, date_format
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed")
    pass


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "event": getattr(record, "event", record.msg),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if hasattr(record, "extra"):
            log_entry.update(record.extra)

        return json.dumps(log_entry)


def setup_logging():
    """Configure structured JSON logging"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    return logger


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Silver Transform - Incremental processing with watermarking"
    )

    parser.add_argument(
        "--batch-id",
        default=str(uuid.uuid4()),
        help="Batch ID for this run",
    )
    parser.add_argument(
        "--bronze-path",
        default="s3a://bronze/ecommerce_events/",
        help="Path to Bronze Delta table",
    )
    parser.add_argument(
        "--silver-path",
        default="s3a://silver/ecommerce_events/",
        help="Path to Silver Delta table",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
    )
    return parser.parse_args()


def create_spark_session(logger):
    """Create Spark session with MinIO S3A configuration and Delta Lake support"""

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    logger.info(
        "Creating Spark session",
        extra={"event": "spark_session_start", "endpoint": endpoint},
    )

    spark = (
        SparkSession.builder.appName("Silver Transform")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    logger.info("Spark session created", extra={"event": "spark_session_success"})
    return spark


def read_watermark(spark, silver_path, logger):
    """
    Read current watermark from silver._watermarks Delta table
    Returns: watermark_value (timestamp) or None if no watermark exists
    """
    watermark_path = f"{silver_path}_watermarks"

    logger.info(
        "Reading watermark",
        extra={"event": "watermark_read_start", "path": watermark_path},
    )

    try:
        df = spark.read.format("delta").load(watermark_path)
        row = df.filter(col("pipeline_name") == "ecommerce_silver").first()

        if row:
            watermark_value = row.last_processed_event_time
            logger.info(
                "Watermark found",
                extra={
                    "event": "watermark_found",
                    "value": str(watermark_value),
                    "updated_at": str(row.updated_at),
                },
            )
            return watermark_value
        else:
            logger.info(
                "No watermark found for pipeline",
                extra={"event": "watermark_missing_pipeline"},
            )
            return None

    except Exception as e:
        logger.info(
            "No watermark table found - first run",
            extra={"event": "watermark_table_missing", "error": str(e)},
        )
        return None


def read_bronze_new_rows(spark, bronze_path, watermark_value, logger):
    """
    Read Bronze rows with event_time > watermark_value
    If watermark_value is None, read all rows
    """
    logger.info(
        "Reading Bronze rows",
        extra={
            "event": "bronze_read_start",
            "path": bronze_path,
            "watermark": str(watermark_value) if watermark_value else "NONE (first run)",
        },
    )

    bronze_df = spark.read.format("delta").load(bronze_path)

    if watermark_value:
        bronze_df = bronze_df.filter(col("event_time") > watermark_value)
        logger.info(
            "Filtering by watermark",
            extra={"event": "watermark_filter_applied", "watermark": str(watermark_value)},
        )
    else:
        logger.info(
            "No watermark - processing all data",
            extra={"event": "watermark_no_filter"},
        )

    row_count = bronze_df.count()
    logger.info(
        "Bronze rows read",
        extra={
            "event": "bronze_read_complete",
            "row_count": row_count,
            "filtered_by_watermark": watermark_value is not None,
        },
    )

    return bronze_df


def apply_business_filters(df, logger):
    """
    Apply filters in order:
    1. user_session IS NOT NULL
    2. price >= 0
    3. event_time >= '2019-01-01' AND event_time <= CURRENT_DATE()

    Log rows dropped per filter
    """
    initial_count = df.count()
    logger.info(
        "Applying business filters",
        extra={"event": "filter_start", "rows_before": initial_count},
    )

    null_session_count = df.filter(col("user_session").isNull()).count()
    df = df.filter(col("user_session").isNotNull())
    logger.info(
        "Filter: null user_session",
        extra={
            "event": "filter_null_session",
            "rows_dropped": null_session_count,
            "rows_remaining": df.count(),
        },
    )

    negative_price_count = df.filter(col("price") < 0).count()
    df = df.filter(col("price") >= 0)
    logger.info(
        "Filter: negative price",
        extra={
            "event": "filter_negative_price",
            "rows_dropped": negative_price_count,
            "rows_remaining": df.count(),
        },
    )

    out_of_range_count = df.filter(
        (col("event_time") < "2019-01-01") | (col("event_time") > datetime.now())
    ).count()
    df = df.filter(
        (col("event_time") >= "2019-01-01") & (col("event_time") <= datetime.now())
    )
    logger.info(
        "Filter: event_time out of range",
        extra={
            "event": "filter_date_range",
            "rows_dropped": out_of_range_count,
            "rows_remaining": df.count(),
        },
    )

    final_count = df.count()
    logger.info(
        "Filters complete",
        extra={
            "event": "filter_complete",
            "rows_before": initial_count,
            "rows_after": final_count,
            "total_dropped": initial_count - final_count,
        },
    )

    return df


def parse_category_code(df, logger):
    """
    Parse category_code into category_l1, category_l2, category_l3
    Handle fewer than 3 levels with NULL
    """
    logger.info("Parsing category_code", extra={"event": "category_parse_start"})

    df = df.withColumn("category_parts", split(col("category_code"), "\\."))

    df = df.withColumn(
        "category_l1",
        when(size("category_parts") >= 1, col("category_parts")[0]).otherwise(None),
    )
    df = df.withColumn(
        "category_l2",
        when(size("category_parts") >= 2, col("category_parts")[1]).otherwise(None),
    )
    df = df.withColumn(
        "category_l3",
        when(size("category_parts") >= 3, col("category_parts")[2]).otherwise(None),
    )

    df = df.drop("category_parts")

    logger.info(
        "Category parsing complete",
        extra={"event": "category_parse_complete"},
    )

    return df


def compute_event_key(df, logger):
    """
    Compute synthetic deduplication key using SHA-256
    Format: sha2(concat_ws("|", user_id, event_type, product_id, event_time_iso), 256)
    """
    logger.info("Computing deduplication key", extra={"event": "event_key_start"})

    df = df.withColumn(
        "event_time_iso",
        date_format(to_utc_timestamp(col("event_time"), "UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )

    df = df.withColumn(
        "event_key",
        sha2(
            concat_ws(
                "|",
                col("user_id").cast("string"),
                col("event_type"),
                col("product_id").cast("string"),
                col("event_time_iso"),
            ),
            256,
        ),
    )

    df = df.drop("event_time_iso")

    key_count = df.select("event_key").distinct().count()
    total_count = df.count()

    logger.info(
        "Deduplication key computed",
        extra={
            "event": "event_key_complete",
            "total_rows": total_count,
            "distinct_keys": key_count,
            "duplicate_rate": round(((total_count - key_count) / total_count) * 100, 4) if total_count > 0 else 0,
        },
    )

    return df


def main():
    args = parse_args()
    logger = setup_logging()
    logger.setLevel(getattr(logging, args.log_level))

    logger.info(
        "Silver transform started",
        extra={
            "event": "job_started",
            "batch_id": args.batch_id,
            "bronze_path": args.bronze_path,
            "silver_path": args.silver_path,
        },
    )

    spark = create_spark_session(logger)

    try:
        watermark_value = read_watermark(spark, args.silver_path, logger)
        bronze_df = read_bronze_new_rows(spark, args.bronze_path, watermark_value, logger)
        filtered_df = apply_business_filters(bronze_df, logger)
        enriched_df = parse_category_code(filtered_df, logger)
        final_df = compute_event_key(enriched_df, logger)

        logger.info(
            "Checking for duplicate keys",
            extra={"event": "duplicate_check_start"},
        )

        duplicates = final_df.groupBy("event_key").count().filter(col("count") > 1)
        duplicate_count = duplicates.count()

        if duplicate_count > 0:
            logger.info(
                "Duplicate keys found",
                extra={
                    "event": "duplicate_keys_found",
                    "duplicate_groups": duplicate_count,
                    "sample_duplicates": duplicates.limit(5).collect(),
                },
            )
        else:
            logger.info(
                "No duplicate keys found",
                extra={"event": "duplicate_keys_none"},
            )

        output_path = f"{args.silver_path}validation/batch_id={args.batch_id}/"
        final_df.write.mode("overwrite").parquet(output_path)

        logger.info(
            "Validation output written",
            extra={
                "event": "validation_write_complete",
                "path": output_path,
                "row_count": final_df.count(),
            },
        )

        logger.info(
            "Silver transform completed",
            extra={"event": "job_completed", "status": "success"},
        )

    except Exception as e:
        logger.error(
            "Job failed",
            extra={"event": "job_failure", "error": str(e)},
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
