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



## Dataset: E-Commerce Behavior Data from Multi-Category Store

**Source:** Kaggle - https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

### Files Available:
- [List the files you see on the Data tab]
- Example: `2019-Oct.csv` (~500 MB)
- Example: `2019-Nov.csv` (~450 MB)

### Dataset Description:
- [What kind of data is it?]
- Example: Customer behavior data from an e-commerce store
- Contains: user sessions, product views, purchases, etc.

### Key Columns:
- [List main columns from the Data tab]
- Example: `event_time`, `user_id`, `product_id`, `category_code`, `price`, etc.



## Ingestion Service Design Specification

### Purpose
Ingest data from Kaggle API and upload to MinIO `raw` bucket with idempotency, retries, and structured logging.

---

### 1. Sequence of Steps

The script performs these steps in order:

1. **Authenticate**
   - Read `KAGGLE_USERNAME` and `KAGGLE_KEY` from environment variables
   - Initialize Kaggle API client

2. **Check if Already Ingested (Idempotency Check)**
   - Check if file exists in MinIO at: `s3a://raw/ecommerce_events/`
   - **Check condition:** Does a `_SUCCESS` file exist at `s3a://raw/ecommerce_events/_SUCCESS`?
   - If YES → Skip download, log `{"event": "skip_already_ingested"}`
   - If NO → Proceed to download

3. **Download Data**
   - Use Kaggle API to download dataset
   - Dataset: `mkechinov/ecommerce-behavior-data-from-multi-category-store`
   - Save temporarily to `/tmp/ecommerce_data/`

4. **Validate Downloaded Data**
   - Check that files were downloaded
   - Check that files are not empty (> 0 bytes)

5. **Extract/Process**
   - [Add extraction details if needed]

6. **Upload to MinIO**
   - Upload files to `s3a://raw/ecommerce_events/`
   - Create a `_SUCCESS` marker file after upload

7. **Structured Logging**
   - Emit JSON logs at each step

---

### 2. Idempotency Check

**Exact check:** Does an object exist at `s3a://raw/ecommerce_events/_SUCCESS`?

| Condition | Action |
|-----------|--------|
| `_SUCCESS` exists | Skip download, log "already ingested" |
| `_SUCCESS` does NOT exist | Proceed with download and processing |

**Why this works:** The `_SUCCESS` file is only created after a successful upload. If it exists, we know the data is already in MinIO.

---

### 3. Retry Policy

**Backoff Strategy:** Exponential backoff with jitter

| Attempt | Delay |
|---------|-------|
| 1st retry | Wait 1 second |
| 2nd retry | Wait 2 seconds |
| 3rd retry | Wait 4 seconds |
| 4th retry | Wait 8 seconds |
| 5th retry | Wait 16 seconds |

**Max attempts:** 5 total tries (initial + 4 retries)

**When to retry:**
- Network errors
- Timeout errors
- 5xx server errors

**When NOT to retry:**
- Authentication errors (401)
- Permission errors (403)
- Invalid request errors (400)

---

### 4. Structured Logging Format

All logs must be JSON format with these fields:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `event` | string | What happened | `"download_start"`, `"download_success"` |
| `status` | string | `"success"` or `"failure"` | `"success"` |
| `duration_ms` | integer | Time taken in milliseconds | `1234` |
| `bytes` | integer | Size of data processed | `48213911` |
| `error` | string | Error message (if failure) | `"Connection timeout"` |

**Example log output:**
```json
{"event": "download_start", "status": "running", "timestamp": "2026-07-19T22:00:00Z", "dataset": "ecommerce-behavior"}
{"event": "download_success", "status": "success", "duration_ms": 2345, "bytes": 48213911, "file_count": 3}
{"event": "upload_start", "status": "running", "timestamp": "2026-07-19T22:00:03Z"}
{"event": "upload_success", "status": "success", "duration_ms": 1234, "bytes": 48213911}
{"event": "ingestion_complete", "status": "success", "total_duration_ms": 3579}


## Implementation Reconciliation (2026-07-20)

### ✅ What Was Implemented as Planned
- All core functionality from the design doc
- Idempotency via MinIO `_SUCCESS` marker
- Structured JSON logging
- Retry with exponential backoff

### 🔄 Deliberate Deviations
1. **Manual upload for large files**
   - Reason: Network limitations
   - Impact: Script still works for any file size, just took time

2. **No automatic cleanup of local files**
   - Reason: Keep for debugging/backup
   - Impact: May need to clean manually

### 📝 Future Improvements
- Add automatic cleanup of local files after upload
- Add checksum validation (MD5)
- Add email notifications on failure



## Bronze Schema — Field Inventory

**Dataset:** E-Commerce Behavior Data from Multi-Category Store
**Source:** Kaggle (mkechinov)
**Sample Files:** 2019-Oct.csv, 2019-Nov.csv

### Field Inventory (Based on Real Data Inspection)

| # | Column Name | Sample Values | Apparent Type | Edge Cases Observed |
|---|-------------|---------------|---------------|---------------------|
| 1 | `event_time` | 2019-11-01 00:00:00 UTC | Timestamp | All rows have timestamps |
| 2 | `event_type` | view, purchase, cart | String | Values: view, cart, purchase, remove_from_cart |
| 3 | `product_id` | 1003461, 5000088 | Long | Always numeric, no nulls |
| 4 | `category_id` | 2053013555631882655 | Long | Always numeric, no nulls |
| 5 | `category_code` | electronics.smartphone, **empty** | String | **NULL/empty values exist!** (rows 3, 4 in samples) |
| 6 | `brand` | xiaomi, lg, **empty** | String | **NULL/empty values exist!** (row with sofa brand is empty) |
| 7 | `price` | 489.07, 28.31 | Double | Always numeric, no nulls |
| 8 | `user_id` | 520088904, 530496790 | Long | Always numeric, no nulls |
| 9 | `user_session` | 4d3b30da-a5e4-49df-b1a8-ba5943f1dd33 | String | Always present (UUID format) |

### Observations from Real Data:

| Observation | Evidence | Impact |
|-------------|----------|--------|
| `category_code` is sometimes empty | Row 3: `,,creed,28.31` (empty between commas) | Must be nullable |
| `brand` is sometimes empty | Row 4: `sofa,,543.10` (empty between commas) | Must be nullable |
| `user_session` is always a UUID | Format: `4d3b30da-a5e4-49df-b1a8-ba5943f1dd33` | String, non-nullable |
| `price` is always numeric | All samples have decimal values | Double, non-nullable |
| `event_type` is always present | view, purchase, cart | String, non-nullable |
| `category_id` is extremely large | 2053013555631882655 | Long, non-nullable |

### Important Decision:

Since we observed empty values for `category_code` and `brand` in the real data, **these fields MUST be nullable** in our Bronze schema. If we mark them non-nullable, Spark will reject valid rows that have empty values.



## Bronze Schema Design

### Explicit StructType Schema (Based on Real Data)

| Column | Spark Type | Nullable? | Reasoning |
|--------|------------|-----------|-----------|
| `event_time` | TimestampType | No | Every event has a timestamp in every row |
| `event_type` | StringType | No | Always one of: view, cart, purchase, remove_from_cart |
| `product_id` | LongType | No | Always a numeric ID |
| `category_id` | LongType | No | Always a numeric category ID |
| `category_code` | StringType | **Yes** | **Real data has empty values!** (must allow null) |
| `brand` | StringType | **Yes** | **Real data has empty values!** (must allow null) |
| `price` | DoubleType | No | Always a decimal number |
| `user_id` | LongType | No | Always a numeric user ID |
| `user_session` | StringType | No | Always a UUID string |

### Complete PySpark Schema:

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    TimestampType, LongType, DoubleType
)

BRONZE_EVENT_SCHEMA = StructType([
    StructField("event_time", TimestampType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("product_id", LongType(), nullable=False),
    StructField("category_id", LongType(), nullable=False),
    StructField("category_code", StringType(), nullable=True),   # IMPORTANT: nullable
    StructField("brand", StringType(), nullable=True),           # IMPORTANT: nullable
    StructField("price", DoubleType(), nullable=False),
    StructField("user_id", LongType(), nullable=False),
    StructField("user_session", StringType(), nullable=False)
])