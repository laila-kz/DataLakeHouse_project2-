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


## Silver Layer: Watermark Design

### Watermark Storage Location
A Delta table at `s3a://silver/_watermarks/` is the preferred storage mechanism. It keeps the watermark state alongside the Silver data in the same storage layer, is easy to inspect, and supports atomic updates with Delta transactions.

### Watermark Table Schema
The watermark table should contain:
- `pipeline_name` — identifies the pipeline or job producing the Silver data
- `last_processed_event_time` — the highest event timestamp successfully processed
- `updated_at` — timestamp of the most recent watermark update
- `last_batch_id` — identifier of the batch that produced the last successful commit

### Update Timing (CRITICAL!)
The watermark advances only after the MERGE operation completes successfully and the transaction is committed. This ensures the watermark represents durable, fully applied data and prevents partially processed state from being treated as complete.

### What Goes Wrong If You Advance First
If the watermark advances before the MERGE succeeds, a downstream retry can skip rows that were never merged. Example: the pipeline processes a batch ending at `2026-01-10T10:00:00Z`, updates the watermark to that timestamp, then crashes during the MERGE. A retry starts from the next batch, but the earlier batch’s rows are now considered already processed and are silently skipped, causing data loss.

### Initial Run (No Watermark)
When the watermark table is empty, the initial Silver run should process the full available Bronze range from the earliest available data and then write the initial watermark after the first successful MERGE commit.

### What Value Does Watermark Track?
The watermark should track `event_time`, not `ingested_at`. `event_time` is the business event timestamp and is the correct basis for incremental processing. `ingested_at` is a pipeline metadata field and can vary depending on ingestion timing.

### Edge Case: Late-Arriving Data
If data arrives later than the watermark threshold, it should not be dropped. The pipeline should continue to process late-arriving events by re-reading the relevant Bronze partitions or by using a bounded reprocess window, then re-running the MERGE logic with the same deterministic deduplication key.


## Silver Layer: Synthetic Deduplication Key

### Why We Need a Synthetic Key
The dataset does not have a natural primary key because clickstream events are observational records rather than true entity records. Multiple rows can represent the same event from the same user and product at the same time across retries, re-ingests, or replays, so a deterministic synthetic key is required to make the Silver table idempotent.

### Key Design
The synthetic key should be built from these fields in this exact order:
1. `user_id`
2. `event_type`
3. `product_id`
4. `event_time`

The fields must be concatenated using the pipe character `|`.

### Hash Function & Format
- Hash: SHA-256
- Concatenation Format: `field1|field2|field3|field4`
- Timestamp Format: ISO-8601 UTC (e.g., `2019-11-01T00:00:00Z`)

### Determinism Test (Manual Verification)
Using the sample row from the provided data:
- `event_time = 2019-11-01 00:00:00 UTC`
- `event_type = view`
- `product_id = 1003461`
- `user_id = 520088904`

The canonical string is:
`2019-11-01T00:00:00Z|view|1003461|520088904`

The SHA-256 hash is:
`095b66e6d760b7b354d776dbec3cedcfed585a8c6aaadab40ac46b2ce775c1fb`

### Implementation in Spark (Pseudocode)
```sql
sha2(concat_ws("|", user_id, event_type, product_id, date_format(event_time, "yyyy-MM-dd'T'HH:mm:ss'Z'")), 256)
```


## Silver Layer: MERGE INTO Behavior

### Match Condition
The MERGE should use the deterministic synthetic key as the join condition:
`ON target.event_key = source.event_key`

### Behavior on Match (WHEN MATCHED)
No `WHEN MATCHED` branch is needed. Clickstream events are immutable facts, so a matched row should be treated as already present and left unchanged.

### Behavior on No Match (WHEN NOT MATCHED)
All rows that do not already exist in the Silver table should be inserted.

### Complete MERGE Statement
```sql
MERGE INTO silver_events AS target
USING staged_events AS source
ON target.event_key = source.event_key
WHEN NOT MATCHED THEN
  INSERT *
```

### Why This Design Fits Clickstream Data
This insert-only MERGE pattern is correct because clickstream events are append-only facts. There is no business need to update an existing event once it has been written into Silver, and preserving immutability avoids accidental overwrites or reprocessing drift.


## Silver Layer: Business Rule Filters

### Filter Order (Performance-Optimized)
The filters should run in this order for efficiency:
1. `user_session IS NOT NULL`
2. `price >= 0`
3. `event_time BETWEEN '2019-01-01' AND CURRENT_DATE()`

This ordering removes the most likely invalid rows early, reducing the amount of data that needs to pass through subsequent checks.

### Filter 1: Null user_session
Predicate:
```sql
WHERE user_session IS NOT NULL
```

### Filter 2: Negative Price
Predicate:
```sql
WHERE price >= 0
```

### Filter 3: Event_time Range
Predicate:
```sql
WHERE event_time >= '2019-01-01' AND event_time <= CURRENT_DATE()
```

### Logging Each Filter
Each filter should emit a structured JSON log with the number of rows dropped, the filter name, and the batch identifier so the lineage of business-rule decisions is easy to audit.

### Why No Quarantine for Business Rules?
No quarantine table is required for these business rules because they represent intentional data-quality decisions rather than schema violations. These rules are part of the Silver transformation contract and should be enforced as part of the normal filter flow.


## Silver Layer: category_code Parsing

### Source Data Format
The Bronze column `category_code` may appear as dot-delimited strings such as:
- `electronics.smartphone`
- `appliances.kitchen.washer`

### Parsing Specification
- Target columns: `category_l1`, `category_l2`, `category_l3`
- Split the value on `.` and map the first three levels to the target columns
- Missing levels should become `NULL`

### Edge Cases Table
| Input | category_l1 | category_l2 | category_l3 |
|-------|-------------|-------------|-------------|
| `electronics.smartphone` | `electronics` | `smartphone` | `NULL` |
| `appliances.kitchen.washer` | `appliances` | `kitchen` | `washer` |
| `electronics` | `electronics` | `NULL` | `NULL` |
| `NULL` | `NULL` | `NULL` | `NULL` |

### Implementation in Spark (Pseudocode)
```sql
split(category_code, '\.') as parts
when(size(parts) >= 1, parts[0]).otherwise(null) as category_l1
when(size(parts) >= 2, parts[1]).otherwise(null) as category_l2
when(size(parts) >= 3, parts[2]).otherwise(null) as category_l3
```

### Why "Extra Levels" Are Dropped
Only the first three levels are preserved to keep the Silver schema simple and stable. Extra levels beyond the third are intentionally dropped because the agreed contract only requires three hierarchical levels.


## Silver Layer: Implementation Guide Reconciliation

### Check Against Requirements
| Guide Requirement | Our Design | Status |
|-------------------|------------|--------|
| Watermark stored in Delta | Delta table at `s3a://silver/_watermarks/` | ✅ |
| Watermark updates after MERGE | Yes, after successful commit | ✅ |
| Synthetic deduplication key | `user_id|event_type|product_id|event_time` hashed with SHA-256 | ✅ |
| MERGE uses event_key | Yes | ✅ |
| Filters applied before dedup | Yes, business rules are applied before deduplication | ✅ |
| category_code parsing matches expectations | `category_l1`, `category_l2`, `category_l3` | ✅ |

### Deviations from Guide
No material deviations were introduced. The design stays aligned with the implementation guide while making the watermark and business-rule behavior explicit.

### Consistency Check
The design is consistent with the implementation guide: the MERGE match condition uses `event_key`, business-rule filters are applied before deduplication, and the parsed category columns align with the agreed Silver contract.


## Silver Design: Learning Checkpoint

### Q1: Why must the watermark advance only after successful MERGE?
Because advancing too early can cause silent data loss if the MERGE fails after the watermark update. The next run may skip the batch that was never applied.

### Q2: What fields make up the deduplication key and format?
The deduplication key is built from `user_id|event_type|product_id|event_time`, using ISO-8601 UTC formatting for the timestamp and then hashing the concatenated string with SHA-256.

### Q3: Why no update branch in MERGE?
Clickstream events are immutable facts, so there is no need to update an existing row once it has been written to Silver.


