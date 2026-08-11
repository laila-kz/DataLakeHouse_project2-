# Data Lakehouse Project

## 📊 Project Overview
A complete data lakehouse implementation using MinIO (storage), Spark (compute), Airflow (orchestration), and Metabase (BI). Built as a portfolio project to demonstrate modern data engineering practices.

---

## 🏷️ Current Progress

### ✅ Phase 0 Complete (Days 1-3)
- [x] Docker Compose stack with Airflow, Spark, MinIO, Metabase
- [x] All services healthy with healthchecks
- [x] 6 MinIO buckets: raw, bronze, silver, gold, reference, logs
- [x] Spark ↔ MinIO connectivity via S3A connector
- [x] Credentials secured in `.env`

### ✅ Phase 1 Complete (Days 4-7)
- [x] Kaggle API integration with env-var authentication
- [x] Idempotent ingestion with MinIO-based `_SUCCESS` marker
- [x] Structured JSON logging
- [x] Retry with exponential backoff (tenacity)
- [x] Partitioned data storage: `raw/ecommerce_events/ingested_date={date}/`
- [x] End-to-end cold-start validation passed

### 📅 Upcoming (Week 2 - Bronze Layer)
- [ ] First Airflow DAG
- [ ] PySpark transformation for Bronze
- [ ] Data validation before writing
- [ ] Incremental processing

---

## 🚀 Running Locally

### Prerequisites
- Docker Desktop 4.0+
- Python 3.9+
- Git
- MinIO Account (free) for credentials

### Quick Start

**1. Clone the Repository**
```bash
git clone <your-repo-url>
cd DataLakehouse




---

### **Option 2: Quick Test with a Small Data Subset**

If you want to actually test the flow without re-uploading everything:

```powershell
# 1. Create a tiny test dataset (10 rows)
echo "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session" > ./data/test_small.csv
echo "2019-10-01 00:00:00 UTC,view,1003461,2053013555631882655,electronics.smartphone,xiaomi,489.07,520088904,4d3b30da-a5e4-49df-b1a8-ba5943f1dd33" >> ./data/test_small.csv
echo "2019-10-01 00:00:00 UTC,view,5000088,2053013566100866035,appliances.sewing_machine,janome,293.65,530496790,8e5f4f83-366c-4f70-860e-ca7417414283" >> ./data/test_small.csv

# 2. Upload it to MinIO
# Using MinIO console: drag and drop to raw bucket

# 3. Run Bronze transform on this small file
# Update your input path in bronze_transform.py or use --input-path

# 4. Run Soda checks


### ✅ Phase 2 Complete (Days 8-12)
- [x] Explicit Bronze schema with strict types
- [x] Schema-enforced reads with quarantine for malformed rows
- [x] Lineage metadata columns (ingested_at, source_file, pipeline_run_id, batch_id)
- [x] Delta Lake writes with date partitioning
- [x] Time travel and version history proven with DESCRIBE HISTORY
- [x] Soda Core quality gates (schema, nulls, freshness, volume, duplicates)
- [x] Cold-start validation: Ingestion → Bronze → Soda Gate

## 🏷️ Tags
- `v0.1-phase0-complete`: Docker stack + MinIO connectivity
- `v0.2-phase1-complete`: Ingestion service working
- `v0.3-phase2-complete`: Bronze Delta layer + Quality gates

### ✅ Phase 3 Complete (Days 13-18)
- [x] Silver incremental MERGE design (watermark, dedup key, business rules)
- [x] Silver transform implementation (watermark read, filters, dedup key)
- [x] MERGE INTO write with post-merge verification
- [x] Watermark advancement strictly after MERGE success
- [x] Crash-safety proven: zero data loss on mid-job failure
- [x] Silver Soda quality gates (duplicates, business rules, category validity, volume anomaly)
- [x] Cold-start validation: Ingestion → Bronze → Soda → Silver → Soda

## 🔒 Reliability

### Crash-Safety Guarantee

The Silver incremental pipeline is proven crash-safe at its most critical boundary.

**What Was Tested:**
- Simulated a crash **immediately after MERGE completion**, **before** watermark advancement
- This is the exact point where data loss would be most likely if watermarking were incorrectly ordered

**What Was Proven:**
- ✅ **No data loss** — MERGE data was already in Silver (Delta ACID guarantees)
- ✅ **No duplication** — MERGE's `event_key` matching prevented re-insertion
- ✅ **Watermark self-correction** — next run correctly advanced the watermark

**How It Works:**
1. MERGE commits data to Silver atomically
2. `event_key` matching ensures idempotent MERGE operations
3. Watermark is the **last** step, so a crash before it just means the next run reprocesses the range (which is safe)

**Test Evidence:**
- The `SIMULATE_CRASH_AFTER_MERGE` environment variable triggers a controlled crash
- Recovery run shows `num_inserted = 0`, watermark correctly advances
- Full test documented in `docs/design_decisions.md` (Crash Recovery Test section)

> **This is a verifiable claim, not an assumption.** The test can be re-run at any time.

## 🏗️ Architecture (Current State)

┌─────────────────────────────────────────────────────────────────────────┐
│ DATA LAKEHOUSE │
├─────────────────────────────────────────────────────────────────────────┤
│ │
│ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│ │ Raw │ │ Bronze │ │ Silver │ │ Gold │ │
│ │ (CSV) │───▶│ (Delta) │───▶│ (Delta) │───▶│ (Future) │ │
│ │ │ │ │ │ │ │ │ │
│ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
│ │ │ │ │
│ ▼ ▼ ▼ │
│ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│ │Ingestion │ │ Quality │ │ Quality │ │
│ │(Kaggle) │ │ Gate │ │ Gate │ │
│ └──────────┘ │ (Soda) │ │ (Soda) │ │
│ └──────────┘ └──────────┘ │
│ │
│ ✅ Phase 0 ✅ Phase 2 ✅ Phase 3 │
│ ✅ Phase 1 (Bronze) (Silver) │
│ │
│ 🔒 Reliability: Crash-safety proven at MERGE/watermark boundary │
└─────────────────────────────────────────────────────────────────────────┘

## 🚀 Running the Full Pipeline

### Full Chain (Ingestion → Bronze → Quality → Silver → Quality)

```bash
# 1. Start the stack
docker compose up -d

# 2. Create buckets
python scripts/create_buckets.py

# 3. Run ingestion
python ingestion/kaggle_ingest.py

# 4. Run Bronze transform
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/*.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  spark_jobs/bronze_transform.py

# 5. Run Bronze Soda checks
docker compose exec spark bash -lc "cd /opt/spark/work-dir && python3 soda/run_soda_scan.py s3a://bronze/ecommerce_events/ soda/checks/bronze_checks.yml soda/configurations/spark_configuration.yml"

# 6. Run Silver transform
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/*.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  spark_jobs/silver_transform.py

# 7. Run Silver Soda checks
docker compose exec spark python3 soda/run_silver_scan.py


Crash-Safety Test (Optional)
bash
# Run with crash simulation
docker compose exec spark bash -c "SIMULATE_CRASH_AFTER_MERGE=true /opt/spark/bin/spark-submit --jars /opt/spark/jars-extra/*.jar --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog spark_jobs/silver_transform.py"

# Run recovery
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/*.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  spark_jobs/silver_transform.py