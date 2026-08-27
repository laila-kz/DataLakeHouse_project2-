# 🎬 E-Commerce Data Lakehouse: Complete Video Demo Runbook

This guide provides an exact, step-by-step master script for recording a portfolio video demo from a complete cold start. It uses **Fast Demo Mode** (lightweight sample dataset) so the entire end-to-end pipeline runs smoothly in seconds on your local laptop without CPU/RAM bottlenecks.

---

## ⚡ Quick Reference Execution Summary

```bash
# 1. Start Fresh Environment
make setup

# 2. Step 1: Shift-Left Data Contracts Validation & Quarantine
make test-contracts

# 3. Step 2: Generate Demo Dataset & Ingest to MinIO
python scripts/create_demo_sample.py
python ingestion/kaggle_ingest.py --data-dir ./data/demo_sample --force

# 4. Step 3: PySpark Bronze & Silver MERGE Transforms
make run-bronze
make run-silver

# 5. Step 4: dbt Models & Full Test Suite (83 Data Tests)
make run-dbt
make test-dbt

# 6. Step 5: PySpark Optimization & Performance Benchmarks
make run-benchmarks
```

---

## 📋 1. Cold-Start Setup & Prerequisites

### Check `.env` Configuration
Ensure `.env` exists at project root:
```env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://localhost:9000
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_key
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Cold-Start Reset
```bash
# Clean previous state and initialize Docker services + MinIO S3 buckets
make clean
make setup
```

---

## 🎥 2. Master Live Video Demo Script

### 🛡️ Step 1: Shift-Left Data Contracts & Schema Quarantine

**Voiceover / Talking Point:** *"Before any data enters our lakehouse, we enforce strict shift-left Data Contracts using a custom CLI engine (`contract_cli.py`) to prevent upstream schema drift from breaking downstream models."*

```bash
# 1A. Validate a CLEAN incoming dataset -> PASSES
python contracts/contract_cli.py \
  --input-file data/test_valid.csv \
  --contract-file contracts/schemas/ecommerce_events_v1.yml

# 1B. Validate a BREAKING schema dataset -> FAILS, QUARANTINES payload, logs violation report
python contracts/contract_cli.py \
  --input-file data/test_breaking.csv \
  --contract-file contracts/schemas/ecommerce_events_v1.yml
```

**Visuals to show on screen:**
- Terminal output showing `[SUCCESS]` vs `[FAILED]`.
- Quarantined file in `data/quarantine/` and generated violation log in `logs/contract_violation_*.json`.

---

### 📥 Step 2: Demo Dataset Generation & MinIO Raw Ingestion

**Voiceover / Talking Point:** *"We extract a clean 50k-row clickstream sample for fast demo execution, ingesting raw events into MinIO S3 raw bucket (`s3a://raw/ecommerce_events/`)."*

```bash
# 2A. Generate 6MB Demo Dataset
python scripts/create_demo_sample.py

# 2B. Ingest Demo Sample to MinIO Raw Bucket
python ingestion/kaggle_ingest.py --data-dir ./data/demo_sample --force
```

**Visuals to show on screen:**
- MinIO Object Browser (`http://localhost:9001`) showing raw files uploaded to `s3a://raw/ecommerce_events/`.

---

### 🥉 Step 3: PySpark Bronze & Incremental Silver MERGE Layer

**Voiceover / Talking Point:** *"PySpark parses raw events into a schema-enforced Bronze Delta Lake table. Then, our Silver layer performs incremental MERGE using SHA-256 event keys and watermark isolation."*

```bash
# 3A. Run Bronze PySpark Transformation
make run-bronze

# 3B. Run Silver Incremental MERGE Transformation
make run-silver

# 3C. [PROPERTIES FLEX] Crash Simulation & Recovery Proof (Optional)
# Inject crash after MERGE commit before watermark update:
docker compose exec spark bash -c "SIMULATE_CRASH_AFTER_MERGE=true /opt/spark/bin/spark-submit --driver-memory 2g --executor-memory 2g --jars /opt/spark/jars-extra/*.jar --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /workspace/spark_jobs/silver_transform.py"

# Run recovery — watermark re-reads safely, MERGE deduplicates via SHA-256 key with 0 duplicate rows!
make run-silver
```

**Visuals to show on screen:**
- Terminal JSON logs showing batch processing, MERGE execution, and watermark advancement.

---

### 📊 Step 4: dbt Dimensional Warehouse & Test Suite (83/83 PASS)

**Voiceover / Talking Point:** *"dbt transforms Silver events into a star schema (`dim_date`, `dim_customer`, `dim_product` SCD2, `fact_events`, `fact_purchases`) and Gold business marts (`mart_daily_summary`, `mart_customer_retention`)."*

```bash
# 4A. Run dbt Transformations
make run-dbt

# 4B. Run Full dbt Test Suite (83 Data Tests & Domain Invariants)
make test-dbt
```

**Visuals to show on screen:**
- Terminal green test summary showing `PASS=83 WARN=0 ERROR=0 TOTAL=83`.

---

### ⚙️ Step 5: Apache Airflow DAG Orchestration

**Voiceover / Talking Point:** *"The entire 10-step pipeline operates as a scheduled Airflow DAG (`ecommerce_lakehouse`) with quality-gate halting semantics and automatic retries."*

1. Open Browser to Airflow UI: [`http://localhost:8080`](http://localhost:8080) (Login: `airflow` / `airflow`).
2. Trigger `ecommerce_lakehouse` DAG.
3. Watch the graph view turn green across all 10 tasks!

---

### 🚀 Step 6: PySpark Performance Benchmarking

**Voiceover / Talking Point:** *"We benchmark our lakehouse optimizations, empirically proving ~3.0x speedups via Delta Z-Ordering and ~3.2x speedups via Broadcast Hash Joins."*

```bash
# Run Optimization Benchmark Suite
make run-benchmarks
```

**Visuals to show on screen:**
- Open [`docs/performance_benchmarks.md`](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse/docs/performance_benchmarks.md) showing the formatted benchmark report tables!

---

## 🎙️ 3. Recommended 90-Second Video Voiceover Script

> *"Hi everyone, welcome to the demo of my E-Commerce Behavioral Analytics Data Lakehouse.
> 
> Before data enters the pipeline, our custom Data Contract engine validates incoming raw clickstream batches against explicit YAML contracts, automatically quarantining breaking schema changes to S3 quarantine prefixes with Slack diff alerts.
> 
> The architecture follows a Medallion pattern: raw CSVs are parsed by PySpark into a Delta Lake Bronze layer with explicit struct schema enforcement and Soda Core quality gates. Our Silver layer executes incremental MERGE using SHA-256 deduplication keys and watermark tracking—proven to recover cleanly from process crashes with zero duplicate rows.
> 
> On the warehouse side, dbt builds a star schema featuring 30-minute idle sessionization, Type 2 Slowly Changing Dimensions (SCD2) for product price history, and Gold business marts for daily category rollups and cohort retention. All 83 dbt data tests pass cleanly.
> 
> The entire 10-step pipeline is orchestrated by Apache Airflow with quality-gate exit code halting, dual failure alerting, and GitHub Actions CI/CD. Finally, our Spark benchmarking suite demonstrates ~3.0x query speedups using Delta Z-Ordering and ~3.2x speedups via Broadcast Joins.
> 
> Thanks for watching!"*
