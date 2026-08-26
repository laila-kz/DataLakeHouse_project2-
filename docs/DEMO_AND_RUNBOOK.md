# 🎬 E-Commerce Data Lakehouse: End-to-End Demo & Execution Runbook

This guide provides a step-by-step script for spinning up the entire project from a complete cold start, running each subsystem, proving reliability & performance features, and recording a portfolio video demo.

---

## 📋 1. Prerequisites & Cold-Start Setup

### Environment Variables (`.env`)
Ensure `.env` exists at the repository root with credentials:

```bash
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://localhost:9000
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_key
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Start Clean Docker Stack
```bash
# 1. Clean up any existing state (optional cold-start wipe)
docker compose down -v

# 2. Build and start all services (Airflow, Spark, MinIO, Metabase)
docker compose up -d --build

# 3. Create required MinIO S3 buckets (raw, bronze, silver, gold, reference, logs)
python scripts/create_buckets.py
```

---

## 🎥 2. Live Demo Script (Step-by-Step Execution Sequence)

### 🛡️ Step 1: Data Contract Enforcement & Raw Quarantine (Shift-Left Governance)

**Demo Point:** Show how the pipeline prevents frontend/upstream schema drift from breaking downstream models.

```bash
# 1A. Validate a CLEAN incoming dataset -> PASSES
python contracts/contract_cli.py \
  --input-file ./data/test_valid.csv \
  --contract-file ./contracts/schemas/ecommerce_events_v1.yml

# 1B. Validate a BREAKING schema dataset -> FAILS, QUARANTINES payload, logs violation report
python contracts/contract_cli.py \
  --input-file ./data/test_breaking.csv \
  --contract-file ./contracts/schemas/ecommerce_events_v1.yml

# Check the quarantine directory and generated JSON violation log
ls -la data/quarantine/
cat logs/contract_violation_*.json
```

---

### 📥 Step 2: Automated Ingestion & PySpark Bronze Layer

**Demo Point:** Fetch clickstream events and write to MinIO Bronze Delta Lake with explicit struct schema enforcement.

```bash
# 2A. Ingest Raw Clickstream Data
python ingestion/kaggle_ingest.py

# 2B. Run PySpark Bronze Transformation
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/*.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark_jobs/bronze_transform.py

# 2C. Run Bronze Soda Quality Gate Scan
docker compose exec spark bash -lc \
  "cd /opt/spark/work-dir && python3 soda/run_soda_scan.py s3a://bronze/ecommerce_events/ soda/checks/bronze_checks.yml soda/configurations/spark_configuration.yml"
```

---

### 🔄 Step 3: Incremental Silver MERGE & Crash-Recovery Proof

**Demo Point:** SHA-256 deduplication and watermark isolation guaranteeing zero data loss or duplication even after process crashes.

```bash
# 3A. Run Silver Transform (Incremental MERGE)
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/*.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark_jobs/silver_transform.py

# 3B. [PROPERTIES FLEX] Crash Simulation & Recovery Proof
# Trigger crash immediately after MERGE execution before watermark update:
docker compose exec spark bash -c \
  "SIMULATE_CRASH_AFTER_MERGE=true /opt/spark/bin/spark-submit --jars /opt/spark/jars-extra/*.jar --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /workspace/spark_jobs/silver_transform.py"

# Re-run recovery — watermark stays intact, MERGE deduplicates via SHA-256 key, zero duplicate rows!
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /opt/spark/jars-extra/*.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark_jobs/silver_transform.py

# 3C. Run Silver Quality Gate
docker compose exec spark python3 soda/run_silver_scan.py
```

---

### 📊 Step 4: dbt Dimensional Modeling & Gold Marts (83/83 Tests)

**Demo Point:** Build 13 dbt models (Staging ➔ Sessionization ➔ Star Schema ➔ Gold Marts) and execute data tests.

```bash
# 4A. Run Full dbt Project (DuckDB backend)
cd dbt
dbt run --profiles-dir .

# 4B. Run Full dbt Test Suite (83 Data Tests & Domain Invariants)
dbt test --profiles-dir .
cd ..
```

---

### ⚙️ Step 5: Full Airflow Orchestration & Automated DAG Execution

**Demo Point:** Show the entire 10-task pipeline executing automatically under Apache Airflow.

1. Open Web Browser to **Airflow Webserver**: [`http://localhost:8080`](http://localhost:8080) (Login: `airflow` / `airflow`).
2. Navigate to DAG: `ecommerce_lakehouse`.
3. Click **Trigger DAG**.
4. Watch all 10 tasks turn green:
   `ingest_raw` ➔ `bronze_transform` ➔ `bronze_quality_gate` ➔ `silver_transform` ➔ `silver_quality_gate` ➔ `dbt_run_staging` ➔ `dbt_run_intermediate` ➔ `dbt_run_dims_facts` ➔ `dbt_run_marts` ➔ `dbt_test_full`.

---

### 🚀 Step 6: PySpark Optimization & Performance Benchmarking

**Demo Point:** Empirically prove distributed computing optimization results.

```bash
# Run Spark Benchmark Suite
python spark_jobs/spark_benchmark.py

# Inspect Generated Markdown Performance Report
cat docs/performance_benchmarks.md
```

---

## 🖥️ 3. Key Visual Elements for Your Video Recording

When recording your video demo (e.g. via Loom or OBS), show these 4 key visuals:

1. **MinIO Object Console** (`http://localhost:9001`):
   - Show `raw/`, `bronze/`, `silver/`, and `data/quarantine/` buckets.
2. **Airflow DAG Graph View** (`http://localhost:8080`):
   - Show the 10 green task nodes executing sequentially with Soda quality gates between transforms.
3. **Terminal Structured JSON Logs**:
   - Show clean JSON formatted logs emitted by `kaggle_ingest.py`, `silver_transform.py`, and `contract_cli.py`.
4. **Performance Benchmark Report** (`docs/performance_benchmarks.md`):
   - Highlight **~3.0x Z-Order speedup** and **~3.2x Broadcast Join speedup**.

---

## 🛠️ 4. Final Documentation Polish Checklist

- [x] **Master Architecture Diagram**: Mermaid diagram present in [`README.md`](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse/README.md).
- [x] **CI/CD Workflow Badge**: GitHub Actions CI workflow in [`.github/workflows/ci.yml`](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse/.github/workflows/ci.yml).
- [x] **Design Decisions Record**: Complete day-by-day technical log in [`docs/design_decisions.md`](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse/docs/design_decisions.md).
- [x] **Performance Benchmarks**: Case study report in [`docs/performance_benchmarks.md`](file:///c:/Users/kheza/Desktop/Data%20Engineering/DataLakehouse/docs/performance_benchmarks.md).
- [x] **Release Tag**: Latest release tagged `v1.2-week9-contracts-benchmarking-complete`.
