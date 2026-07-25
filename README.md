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