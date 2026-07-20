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