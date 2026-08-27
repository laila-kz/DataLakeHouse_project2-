.PHONY: help setup init-buckets create-demo-sample test-contracts run-ingest run-ingest-sample run-bronze run-silver run-dbt test-dbt run-benchmarks run-demo clean

help:
	@echo "E-Commerce Data Lakehouse CLI Commands"
	@echo "--------------------------------------"
	@echo "make setup              : Check prerequisites & start Docker stack"
	@echo "make create-demo-sample : Generate lightweight sample dataset (50k rows ~6MB) for fast video demos"
	@echo "make test-contracts     : Run Data Contract validation CLI on valid & breaking test data"
	@echo "make run-ingest-sample  : Ingest demo sample dataset into MinIO raw bucket (super fast)"
	@echo "make run-ingest         : Ingest full dataset into MinIO raw bucket"
	@echo "make run-bronze         : Execute PySpark Bronze layer transform"
	@echo "make run-silver         : Execute PySpark Silver incremental MERGE transform"
	@echo "make run-dbt            : Execute dbt models (staging -> intermediate -> core -> marts)"
	@echo "make test-dbt           : Execute full dbt test suite (83 data tests)"
	@echo "make run-benchmarks     : Run PySpark Optimization & Performance Benchmarking Suite"
	@echo "make run-demo           : Execute full end-to-end demo pipeline in 30 seconds"
	@echo "make clean              : Bring down Docker stack and clean transient data"

setup:
	@echo "--> Checking Docker Compose stack..."
	docker compose up -d --build
	@echo "--> Creating MinIO Buckets..."
	python scripts/create_buckets.py
	@echo "--> Setup complete! Stack is healthy."

init-buckets:
	python scripts/create_buckets.py

create-demo-sample:
	python scripts/create_demo_sample.py

test-contracts:
	@echo "--> Testing Valid Dataset against Contract..."
	python contracts/contract_cli.py --input-file data/test_valid.csv --contract-file contracts/schemas/ecommerce_events_v1.yml
	@echo "--> Testing Breaking Dataset against Contract (Expect Failure & Quarantine)..."
	-python contracts/contract_cli.py --input-file data/test_breaking.csv --contract-file contracts/schemas/ecommerce_events_v1.yml

run-ingest-sample:
	python ingestion/kaggle_ingest.py --data-dir ./data/demo_sample --force

run-ingest:
	python ingestion/kaggle_ingest.py --force

run-bronze:
	docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 2g --executor-memory 2g --jars /opt/spark/jars-extra/*.jar --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /workspace/spark_jobs/bronze_transform.py

run-silver:
	docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 2g --executor-memory 2g --jars /opt/spark/jars-extra/*.jar --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /workspace/spark_jobs/silver_transform.py

run-dbt:
	cd dbt && dbt run --profiles-dir .

test-dbt:
	cd dbt && dbt test --profiles-dir .

run-benchmarks:
	python spark_jobs/spark_benchmark.py

run-demo: create-demo-sample run-ingest-sample run-bronze run-silver run-dbt test-dbt
	@echo "--> 🚀 DEMO PIPELINE EXECUTION COMPLETED SUCCESSFULLY IN SECONDS!"

clean:
	docker compose down -v
