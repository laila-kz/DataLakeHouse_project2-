.PHONY: help setup init-buckets test-contracts run-ingest run-bronze run-silver run-dbt test-dbt run-benchmarks clean

help:
	@echo "E-Commerce Data Lakehouse CLI Commands"
	@echo "--------------------------------------"
	@echo "make setup          : Check prerequisites & start Docker stack"
	@echo "make init-buckets   : Initialize MinIO S3 buckets"
	@echo "make test-contracts : Run Data Contract validation CLI on valid & breaking test data"
	@echo "make run-ingest     : Run raw clickstream ingestion script"
	@echo "make run-bronze     : Execute PySpark Bronze layer transform"
	@echo "make run-silver     : Execute PySpark Silver incremental MERGE transform"
	@echo "make run-dbt        : Execute dbt models (staging -> intermediate -> core -> marts)"
	@echo "make test-dbt       : Execute full dbt test suite (83 data tests)"
	@echo "make run-benchmarks : Run PySpark Optimization & Performance Benchmarking Suite"
	@echo "make clean          : Bring down Docker stack and clean transient data"

setup:
	@echo "--> Checking Docker Compose stack..."
	docker compose up -d --build
	@echo "--> Creating MinIO Buckets..."
	python scripts/create_buckets.py
	@echo "--> Setup complete! Stack is healthy."

init-buckets:
	python scripts/create_buckets.py

test-contracts:
	@echo "--> Testing Valid Dataset against Contract..."
	python contracts/contract_cli.py --input-file data/test_valid.csv --contract-file contracts/schemas/ecommerce_events_v1.yml
	@echo "--> Testing Breaking Dataset against Contract (Expect Failure & Quarantine)..."
	-python contracts/contract_cli.py --input-file data/test_breaking.csv --contract-file contracts/schemas/ecommerce_events_v1.yml

run-ingest:
	python ingestion/kaggle_ingest.py

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

clean:
	docker compose down -v
