# ADR-006: OpenLineage Standard & Marquez for Metadata Governance and Operational Lineage

## Status
**Accepted**

## Context
A major operational blind spot in multi-engine Lakehouse environments (Spark for Bronze/Silver + dbt/DuckDB for Gold + Airflow for Orchestration) is fragmented metadata. Without unified lineage tracking:
- There is no single pane of glass showing data flow from raw files to final BI marts.
- Schema drift and column-level mutations across engine boundaries go untracked.
- Debugging pipeline incidents requires manually correlating Airflow task logs, Delta table version logs, and dbt manifest files.

## Decision
We adopted the **Linux Foundation OpenLineage Standard** with **Marquez** as the centralized metadata and lineage repository:
- **Marquez Deployment:** Runs as a lightweight service in `docker-compose.yml` (`marquezproject/marquez`), exposing standard OpenLineage REST APIs and a visual Web UI.
- **Spark Instrumentation:** PySpark jobs emit run-level facets, input/output Delta dataset URIs, schemas, and row-count metrics via `OpenLineageSparkListener`.
- **Airflow & dbt Lineage:** Airflow's OpenLineage transport (`AIRFLOW__OPENLINEAGE__TRANSPORT`) automatically captures task lifecycle states and dbt model-level dependencies.
- **Incident Blast Radius Analysis:** When contract violations occur, Marquez visualizes the exact downstream models and consumers impacted by the quarantine.

## Alternatives Considered
- **Custom Metadata Logging to SQLite / Postgres:** Low engineering signal; reinvents lineage serialization formats without adhering to open standards.
- **DataHub / Apache Atlas:** Highly capable, but has significantly heavier infra overhead (Elasticsearch, Kafka, Neo4j) compared to Marquez for local execution.
- **Static dbt Docs (`dbt docs generate`):** Only captures dbt SQL lineage, completely blind to Spark jobs, MinIO files, and upstream ingestion tasks.

## Tradeoffs & Consequences
- **Positive:**
  - Standardized, vendor-neutral OpenLineage JSON spec.
  - Automatic collection of dataset schemas, job run durations, and input/output dependencies across both Spark and dbt.
  - Clear visual interface for incident root-cause and blast-radius triage.
- **Negative:**
  - Requires adding the Marquez container to Docker Compose and adding OpenLineage listener configuration to Spark jobs.
