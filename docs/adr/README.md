# Architecture Decision Records (ADRs)

This directory documents the core architectural decisions, design tradeoffs, and technical rationale for the E-Commerce Lakehouse Platform.

| ADR # | Title | Status | Primary Focus |
|---|---|---|---|
| [ADR-001](ADR-001_minio_and_delta_lake_local_storage.md) | MinIO S3 Object Storage & Delta Lake for Local Storage | Accepted | Storage Layer & ACID Transactions |
| [ADR-002](ADR-002_hybrid_event_time_watermarking_and_lateness_buffer.md) | Hybrid Event-Time Watermarking & Lateness Lookback | Accepted | Incremental Ingestion & Late Data |
| [ADR-003](ADR-003_sha256_synthetic_event_keys.md) | Deterministic SHA-256 Composite Keys for Deduplication | Accepted | Idempotency & Data Modeling |
| [ADR-004](ADR-004_dbt_on_duckdb_for_gold_layer.md) | dbt-on-DuckDB for Dimensional Modeling and Gold Marts | Accepted | Analytics Engineering & Testing |
| [ADR-005](ADR-005_late_data_and_partition_rebuild_strategy.md) | Impacted-Partition Recompute for Late-Arriving Events | Accepted | Gold Mart Incremental Strategy |
| [ADR-006](ADR-006_openlineage_and_marquez_for_metadata_governance.md) | OpenLineage Standard & Marquez for Metadata Governance | Accepted | Lineage, Governance & Incident Triage |
