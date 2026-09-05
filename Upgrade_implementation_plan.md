# E-Commerce Lakehouse → Upgrade Implementation Plan

**Goal:** move the project from "I used the standard DE stack correctly" (already true, Phases 0–9 prove it) to "I engineered an enterprise-grade Data Platform with standardized metadata observability, OpenLineage governance, cross-layer reconciliation, and self-healing backfills" — without re-adding Kafka/streaming, since that ground is already covered by your other portfolio project.

Everything below is filtered for: (a) highest resume signal for Senior Data Engineering & Data Platform roles, (b) enterprise open-source standards (**OpenLineage + Marquez**), (c) zero generic BI dashboards, and (d) airtight watermark, contract, and backfill execution.

---

## How the plan is structured

| Tier | Theme | Why |
|---|---|---|
| **Tier 1** | Make the *existing* pipeline trustworthy | Cheapest, highest credibility-per-hour. Fixes silent data drop bugs, reconciliation, and failure recovery. |
| **Tier 2** | Add platform-grade capabilities | Replay/backfill system with watermark isolation & schema contract evolution registry. |
| **Tier 3** | Enterprise Governance & OpenLineage Control Plane | Industry-standard **OpenLineage + Marquez** layer capturing automated lineage, dataset facets, job run lifecycle, and schema evolution across Spark, Delta Lake, and dbt. |
| **Skip / optional** | Generic BI dashboards & extra marts | Low DE signal, distracts from platform-level engineering. |

---

## Tier 1 — Make it trustworthy (do all of these)

| # | Item | What it actually means | Effort |
|---|---|---|---|
| 1.1 | **Rewrite the README** | Single current-state phase list (delete duplicated old Phase 4/5 sections), fix the local `file:///C:/...` link, replace "production-ready" with "production-patterned local implementation," document that the Airflow DAG uses `demo_sample`, correct the dbt model path claim (`models/marts`, not `core`), add OS-specific (PowerShell/Linux) commands | Low (1 doc) |
| 1.2 | **Cross-layer reconciliation checks** | A dedicated Python runner (`checks/run_reconciliation.py`) querying MinIO Delta + DuckDB to assert: `raw ≥ bronze_valid + bronze_quarantined`, `bronze ≥ silver + filtered`, `silver events = fact_events`, `mart revenue = sum(fact_purchases.revenue)`. Persist results to a `reconciliation_results` table | Medium |
| 1.3 | **Platform observability metrics** | One `pipeline_metrics` table populated per run: input/output row counts, quarantined count, duplicate count, null %, min/max event time, processing duration, throughput (rows/sec), watermark lag (freshness), late-event count | Medium |
| 1.4 | **Formalize failure-injection into a real test matrix** | Extend `SIMULATE_CRASH_AFTER_MERGE` into a documented test matrix — `FAIL_AFTER_INGEST`, `FAIL_AFTER_BRONZE_WRITE`, `FAIL_AFTER_SILVER_MERGE`, `FAIL_BEFORE_WATERMARK_UPDATE`, `FAIL_DURING_DBT` — with expected behavior and a verified pass/fail table in the README | Medium |
| 1.5 | **Late-arriving data handling (End-to-End)** | **1. Silver Ingestion:** Add an allowed lateness lookback buffer or ingestion watermark so `silver_transform.py` does not drop late Bronze rows.<br>**2. Gold Marts:** Update `mart_daily_summary` to detect affected dates via `ingested_at > (select max(last_run_at) from {{ this }})` and execute partition-level `delete+insert` | Medium |
| 1.6 | **3–5 ADRs** | Short docs: why MinIO+Delta locally, why hybrid event-time + ingestion-time watermarking, why SHA-256 event keys, why dbt-on-DuckDB, why OpenLineage standard with Marquez. Format: Context / Decision / Alternatives / Tradeoffs / Consequences | Low |

---

## Tier 2 — Add real platform differentiation (do both)

| # | Item | What it means | Effort | Recommended? |
|---|---|---|---|---|
| 2.1 | **Replay & backfill system with watermark isolation** | `replay_events.py --source --start-date --end-date --mode backfill --dry-run`. Crucial fix: `--mode backfill` disables `event_time > watermark` filtering and bypasses `advance_watermark()` so historical reprocessing never corrupts or regresses production watermarks. Logs to `replay_audit` table | High | **Yes — Primary Differentiator** |
| 2.2 | **Contract versioning & compatibility registry** | Classify schema changes as Compatible / Warning / Breaking (e.g. adding nullable column = compatible, type change/column drop = breaking). Version contracts (`_v1.yml`, `_v2.yml`). Ingest-gate routes breaking rows to `s3://quarantine/` and updates a schema registry table | Medium | **Yes — Primary Differentiator** |
| 2.3 | Additional analytics marts (funnels / retention) | Standard BI marts | Medium | Optional / Skip (low platform signal) |

---

## Tier 3 — Enterprise Governance: OpenLineage + Marquez Control Plane

Instead of a generic BI dashboard or toy frontend, integrate the **Linux Foundation OpenLineage Standard** with a **Marquez** metadata server in Docker.

### Core Implementation:
1. **Marquez Metadata Platform (Dockerized):**
   - Add `marquez` and `marquez-web` to `docker-compose.yml` to store and visualize dataset lineage, job run lifecycles, and column-level facets.
2. **Spark OpenLineage Instrumentation:**
   - Configure Spark with the OpenLineage Spark listener (`OpenLineageSparkListener`) to automatically emit input/output datasets, Delta table schemas, and job run durations for Bronze and Silver transforms to Marquez.
3. **dbt & Airflow OpenLineage Emission:**
   - Configure `openlineage-dbt` / Airflow OpenLineage provider (`AIRFLOW__OPENLINEAGE__TRANSPORT`) to trace dbt staging/intermediate/marts DAG lineage directly into Marquez.
4. **End-to-End Governance & Incident Walkthrough:**
   - Marquez UI visualizes complete end-to-end data flow: `Raw Files` ➔ `Bronze Delta` ➔ `Silver Delta` ➔ `Gold Marts`.
   - Demonstrates schema evolution facets, dataset versioning, and job run health.
   - Incident scenario: A simulated breaking batch fails the contract gate, emitting a failed job event to Marquez, highlighting impacted downstream datasets in the lineage graph. Replay remediates and updates Marquez run state to success.

---

## Suggested build order

1. **Stage 1 — Baseline & Docs:** README rewrite + ADR-001 to ADR-004 (1.1, 1.6).
2. **Stage 2 — Trust & Correctness:**
   - Silver lateness buffer + `mart_daily_summary` incremental partition recompute (1.5).
   - Cross-layer reconciliation runner (`checks/run_reconciliation.py`) + `pipeline_metrics` (1.2, 1.3).
   - Formal failure-injection matrix (1.4) + write ADR-005.
3. **Stage 3 — Platform Capabilities:**
   - Replay & backfill engine with watermark isolation (`replay_events.py` + `replay_audit`) (2.1).
   - Contract compatibility classifier (`contract_cli.py` + versioned registry + quarantine routing) (2.2).
4. **Stage 4 — OpenLineage + Marquez Integration & Demo:**
   - Spin up Marquez in `docker-compose.yml`.
   - Instrument Spark, dbt, and Airflow with OpenLineage listeners (Tier 3).
   - Record a 90-second platform walkthrough showcasing automated lineage graph, dataset facets, and incident recovery in Marquez.

---

## One-line resume bullets this plan unlocks

- *"Architected automated data lineage and metadata observability using the OpenLineage standard and Marquez across Spark, Delta Lake, and dbt."*
- *"Built cross-layer reconciliation controls across MinIO Delta Lake and DuckDB, detecting row-count and revenue drift with zero false positives."*
- *"Designed an idempotent replay and backfill engine with watermark isolation, preventing historical backfills from regressing production watermarks."*
- *"Extended a YAML data-contract engine with compatibility classification (breaking/compatible/warning) and automated dead-letter quarantine routing."*
- *"Implemented partition-level recomputation for late-arriving events across Silver and Gold layers, eliminating stale incremental aggregations."*