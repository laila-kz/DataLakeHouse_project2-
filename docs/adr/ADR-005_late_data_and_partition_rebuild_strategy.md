# ADR-005: Impacted-Partition Recompute Strategy for Late-Arriving Events

## Status
**Accepted**

## Context
In clickstream pipelines, events often arrive hours or days out of order. When late data arrives in the analytical warehouse:
1. **Naive Incremental Filtering (`where event_date > max(processed_date)`):** Fails to incorporate late events into past historical aggregates (e.g. historical daily revenue remains permanently inaccurate).
2. **Full Table Refresh (`--full-refresh`):** Recomputes all historical dates, incurring quadratic compute costs and locking downstream tables as data volumes grow.

## Decision
We implemented a **Two-Tier Late-Data Handling Strategy**:

1. **Bronze ➔ Silver (Lateness Buffer):**
   - Ingestion from Bronze uses an allowed lookback buffer (`event_time >= watermark - INTERVAL 3 DAYS`).
   - PySpark Delta `MERGE` seamlessly merges late events into their corresponding partition (`event_date`), updating partition stats without full rewrites.
2. **Silver ➔ Gold (`mart_daily_summary`):**
   - The Gold mart dynamically identifies affected calendar dates by filtering for source records ingested after the mart's previous run:
     ```sql
     where date in (
         select distinct cast(event_time as date)
         from {{ ref('fact_events') }}
         where ingested_at > (select coalesce(max(last_recomputed_at), '1970-01-01') from {{ this }})
     )
     ```
   - dbt applies an incremental `delete+insert` strategy scoped **strictly** to the affected `(date, category_l1)` partition keys.

## Alternatives Considered
- **Daily Full Rebuild:** Computationally expensive and unscalable for multi-year historical clickstreams.
- **Sliding Window Recompute (Always recompute last 7 days):** Wastefully recomputes static historical partitions even when no late data arrived for those days.

## Tradeoffs & Consequences
- **Positive:**
  - Guarantees 100% metric accuracy for late-arriving purchases without full table rebuilds.
  - Minimizes compute by isolating writes to only touched `(date, category_l1)` keys.
- **Negative:**
  - Requires tracking `ingested_at` throughout fact tables and `last_recomputed_at` within downstream marts.
