# ADR-003: Deterministic SHA-256 Composite Keys for Event Deduplication

## Status
**Accepted**

## Context
Raw e-commerce clickstream datasets (such as the Kaggle 2019-Oct dataset) lack a globally unique event identifier per row. Upstream tracking clients frequently produce duplicate events due to network retries, client-side resends, and parallel worker exports.

To enforce idempotency during Delta `MERGE` and enable row-level traceability across downstream dimensional models (facts, dimensions, marts), each event requires a deterministic primary key that can be independently recomputed without distributed coordination.

## Decision
We synthesize a deterministic primary key (`event_key`) computed as a SHA-256 cryptographic hash of four business dimensions normalized to UTC:

```python
sha2(
    concat_ws(
        "|",
        col("user_id").cast("string"),
        col("event_type"),
        col("product_id").cast("string"),
        date_format(to_utc_timestamp(col("event_time"), "UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ),
    256
)
```

## Alternatives Considered
- **UUID / Auto-incrementing Integers:** Non-deterministic. Running the same raw batch twice generates different keys, breaking idempotent `MERGE` and creating duplicate records.
- **MD5 Hashing:** Faster, but has a higher theoretical collision probability and is avoided in modern security-compliant environments.
- **Natural Composite Primary Key `(user_id, event_time, product_id, event_type)`:** Joining and merging on 4 separate columns increases join complexity, index size, and dbt query verbosity.

## Tradeoffs & Consequences
- **Positive:**
  - Guaranteed idempotency: Identical source events always produce the identical 64-character hex key.
  - High performance: Single-column hash matching in Delta `MERGE` (`target.event_key = source.event_key`) and dbt joins.
  - Consistent lineage: The same `event_key` propagates from Silver staging all the way to `fact_events` and `fact_purchases`.
- **Negative:**
  - SHA-256 strings occupy 64 bytes per row, resulting in higher storage overhead than integer IDs.
  - Two genuine purchases by the exact same user for the exact same product in the exact same second would be deduplicated as one event (an acceptable domain tradeoff for web clickstreams).
