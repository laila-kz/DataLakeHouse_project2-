# Week 5 ER Diagram — Dimensional Model

```mermaid
erDiagram
    dim_date {
        int date_key PK
        date date_day
        int year
        int quarter
        int month
        int week
        int day_of_week
        boolean is_weekend
    }

    dim_customer {
        varchar customer_key PK
        varchar user_id
        timestamp first_seen_at
        int first_seen_date_key
    }

    dim_product {
        varchar product_key PK
        varchar product_id
        decimal price
        varchar category_l1
        varchar category_l2
        varchar category_l3
        varchar brand
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }

    fact_events {
        varchar event_key PK
        int date_key FK
        varchar customer_key FK
        varchar product_key FK
        varchar product_id
        varchar user_id
        varchar session_id
        int user_session_seq
        varchar event_type
        decimal price
        timestamp event_time
    }

    fact_purchases {
        varchar purchase_key PK
        int date_key FK
        varchar customer_key FK
        varchar product_key FK
        varchar product_id
        varchar user_id
        varchar session_id
        decimal revenue
        int quantity
        timestamp purchased_at
    }

    dim_date ||--o{ fact_events : "date_key"
    dim_customer ||--o{ fact_events : "customer_key"
    dim_product ||--o{ fact_events : "product_key (time-ranged)"

    dim_date ||--o{ fact_purchases : "date_key"
    dim_customer ||--o{ fact_purchases : "customer_key"
    dim_product ||--o{ fact_purchases : "product_key (time-ranged)"
```

## Notes

- `dim_product` uses **Type 2 SCD** — `product_key` is a surrogate; fact tables join via
  `fact.event_time >= dim_product.valid_from AND (fact.event_time < dim_product.valid_to OR dim_product.valid_to IS NULL)`
- `dim_customer` is **Type 1** (thin dimension, no history tracking).
- `dim_date` covers 2019-01-01 → 2027-12-31 to accommodate all event timestamps.
- `fact_events` grain: 1 row per event (all event types).
- `fact_purchases` grain: 1 row per purchase event (`event_type = 'purchase'`). Revenue = price at event time.
