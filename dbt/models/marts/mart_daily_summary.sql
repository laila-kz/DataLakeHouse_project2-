{{
    config(
        materialized='incremental',
        unique_key=['date', 'category_l1'],
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

{% if is_incremental() %}

with watermark as (
    select coalesce(max(last_recomputed_at), cast('1970-01-01' as timestamp)) as max_ts
    from {{ this }}
),

affected_dates as (
    select distinct cast(event_time as date) as affected_date
    from {{ ref('fact_events') }}, watermark
    where ingested_at > watermark.max_ts
    union
    select distinct cast(purchased_at as date) as affected_date
    from {{ ref('fact_purchases') }}, watermark
    where ingested_at > watermark.max_ts
),

{% else %}

with affected_dates as (
    select distinct cast(event_time as date) as affected_date
    from {{ ref('fact_events') }}
),

{% endif %}

events as (

    select
        cast(f.event_time as date) as date,
        coalesce(p.category_l1, 'unknown') as category_l1,
        count(*) as total_events,
        count(distinct f.user_id) as active_users
    from {{ ref('fact_events') }} f
    left join {{ ref('dim_product') }} p
        on f.product_key = p.product_key
    where cast(f.event_time as date) in (select affected_date from affected_dates)
    group by 1, 2

),

purchases as (

    select
        cast(f.purchased_at as date) as date,
        coalesce(p.category_l1, 'unknown') as category_l1,
        count(*) as total_purchases,
        coalesce(sum(f.revenue), 0.0) as total_revenue
    from {{ ref('fact_purchases') }} f
    left join {{ ref('dim_product') }} p
        on f.product_key = p.product_key
    where cast(f.purchased_at as date) in (select affected_date from affected_dates)
    group by 1, 2

),

combined as (

    select
        coalesce(e.date, p.date) as date,
        coalesce(e.category_l1, p.category_l1) as category_l1,
        coalesce(e.total_events, 0) as total_events,
        coalesce(p.total_purchases, 0) as total_purchases,
        coalesce(p.total_revenue, 0.0) as total_revenue,
        coalesce(e.active_users, 0) as active_users,
        current_timestamp as last_recomputed_at
    from events e
    full outer join purchases p
        on e.date = p.date
        and e.category_l1 = p.category_l1

)

select * from combined
