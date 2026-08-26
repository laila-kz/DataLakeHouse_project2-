{{
    config(
        materialized='incremental',
        unique_key=['date', 'category_l1'],
        incremental_strategy='delete+insert'
    )
}}

with events as (

    select
        cast(f.event_time as date) as date,
        coalesce(p.category_l1, 'unknown') as category_l1,
        count(*) as total_events,
        count(distinct f.user_id) as active_users
    from {{ ref('fact_events') }} f
    left join {{ ref('dim_product') }} p
        on f.product_key = p.product_key
    {% if is_incremental() %}
    where cast(f.event_time as date) > (select max(date) from {{ this }})
    {% endif %}
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
    {% if is_incremental() %}
    where cast(f.purchased_at as date) > (select max(date) from {{ this }})
    {% endif %}
    group by 1, 2

),

combined as (

    select
        coalesce(e.date, p.date) as date,
        coalesce(e.category_l1, p.category_l1) as category_l1,
        coalesce(e.total_events, 0) as total_events,
        coalesce(p.total_purchases, 0) as total_purchases,
        coalesce(p.total_revenue, 0.0) as total_revenue,
        coalesce(e.active_users, 0) as active_users
    from events e
    full outer join purchases p
        on e.date = p.date
        and e.category_l1 = p.category_l1

)

select * from combined
