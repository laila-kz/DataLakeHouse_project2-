with events as (

    select * from {{ ref('stg_events') }}

),

ranked_products as (

    select
        product_id,
        category_id,
        category_code,
        category_l1,
        category_l2,
        category_l3,
        brand,
        price,
        event_time,
        row_number() over (
            partition by product_id
            order by event_time desc
        ) as rn
    from events

),

deduplicated as (

    select
        product_id,
        category_id,
        category_code,
        category_l1,
        category_l2,
        category_l3,
        brand,
        price as latest_price,
        event_time as last_seen_at
    from ranked_products
    where rn = 1

)

select * from deduplicated
