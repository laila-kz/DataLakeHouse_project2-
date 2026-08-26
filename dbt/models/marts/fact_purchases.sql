with purchases as (

    select * from {{ ref('int_events_enriched') }}
    where event_type = 'purchase'

),

with_date as (

    select
        e.*,
        d.date_key
    from purchases e
    left join {{ ref('dim_date') }} d
        on cast(e.event_date as date) = d.date_day

),

with_customer as (

    select
        e.*,
        c.customer_key
    from with_date e
    left join {{ ref('dim_customer') }} c
        on e.user_id = c.user_id

),

with_product as (

    select
        e.*,
        p.product_key
    from with_customer e
    left join {{ ref('dim_product') }} p
        on e.product_id = p.product_id
        and e.event_time >= p.valid_from
        and (e.event_time < p.valid_to or p.valid_to is null)

),

final as (

    select
        event_key as purchase_key,
        date_key,
        customer_key,
        product_key,
        product_id,
        user_id,
        session_id,
        price as revenue,
        1 as quantity,
        event_time as purchased_at,
        event_date as purchased_date,
        ingested_at,
        batch_id
    from with_product

)

select * from final
