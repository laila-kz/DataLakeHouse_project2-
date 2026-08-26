with enriched as (

    select * from {{ ref('int_events_enriched') }}

),

with_date as (

    select
        e.*,
        d.date_key
    from enriched e
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
        event_key,
        session_id,
        user_session_seq,
        date_key,
        customer_key,
        product_key,
        product_id,
        user_id,
        event_type,
        price,
        event_time,
        raw_user_session,
        ingested_at,
        batch_id
    from with_product

)

select * from final
