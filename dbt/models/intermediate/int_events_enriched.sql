with events as (

    select * from {{ ref('stg_events') }}

),

sessions as (

    select
        event_key,
        session_id,
        user_session_seq
    from {{ ref('int_sessions') }}

),

products as (

    select
        product_id,
        brand as product_brand,
        category_code as product_category_code,
        category_l1,
        category_l2,
        category_l3,
        latest_price as product_latest_price
    from {{ ref('stg_products') }}

),

joined as (

    select
        e.event_key,
        e.user_id,
        s.session_id,
        s.user_session_seq,
        e.user_session as raw_user_session,
        e.event_type,
        e.product_id,
        e.category_id,
        coalesce(e.category_code, p.product_category_code) as category_code,
        coalesce(e.category_l1, p.category_l1) as category_l1,
        coalesce(e.category_l2, p.category_l2) as category_l2,
        coalesce(e.category_l3, p.category_l3) as category_l3,
        coalesce(e.brand, p.product_brand) as brand,
        e.price,
        p.product_latest_price,
        e.event_time,
        e.event_date,
        e.ingested_at,
        e.source_file,
        e.pipeline_run_id,
        e.batch_id
    from events e
    inner join sessions s
        on e.event_key = s.event_key
    left join products p
        on e.product_id = p.product_id

)

select * from joined
