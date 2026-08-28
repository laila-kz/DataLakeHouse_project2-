with source as (

    select * from {{ source('silver', 'ecommerce_events') }}

),

renamed as (

    select
        event_key,
        user_id,
        user_session,
        event_type,
        product_id,
        category_id,
        category_code,
        category_l1,
        category_l2,
        category_l3,
        brand,
        price,
        event_time,
        event_date,
        ingested_at,
        source_file,
        pipeline_run_id,
        batch_id

    from source

),

deduped as (

    select
        *,
        row_number() over (
            partition by event_key 
            order by ingested_at desc
        ) as rn
    from renamed

)

select
    event_key,
    user_id,
    user_session,
    event_type,
    product_id,
    category_id,
    category_code,
    category_l1,
    category_l2,
    category_l3,
    brand,
    price,
    event_time,
    event_date,
    ingested_at,
    source_file,
    pipeline_run_id,
    batch_id
from deduped
where rn = 1
