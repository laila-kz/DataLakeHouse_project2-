with raw_events as (

    select
        product_id,
        brand,
        category_code,
        category_l1,
        category_l2,
        category_l3,
        price,
        event_time
    from {{ ref('int_events_enriched') }}
    where product_id is not null

),

detect_changes as (

    select
        *,
        case
            when lag(event_time) over (partition by product_id order by event_time) is null then 1
            when price != lag(price) over (partition by product_id order by event_time) then 1
            when coalesce(category_code, '') != coalesce(lag(category_code) over (partition by product_id order by event_time), '') then 1
            else 0
        end as is_new_version
    from raw_events

),

version_indexing as (

    select
        *,
        sum(is_new_version) over (
            partition by product_id
            order by event_time
            rows between unbounded preceding and current row
        ) as product_version
    from detect_changes

),

version_boundaries as (

    select
        product_id,
        product_version,
        min(event_time) as valid_from,
        arg_max(brand, event_time) as brand,
        arg_max(category_code, event_time) as category_code,
        arg_max(category_l1, event_time) as category_l1,
        arg_max(category_l2, event_time) as category_l2,
        arg_max(category_l3, event_time) as category_l3,
        arg_max(price, event_time) as price
    from version_indexing
    group by product_id, product_version

),

final_history as (

    select
        md5(cast(product_id as varchar) || '_' || cast(valid_from as varchar)) as product_key,
        product_id,
        product_version,
        brand,
        category_code,
        category_l1,
        category_l2,
        category_l3,
        price,
        valid_from,
        lead(valid_from) over (
            partition by product_id order by valid_from
        ) as valid_to,
        case
            when lead(valid_from) over (partition by product_id order by valid_from) is null then true
            else false
        end as is_current
    from version_boundaries

)

select * from final_history
