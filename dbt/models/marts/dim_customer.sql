with events as (

    select
        user_id,
        event_time,
        event_date
    from {{ ref('int_events_enriched') }}
    where user_id is not null

),

customer_base as (

    select
        user_id,
        min(event_time) as first_seen_at,
        cast(strftime(cast(min(event_time) as date), '%Y%m%d') as integer) as first_seen_date_key
    from events
    group by user_id

)

select
    md5(cast(user_id as varchar)) as customer_key,
    user_id,
    first_seen_at,
    first_seen_date_key
from customer_base
