with events as (

    select * from {{ ref('stg_events') }}

),

events_with_lag as (

    select
        event_key,
        user_id,
        event_type,
        product_id,
        price,
        event_time,
        user_session as raw_user_session,
        lag(event_time) over (
            partition by user_id
            order by event_time, event_key
        ) as previous_event_time
    from events

),

session_flags as (

    select
        *,
        case
            when previous_event_time is null then 1
            when (epoch(event_time) - epoch(previous_event_time)) > 1800 then 1
            else 0
        end as is_new_session
    from events_with_lag

),

session_indexing as (

    select
        *,
        sum(is_new_session) over (
            partition by user_id
            order by event_time, event_key
            rows between unbounded preceding and current row
        ) as user_session_seq
    from session_flags

),

final as (

    select
        event_key,
        user_id,
        concat(cast(user_id as varchar), '_', cast(user_session_seq as varchar)) as session_id,
        user_session_seq,
        event_type,
        product_id,
        price,
        event_time,
        previous_event_time,
        raw_user_session
    from session_indexing

)

select * from final
