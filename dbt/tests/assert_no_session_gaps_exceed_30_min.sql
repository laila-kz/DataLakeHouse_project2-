-- Singular test: Assert that no event-to-event gap within the same session exceeds 30 minutes (1800 seconds)
select
    session_id,
    user_id,
    event_key,
    event_time,
    previous_event_time,
    (epoch(event_time) - epoch(previous_event_time)) as gap_seconds
from {{ ref('int_sessions') }}
where previous_event_time is not null
  and (epoch(event_time) - epoch(previous_event_time)) > 1800
