-- Stop visits trustworthy enough to measure lateness with:
--   * finalized_by = 'passed': the vehicle was seen passing the stop, so the final prediction
--     was made right at arrival (median horizon 0 s). 'expired' rows come from trips that
--     vanished from the feed and include assignment glitches (e.g. a single prediction 93 min early).
--   * matched to the schedule and with an observed time, on a regular (not ADDED) trip.
select
    service_date,
    route_id,
    direction_id,
    trip_id,
    stop_sequence,
    scheduled_stop_id as stop_id,
    scheduled_time,
    observed_time,
    delay_seconds,
    scheduled_hour_local as hour_of_day,
    processed_time
from {{ source('silver', 'stop_delays') }}
where finalized_by = 'passed'
  and schedule_matched
  and delay_seconds is not null
  and trip_schedule_relationship = 'SCHEDULED'
  and coalesce(stop_schedule_relationship, 'SCHEDULED') = 'SCHEDULED'
