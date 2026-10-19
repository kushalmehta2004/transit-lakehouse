{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['service_date'],
    )
}}

-- Delay per route, per service day, per scheduled hour of day (agency local time).
-- Incremental: each run re-aggregates only the trailing `reprocess_days` service days
-- (silver keeps adding rows for a day as stops are finalized) and overwrites those
-- service_date partitions wholesale. Not MERGE: a merge only upserts groups present in the
-- new aggregate, so a group whose silver rows were deleted (e.g. a removed bad row) would
-- linger in gold forever; overwriting the partition drops it.
-- Daily grain keeps percentiles exact per day; roll up across days in consumers.

with visits as (
    select * from {{ ref('stg_stop_delays') }}
    {% if is_incremental() %}
    where service_date >= (
        select date_sub(coalesce(max(service_date), date'1970-01-01'), {{ var('reprocess_days') }}) from {{ this }}
    )
    {% endif %}
)

select
    route_id,
    service_date,
    hour_of_day,
    count(*)                                                      as n_stop_visits,
    count(distinct trip_id)                                       as n_trips,
    round(avg(delay_seconds), 1)                                  as avg_delay_s,
    percentile_approx(delay_seconds, 0.5)                         as median_delay_s,
    percentile_approx(delay_seconds, 0.9)                         as p90_delay_s,
    round(avg(case when delay_seconds > 300 then 1.0 else 0.0 end), 4) as share_late_5min,
    round(avg(case when delay_seconds between -60 and 300 then 1.0 else 0.0 end), 4) as share_on_time,
    current_timestamp()                                           as refreshed_at
from visits
group by route_id, service_date, hour_of_day
