{{ config(materialized='table') }}

-- Routes that are late across a rolling window, not just on one bad trip.
-- Built from the small daily gold table (not silver), so a full rebuild each run is cheap.
-- Chronic = enough observations AND (average delay >= chronic_avg_delay_s
--           OR share of visits > 5 min late >= chronic_share_late_5min).

with window_rows as (
    select *
    from {{ ref('route_lateness_by_hour') }}
    where service_date >= date_sub(current_date(), {{ var('chronic_window_days') }})
),

per_route as (
    select
        route_id,
        sum(n_stop_visits)                                            as n_stop_visits,
        count(distinct service_date)                                  as days_observed,
        round(sum(avg_delay_s * n_stop_visits) / sum(n_stop_visits), 1)    as avg_delay_s,
        round(sum(share_late_5min * n_stop_visits) / sum(n_stop_visits), 4) as share_late_5min,
        max_by(hour_of_day, avg_delay_s)                              as worst_hour_of_day,
        max(avg_delay_s)                                              as worst_hour_avg_delay_s
    from window_rows
    group by route_id
)

select
    p.route_id,
    r.route_name,
    r.mode,
    p.n_stop_visits,
    p.days_observed,
    p.avg_delay_s,
    p.share_late_5min,
    p.worst_hour_of_day,
    p.worst_hour_avg_delay_s,
    rank() over (order by p.avg_delay_s desc)                         as lateness_rank,
    current_timestamp()                                               as refreshed_at
from per_route p
left join {{ ref('dim_routes') }} r on r.route_id = p.route_id
where p.n_stop_visits >= {{ var('chronic_min_visits') }}
  and (p.avg_delay_s >= {{ var('chronic_avg_delay_s') }}
       or p.share_late_5min >= {{ var('chronic_share_late_5min') }})
