{{ config(materialized='table') }}

-- Route names/types from the static GTFS schedule (reference parquet, refreshed by the poller).
select
    route_id,
    route_short_name,
    route_long_name,
    coalesce(nullif(route_short_name, ''), route_long_name) as route_name,
    route_type,
    case route_type
        when 0 then 'light_rail'
        when 1 then 'subway'
        when 2 then 'commuter_rail'
        when 3 then 'bus'
        when 4 then 'ferry'
        else 'other'
    end as mode
from parquet.`{{ var('reference_path') }}/routes/`
