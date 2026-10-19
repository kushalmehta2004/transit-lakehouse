{#-
    Fails on any delay outside [plausible_delay_min_s, plausible_delay_max_s].
    Real passed stop visits sit within -16 min .. +43 min; a 10-hour "delay" means a bad
    schedule join or a timezone / service-date bug, not a late bus.
-#}
{% test delay_in_plausible_range(model, column_name,
                                 min_seconds=var('plausible_delay_min_s'),
                                 max_seconds=var('plausible_delay_max_s')) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_seconds }} or {{ column_name }} > {{ max_seconds }})
{% endtest %}
