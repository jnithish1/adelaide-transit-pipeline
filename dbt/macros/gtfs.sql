{#
    GTFS stores times as HH:MM:SS relative to the service day, and HH can go
    past 24 for trips that run after midnight ("25:10:00" = 1:10am the next
    calendar day, but still the previous service day). So we keep them as
    seconds-since-service-day-start rather than casting to TIME.
#}
{% macro gtfs_time_to_seconds(column) -%}
    (
        try_to_number(split_part({{ column }}, ':', 1)) * 3600
      + try_to_number(split_part({{ column }}, ':', 2)) * 60
      + try_to_number(split_part({{ column }}, ':', 3))
    )
{%- endmacro %}


{# Service day + seconds offset -> local timestamp (Australia/Adelaide, NTZ). #}
{% macro gtfs_service_timestamp(service_date, seconds) -%}
    dateadd(second, {{ seconds }}, {{ service_date }}::timestamp_ntz)
{%- endmacro %}


{# Unix epoch seconds (UTC) -> Adelaide local time, NTZ. Handles daylight saving. #}
{% macro epoch_to_adelaide(epoch) -%}
    convert_timezone('UTC', 'Australia/Adelaide', to_timestamp_ntz({{ epoch }}))
{%- endmacro %}


{# YYYYMMDD string -> DATE #}
{% macro gtfs_date(column) -%}
    try_to_date({{ column }}, 'YYYYMMDD')
{%- endmacro %}


{# Smart-key for the date dimension, e.g. 2026-10-12 -> 20261012 #}
{% macro date_key(date_expr) -%}
    to_number(to_char({{ date_expr }}, 'YYYYMMDD'))
{%- endmacro %}


{# Smart-key for the time-of-day dimension, e.g. 17:45 -> 1745 #}
{% macro time_key(ts_expr) -%}
    (hour({{ ts_expr }}) * 100 + minute({{ ts_expr }}))
{%- endmacro %}


{# GTFS route_type -> readable mode #}
{% macro route_mode(route_type) -%}
    case {{ route_type }}
        when 0 then 'Tram'
        when 1 then 'Metro'
        when 2 then 'Train'
        when 3 then 'Bus'
        when 4 then 'Ferry'
        else 'Other'
    end
{%- endmacro %}
