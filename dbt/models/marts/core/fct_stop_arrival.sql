{#
    FACT: one row per observed stop visit (trip x stop x service day).

    Grain:     service_date + trip_id + stop_sequence
    Measures:  arrival delay (seconds), plus punctuality flags you can SUM/AVG
    Dims:      dim_date (service day), dim_time (scheduled time of day),
               dim_route & dim_stop (type 2, as at the service day), dim_trip
    Degenerate: trip_id, stop_sequence, vehicle_id

    Delay: Adelaide Metro's feed sometimes sends a delay, sometimes only a
    predicted time. We use the delay when present, otherwise compute it from
    the predicted time vs the timetable. delay_source records which.
#}
{{
    config(
        materialized='incremental',
        unique_key='stop_event_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with events as (
    select * from {{ ref('int_gtfs_rt__stop_events') }}
    {% if is_incremental() %}
    where service_date >= (
        select dateadd(day, -{{ var('lookback_days') }}, max(service_date)) from {{ this }}
    )
    {% endif %}
),

schedule as (
    select trip_id, stop_sequence, stop_id, arrival_seconds, departure_seconds, timepoint
    from {{ ref('stg_gtfs__stop_times') }}
),

trips   as (select * from {{ ref('dim_trip') }}),
routes  as (select * from {{ ref('dim_route') }}),
stops   as (select * from {{ ref('dim_stop') }}),

joined as (
    select
        e.*,
        coalesce(e.stop_id, s.stop_id)                                     as resolved_stop_id,
        coalesce(t.route_id, e.route_id)                                   as resolved_route_id,
        coalesce(t.trip_key, '-1')                                         as trip_key,
        s.timepoint,
        {{ gtfs_service_timestamp('e.service_date', 'coalesce(s.arrival_seconds, s.departure_seconds)') }}   as scheduled_arrival_local,
        {{ gtfs_service_timestamp('e.service_date', 'coalesce(s.departure_seconds, s.arrival_seconds)') }}   as scheduled_departure_local
    from events e
    left join schedule s
        on  s.trip_id       = e.trip_id
        and s.stop_sequence = e.stop_sequence
    left join trips t
        on t.trip_id = e.trip_id
),

with_delay as (
    select
        *,
        coalesce(
            arrival_delay_seconds,
            datediff(second, scheduled_arrival_local, predicted_arrival_local),
            departure_delay_seconds,
            datediff(second, scheduled_departure_local, predicted_departure_local)
        ) as delay_seconds,
        case
            when arrival_delay_seconds is not null                                   then 'feed_arrival_delay'
            when predicted_arrival_local is not null and scheduled_arrival_local is not null then 'computed_arrival'
            when departure_delay_seconds is not null                                 then 'feed_departure_delay'
            when predicted_departure_local is not null and scheduled_departure_local is not null then 'computed_departure'
            else 'none'
        end as delay_source
    from joined
)

select
    -- keys
    d.stop_event_id                                                  as stop_event_key,
    {{ date_key('d.service_date') }}                                 as date_key,
    coalesce(
        {{ time_key('d.scheduled_arrival_local') }},
        {{ time_key('d.predicted_arrival_local') }},
        -1
    )                                                                as scheduled_time_key,
    coalesce(r.route_key, '-1')                                      as route_key,
    coalesce(st.stop_key, '-1')                                      as stop_key,
    d.trip_key,

    -- degenerate dimensions
    d.trip_id,
    d.stop_sequence,
    d.vehicle_id,
    d.service_date,

    -- times
    d.scheduled_arrival_local,
    d.predicted_arrival_local,
    d.last_observed_at_local,

    -- measures
    d.delay_seconds,
    d.delay_seconds / 60.0                                           as delay_minutes,
    d.observation_count,

    -- flags (booleans: count with count_if / avg with iff(...,1,0))
    d.is_trip_cancelled                                              as is_cancelled,
    d.stop_schedule_relationship = 'SKIPPED'                         as is_skipped,
    d.timepoint = 1                                                  as is_timepoint,
    case
        when d.is_trip_cancelled                                     then 'Cancelled'
        when d.stop_schedule_relationship = 'SKIPPED'                then 'Skipped'
        when d.delay_seconds is null                                 then 'No data'
        when d.delay_seconds < {{ var('on_time_early_seconds') }}    then 'Early'
        when d.delay_seconds <= {{ var('on_time_late_seconds') }}    then 'On time'
        else 'Late'
    end                                                              as punctuality_status,
    case when d.delay_seconds is null or d.is_trip_cancelled then null
         else d.delay_seconds between {{ var('on_time_early_seconds') }} and {{ var('on_time_late_seconds') }}
    end                                                              as is_on_time,

    -- lineage
    d.delay_source,
    d.event_source,
    current_timestamp()                                              as dbt_updated_at
from with_delay d
left join routes r
    on  r.route_id = d.resolved_route_id
    and d.service_date::timestamp_ntz >= r.valid_from
    and d.service_date::timestamp_ntz <  r.valid_to
    and r.route_key <> '-1'
left join stops st
    on  st.stop_id = d.resolved_stop_id
    and d.service_date::timestamp_ntz >= st.valid_from
    and d.service_date::timestamp_ntz <  st.valid_to
    and st.stop_key <> '-1'
