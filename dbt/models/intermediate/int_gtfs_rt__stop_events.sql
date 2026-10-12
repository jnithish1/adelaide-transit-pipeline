{#
    One row per (service_date, trip_id, stop_sequence): the LAST realtime
    observation of that stop visit.

    Why the last one: a trip appears in every 15-minute snapshot while it's
    running, with a fresh delay estimate each time. Feeds stop reporting a stop
    once the vehicle has passed it, so the final observation is the closest we
    get to the actual arrival. observation_count and first_observed_at are kept
    so you can see how much evidence each row is based on.

    Cancelled trips often arrive with no stop detail at all, so they're
    expanded to their scheduled stops from the timetable - otherwise
    cancellations would silently vanish from the fact table.

    Incremental: each run rebuilds the last `lookback_days` service days, which
    covers trips that cross midnight and snapshots that land late.
#}
{{
    config(
        materialized='incremental',
        unique_key='stop_event_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with updates as (
    select * from {{ ref('stg_gtfs_rt__trip_updates') }}
    {% if is_incremental() %}
    where service_date >= (
        select dateadd(day, -{{ var('lookback_days') }}, max(service_date)) from {{ this }}
    )
    {% endif %}
),

-- latest trip-level status (catches cancellations)
trip_status as (
    select
        service_date,
        trip_id,
        route_id,
        direction_id,
        vehicle_id,
        trip_schedule_relationship,
        observed_at_local
    from updates
    qualify row_number() over (
        partition by service_date, trip_id
        order by observed_at_local desc, loaded_at desc
    ) = 1
),

-- latest observation per stop visit
stop_observations as (
    select
        service_date,
        trip_id,
        stop_sequence,
        stop_id,
        route_id,
        direction_id,
        vehicle_id,
        stop_schedule_relationship,
        arrival_delay_seconds,
        predicted_arrival_local,
        departure_delay_seconds,
        predicted_departure_local,
        count(*) over (partition by service_date, trip_id, stop_sequence)               as observation_count,
        min(observed_at_local) over (partition by service_date, trip_id, stop_sequence) as first_observed_at_local,
        observed_at_local                                                                as last_observed_at_local
    from updates
    where stop_sequence is not null
    qualify row_number() over (
        partition by service_date, trip_id, stop_sequence
        order by observed_at_local desc, loaded_at desc
    ) = 1
),

observed as (
    select
        o.*,
        coalesce(t.trip_schedule_relationship = 'CANCELED', false) as is_trip_cancelled,
        'realtime_stop_update'                                      as event_source
    from stop_observations o
    left join trip_status t
        on  o.service_date = t.service_date
        and o.trip_id      = t.trip_id
),

-- cancelled trips: one row per scheduled stop not already observed
cancelled_expanded as (
    select
        t.service_date,
        t.trip_id,
        st.stop_sequence,
        st.stop_id,
        t.route_id,
        t.direction_id,
        t.vehicle_id,
        'SKIPPED'                   as stop_schedule_relationship,
        null::number                as arrival_delay_seconds,
        null::timestamp_ntz         as predicted_arrival_local,
        null::number                as departure_delay_seconds,
        null::timestamp_ntz         as predicted_departure_local,
        1                           as observation_count,
        t.observed_at_local         as first_observed_at_local,
        t.observed_at_local         as last_observed_at_local,
        true                        as is_trip_cancelled,
        'cancelled_trip_expanded'   as event_source
    from trip_status t
    inner join {{ ref('stg_gtfs__stop_times') }} st
        on st.trip_id = t.trip_id
    where t.trip_schedule_relationship = 'CANCELED'
      and not exists (
          select 1 from stop_observations o
          where o.service_date  = t.service_date
            and o.trip_id       = t.trip_id
            and o.stop_sequence = st.stop_sequence
      )
),

unioned as (
    select * from observed
    union all
    select * from cancelled_expanded
)

select
    {{ dbt_utils.generate_surrogate_key(['service_date', 'trip_id', 'stop_sequence']) }} as stop_event_id,
    *
from unioned
