-- One row per stop_time_update per realtime snapshot. This is the high-volume
-- table (~1M+ rows a day), so it's incremental: each run only reads rows
-- loaded since the last run. Rows are append-only in RAW, so 'append' is safe
-- and much cheaper than a merge.
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('raw', 'gtfs_rt_trip_updates') }}
    {% if is_incremental() %}
    where _loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['_source_file', '_source_row']) }}  as trip_update_id,

        -- when the snapshot was taken (feed header, falling back to capture time)
        coalesce(
            {{ epoch_to_adelaide('record:feed_timestamp::number') }},
            convert_timezone('Australia/Adelaide', record:captured_at::timestamp_tz)::timestamp_ntz
        )                                                        as observed_at_local,

        record:trip_id::varchar                                  as trip_id,
        record:route_id::varchar                                 as route_id,
        record:direction_id::number                              as direction_id,
        {{ gtfs_date('record:start_date::varchar') }}            as start_date,
        record:trip_schedule_relationship::varchar               as trip_schedule_relationship,
        record:vehicle_id::varchar                               as vehicle_id,

        record:stop_sequence::number                             as stop_sequence,
        record:stop_id::varchar                                  as stop_id,
        record:stop_schedule_relationship::varchar               as stop_schedule_relationship,

        record:arrival_delay::number                             as arrival_delay_seconds,
        {{ epoch_to_adelaide('record:arrival_time::number') }}   as predicted_arrival_local,
        record:departure_delay::number                           as departure_delay_seconds,
        {{ epoch_to_adelaide('record:departure_time::number') }} as predicted_departure_local,

        _source_file                                             as source_file,
        _loaded_at                                               as loaded_at
    from source
)

select
    *,
    -- service date: from the feed when given, otherwise the local day of the
    -- snapshot (an approximation only used for trips without start_date)
    coalesce(start_date, observed_at_local::date)                as service_date
from renamed
where trip_id is not null
