-- The largest static table (~1.5M rows per timetable version), so it's
-- materialised as a table rather than a view.
{{ config(materialized='table') }}

with source as (
    select * from {{ source('raw', 'gtfs_stop_times') }}
),

renamed as (
    select
        record:trip_id::varchar                                  as trip_id,
        try_to_number(record:stop_sequence::varchar)             as stop_sequence,
        record:stop_id::varchar                                  as stop_id,
        nullif(record:arrival_time::varchar, '')                 as arrival_time_raw,
        nullif(record:departure_time::varchar, '')               as departure_time_raw,
        {{ gtfs_time_to_seconds("nullif(record:arrival_time::varchar, '')") }}   as arrival_seconds,
        {{ gtfs_time_to_seconds("nullif(record:departure_time::varchar, '')") }} as departure_seconds,
        try_to_number(record:pickup_type::varchar)               as pickup_type,
        try_to_number(record:drop_off_type::varchar)             as drop_off_type,
        try_to_number(record:timepoint::varchar)                 as timepoint,
        try_to_double(record:shape_dist_traveled::varchar)       as shape_dist_traveled,
        record:_feed_version::varchar                            as feed_version,
        _loaded_at                                               as loaded_at
    from source
)

select * from renamed
where trip_id is not null and stop_sequence is not null
qualify row_number() over (
    partition by trip_id, stop_sequence order by loaded_at desc
) = 1
