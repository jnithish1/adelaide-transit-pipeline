-- Trips across all timetable versions. A trip_id that appears in several
-- versions keeps its latest definition, while trips from older versions stay
-- available so historical realtime data can still be matched.
with source as (
    select * from {{ source('raw', 'gtfs_trips') }}
),

renamed as (
    select
        record:trip_id::varchar                                as trip_id,
        record:route_id::varchar                               as route_id,
        record:service_id::varchar                             as service_id,
        nullif(record:trip_headsign::varchar, '')              as trip_headsign,
        try_to_number(record:direction_id::varchar)            as direction_id,
        nullif(record:block_id::varchar, '')                   as block_id,
        nullif(record:shape_id::varchar, '')                   as shape_id,
        try_to_number(record:wheelchair_accessible::varchar)   as wheelchair_accessible,
        record:_feed_version::varchar                          as feed_version,
        _loaded_at                                             as loaded_at
    from source
)

select * from renamed
where trip_id is not null
qualify row_number() over (partition by trip_id order by loaded_at desc) = 1
