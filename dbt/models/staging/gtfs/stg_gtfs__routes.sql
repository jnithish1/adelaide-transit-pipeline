-- Current state of each route: the version from the most recent timetable load.
-- History is kept by the snapshot (snapshots/gtfs_snapshots.yml).
with source as (
    select * from {{ source('raw', 'gtfs_routes') }}
),

renamed as (
    select
        record:route_id::varchar                          as route_id,
        nullif(record:agency_id::varchar, '')             as agency_id,
        nullif(record:route_short_name::varchar, '')      as route_short_name,
        nullif(record:route_long_name::varchar, '')       as route_long_name,
        nullif(record:route_desc::varchar, '')            as route_desc,
        try_to_number(record:route_type::varchar)         as route_type,
        nullif(record:route_color::varchar, '')           as route_color,
        nullif(record:route_text_color::varchar, '')      as route_text_color,
        record:_feed_version::varchar                     as feed_version,
        _loaded_at                                        as loaded_at
    from source
)

select * from renamed
where route_id is not null
qualify row_number() over (partition by route_id order by loaded_at desc) = 1
