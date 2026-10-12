with source as (
    select * from {{ source('raw', 'gtfs_stops') }}
),

renamed as (
    select
        record:stop_id::varchar                               as stop_id,
        nullif(record:stop_code::varchar, '')                 as stop_code,
        trim(record:stop_name::varchar)                       as stop_name,
        try_to_double(record:stop_lat::varchar)               as stop_lat,
        try_to_double(record:stop_lon::varchar)               as stop_lon,
        coalesce(try_to_number(record:location_type::varchar), 0) as location_type,
        nullif(record:parent_station::varchar, '')            as parent_station_id,
        try_to_number(record:wheelchair_boarding::varchar)    as wheelchair_boarding,
        nullif(record:zone_id::varchar, '')                   as zone_id,
        record:_feed_version::varchar                         as feed_version,
        _loaded_at                                            as loaded_at
    from source
)

select * from renamed
where stop_id is not null
qualify row_number() over (partition by stop_id order by loaded_at desc) = 1
