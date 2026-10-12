-- Exceptions to the regular calendar: 1 = service added, 2 = service removed
-- (e.g. public holidays).
with source as (
    select * from {{ source('raw', 'gtfs_calendar_dates') }}
),

renamed as (
    select
        record:service_id::varchar                          as service_id,
        {{ gtfs_date('record:date::varchar') }}             as exception_date,
        try_to_number(record:exception_type::varchar)       as exception_type,
        record:_feed_version::varchar                       as feed_version,
        _loaded_at                                          as loaded_at
    from source
)

select * from renamed
where service_id is not null and exception_date is not null
qualify row_number() over (
    partition by service_id, exception_date order by loaded_at desc
) = 1
