with source as (
    select * from {{ source('raw', 'gtfs_calendar') }}
),

renamed as (
    select
        record:service_id::varchar                          as service_id,
        record:monday::varchar    = '1'                     as runs_monday,
        record:tuesday::varchar   = '1'                     as runs_tuesday,
        record:wednesday::varchar = '1'                     as runs_wednesday,
        record:thursday::varchar  = '1'                     as runs_thursday,
        record:friday::varchar    = '1'                     as runs_friday,
        record:saturday::varchar  = '1'                     as runs_saturday,
        record:sunday::varchar    = '1'                     as runs_sunday,
        {{ gtfs_date('record:start_date::varchar') }}       as start_date,
        {{ gtfs_date('record:end_date::varchar') }}         as end_date,
        record:_feed_version::varchar                       as feed_version,
        _loaded_at                                          as loaded_at
    from source
)

select * from renamed
where service_id is not null
qualify row_number() over (partition by service_id order by loaded_at desc) = 1
