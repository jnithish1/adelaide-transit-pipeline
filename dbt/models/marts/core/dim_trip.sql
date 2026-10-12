-- Type 1 trip dimension: trip attributes rarely change within a timetable, and
-- trip_ids are replaced (not edited) between versions, so history isn't needed.
with trips as (
    select * from {{ ref('stg_gtfs__trips') }}
),

calendar as (
    select * from {{ ref('stg_gtfs__calendar') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['t.trip_id']) }}   as trip_key,
    t.trip_id,
    t.route_id,
    t.service_id,
    t.trip_headsign,
    t.direction_id,
    case t.direction_id when 0 then 'Outbound' when 1 then 'Inbound' end as direction_name,
    t.block_id,
    t.shape_id,
    case t.wheelchair_accessible when 1 then true when 2 then false end   as is_wheelchair_accessible,
    case
        when c.runs_monday and c.runs_friday and not c.runs_saturday and not c.runs_sunday then 'Weekday'
        when c.runs_saturday and not c.runs_monday then 'Saturday'
        when c.runs_sunday and not c.runs_monday then 'Sunday / Public holiday'
        when c.service_id is null then 'Exceptions only'
        else 'Mixed'
    end                                                      as service_pattern,
    t.feed_version
from trips t
left join calendar c
    on c.service_id = t.service_id

union all

select '-1', '-1', '-1', null, 'Unknown trip', null, null, null, null, null, 'Unknown', null
