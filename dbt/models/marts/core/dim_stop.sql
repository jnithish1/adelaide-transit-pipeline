-- Type 2 stop dimension built on the snapshot (see dim_route for the pattern).
with snap as (
    select * from {{ ref('snap_gtfs__stops') }}
),

versions as (
    select
        {{ dbt_utils.generate_surrogate_key(['stop_id', 'dbt_valid_from']) }} as stop_key,
        stop_id,
        stop_code,
        stop_name,
        stop_lat,
        stop_lon,
        case location_type
            when 0 then 'Stop / platform'
            when 1 then 'Station'
            when 2 then 'Entrance / exit'
            when 3 then 'Generic node'
            when 4 then 'Boarding area'
            else 'Unknown'
        end                                                                  as stop_type,
        parent_station_id,
        case wheelchair_boarding when 1 then true when 2 then false end      as is_wheelchair_accessible,
        zone_id,
        case
            when row_number() over (partition by stop_id order by dbt_valid_from) = 1
                then '1900-01-01'::timestamp_ntz
            else dbt_valid_from::timestamp_ntz
        end                                                                  as valid_from,
        coalesce(dbt_valid_to::timestamp_ntz, '9999-12-31'::timestamp_ntz)  as valid_to,
        dbt_valid_to is null                                                 as is_current
    from snap
),

unknown_member as (
    select
        '-1', '-1', null, 'Unknown stop', null, null, 'Unknown', null, null, null,
        '1900-01-01'::timestamp_ntz, '9999-12-31'::timestamp_ntz, true
)

select * from versions
union all
select * from unknown_member
