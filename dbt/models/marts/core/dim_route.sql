-- Type 2 route dimension built on the snapshot. One row per version of a
-- route. Facts join on route_id AND service date between valid_from and
-- valid_to, so a trip in March reports against the route as it was in March.
--
-- The first version of each route is back-dated to 1900-01-01: we only
-- started snapshotting when the pipeline went live, and realtime data
-- captured before that should still find a match.
with snap as (
    select * from {{ ref('snap_gtfs__routes') }}
),

versions as (
    select
        {{ dbt_utils.generate_surrogate_key(['route_id', 'dbt_valid_from']) }} as route_key,
        route_id,
        route_short_name,
        route_long_name,
        coalesce(route_short_name, route_id) || coalesce(' - ' || route_long_name, '') as route_display_name,
        route_desc,
        route_type,
        {{ route_mode('route_type') }}                                       as route_mode,
        route_color,
        case
            when row_number() over (partition by route_id order by dbt_valid_from) = 1
                then '1900-01-01'::timestamp_ntz
            else dbt_valid_from::timestamp_ntz
        end                                                                  as valid_from,
        coalesce(dbt_valid_to::timestamp_ntz, '9999-12-31'::timestamp_ntz)  as valid_to,
        dbt_valid_to is null                                                 as is_current
    from snap
),

unknown_member as (
    -- Kimball 'unknown' row: facts that can't be matched point here instead of
    -- having a NULL foreign key.
    select
        '-1', '-1', null, null, 'Unknown route', null, null, 'Unknown', null,
        '1900-01-01'::timestamp_ntz, '9999-12-31'::timestamp_ntz, true
)

select * from versions
union all
select * from unknown_member
