-- On-time performance per route per day. The table you'd put behind a
-- dashboard: "which routes ran late this week, and how late?"
with f as (
    select * from {{ ref('fct_stop_arrival') }}
)

select
    dd.date_day                                                         as service_date,
    dd.day_name,
    dd.service_day_type,
    r.route_key,
    r.route_id,
    r.route_short_name,
    r.route_display_name,
    r.route_mode,
    count(*)                                                            as stop_events,
    count_if(f.is_on_time is not null)                                  as stop_events_with_delay,
    count_if(f.is_on_time)                                              as on_time_events,
    round(100 * count_if(f.is_on_time) / nullif(count_if(f.is_on_time is not null), 0), 1) as on_time_pct,
    count_if(f.punctuality_status = 'Late')                             as late_events,
    count_if(f.punctuality_status = 'Early')                            as early_events,
    count(distinct iff(f.is_cancelled, f.trip_id, null))                as cancelled_trips,
    count(distinct f.trip_id)                                           as observed_trips,
    round(avg(iff(f.is_cancelled, null, f.delay_seconds)) / 60, 2)      as avg_delay_minutes,
    round(median(iff(f.is_cancelled, null, f.delay_seconds)) / 60, 2)   as median_delay_minutes,
    round(percentile_cont(0.9) within group (order by iff(f.is_cancelled, null, f.delay_seconds)) / 60, 2)
                                                                        as p90_delay_minutes
from f
inner join {{ ref('dim_date') }}  dd on dd.date_key  = f.date_key
inner join {{ ref('dim_route') }} r  on r.route_key  = f.route_key
group by all
