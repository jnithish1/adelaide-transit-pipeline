-- "Do Adelaide buses, trains and trams run later when it rains or on hot days?"
-- Joins each stop visit to the weather in the clock hour it was scheduled.
-- Note: the join uses the scheduled clock hour, not the service date, so a
-- 00:30 arrival on a trip from the previous service day still gets the
-- right weather.
with f as (
    select *
    from {{ ref('fct_stop_arrival') }}
    where not is_cancelled
      and delay_seconds is not null
      and scheduled_arrival_local is not null
),

w as (
    select * from {{ ref('fct_weather_hourly') }}
)

select
    r.route_mode,
    t.day_period,
    dd.service_day_type,
    w.rain_band,
    w.temperature_band,
    count(*)                                                         as stop_events,
    round(100 * avg(iff(f.is_on_time, 1, 0)), 1)                     as on_time_pct,
    round(avg(f.delay_seconds) / 60, 2)                              as avg_delay_minutes,
    round(percentile_cont(0.9) within group (order by f.delay_seconds) / 60, 2) as p90_delay_minutes
from f
inner join w
    on w.hour_start_local = date_trunc('hour', f.scheduled_arrival_local)
inner join {{ ref('dim_route') }} r  on r.route_key = f.route_key
inner join {{ ref('dim_time') }}  t  on t.time_key  = f.scheduled_time_key
inner join {{ ref('dim_date') }}  dd on dd.date_key = f.date_key
group by all
having count(*) >= 100   -- hide buckets too small to mean anything
