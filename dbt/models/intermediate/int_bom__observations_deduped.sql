{#
    1. Deduplicate: the same half-hourly observation lands up to 3 times
       (rolling 72h files). Keep the most recently loaded copy, in case BOM
       corrected a value.
    2. Turn BOM's cumulative "rain since 9am" into rain per interval. The
       counter resets at 09:00 local time, so the 'rain day' runs 09:00-08:59.
       interval rain = this reading - previous reading in the same rain day.
       Readings never decrease within a rain day; greatest(...,0) guards
       against occasional BOM corrections.
#}
with deduped as (
    select *
    from {{ ref('stg_bom__observations') }}
    qualify row_number() over (
        partition by station_id, observed_at_local
        order by loaded_at desc
    ) = 1
),

with_rain_day as (
    select
        *,
        to_date(dateadd(hour, -9, observed_at_local)) as rain_day
    from deduped
)

select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observed_at_local']) }} as observation_id,
    station_id,
    station_name,
    station_lat,
    station_lon,
    observed_at_local,
    rain_day,
    air_temp_c,
    apparent_temp_c,
    relative_humidity_pct,
    wind_speed_kmh,
    wind_gust_kmh,
    wind_direction,
    pressure_msl_hpa,
    weather_description,
    rain_since_9am_mm,
    greatest(
        rain_since_9am_mm - coalesce(
            lag(rain_since_9am_mm) over (
                partition by station_id, rain_day order by observed_at_local
            ), 0),
        0
    ) as rain_interval_mm
from with_rain_day
