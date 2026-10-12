-- FACT: weather per location per local clock hour (Adelaide time).
-- Grain: station_id + hour_start_local, lining up with dim_time at the hour
-- (time_key = HH00). Grouped because, once a year when daylight saving ends,
-- two UTC hours fall in the same local hour: rain is summed, the rest averaged.
with obs as (
    select * from {{ ref('int_weather__hourly_deduped') }}
),

hourly as (
    select
        station_id,
        hour_start_local,
        count(*)                        as source_hours,
        avg(air_temp_c)                 as air_temp_c,
        avg(apparent_temp_c)            as apparent_temp_c,
        avg(relative_humidity_pct)      as relative_humidity_pct,
        avg(wind_speed_kmh)             as wind_speed_kmh,
        max(wind_gust_kmh)              as wind_gust_kmh,
        sum(precipitation_mm)           as precipitation_mm,
        sum(rain_mm)                    as rain_mm,
        max(weather_code)               as weather_code
    from obs
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['h.station_id', 'h.hour_start_local']) }} as weather_hour_key,
    {{ date_key('h.hour_start_local') }}                     as date_key,
    hour(h.hour_start_local) * 100                           as time_key,
    ws.station_key,
    h.hour_start_local,
    h.source_hours,
    round(h.air_temp_c, 1)                                   as air_temp_c,
    round(h.apparent_temp_c, 1)                              as apparent_temp_c,
    round(h.relative_humidity_pct, 0)                        as relative_humidity_pct,
    round(h.wind_speed_kmh, 1)                               as wind_speed_kmh,
    round(h.wind_gust_kmh, 1)                                as wind_gust_kmh,
    round(h.precipitation_mm, 1)                             as precipitation_mm,
    round(h.rain_mm, 1)                                      as rain_mm,
    h.weather_code,
    h.precipitation_mm > 0                                   as is_wet_hour,
    case
        when h.precipitation_mm is null or h.precipitation_mm = 0 then 'Dry'
        when h.precipitation_mm < 1  then 'Light rain'
        when h.precipitation_mm < 4  then 'Moderate rain'
        else 'Heavy rain'
    end                                                      as rain_band,
    case
        when h.air_temp_c >= 35 then 'Hot (35C+)'
        when h.air_temp_c >= 25 then 'Warm'
        when h.air_temp_c >= 12 then 'Mild'
        else 'Cold'
    end                                                      as temperature_band
from hourly h
inner join {{ ref('dim_weather_station') }} ws
    on ws.station_id = h.station_id
