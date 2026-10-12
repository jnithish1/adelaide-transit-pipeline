-- FACT: weather per station per clock hour (Adelaide local time).
-- Grain: station_id + hour. Rolled up from half-hourly BOM observations so it
-- lines up with dim_time at the hour (time_key = HH00).
with obs as (
    select * from {{ ref('int_bom__observations_deduped') }}
),

hourly as (
    select
        station_id,
        date_trunc('hour', observed_at_local)                as hour_start_local,
        count(*)                                             as observation_count,
        avg(air_temp_c)                                      as avg_air_temp_c,
        min(air_temp_c)                                      as min_air_temp_c,
        max(air_temp_c)                                      as max_air_temp_c,
        avg(relative_humidity_pct)                           as avg_relative_humidity_pct,
        avg(wind_speed_kmh)                                  as avg_wind_speed_kmh,
        max(wind_gust_kmh)                                   as max_wind_gust_kmh,
        sum(rain_interval_mm)                                as rain_mm
    from obs
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['h.station_id', 'h.hour_start_local']) }} as weather_hour_key,
    {{ date_key('h.hour_start_local') }}                     as date_key,
    hour(h.hour_start_local) * 100                           as time_key,
    ws.station_key,
    h.hour_start_local,
    h.observation_count,
    round(h.avg_air_temp_c, 1)                               as avg_air_temp_c,
    h.min_air_temp_c,
    h.max_air_temp_c,
    round(h.avg_relative_humidity_pct, 0)                    as avg_relative_humidity_pct,
    round(h.avg_wind_speed_kmh, 1)                           as avg_wind_speed_kmh,
    h.max_wind_gust_kmh,
    round(h.rain_mm, 1)                                      as rain_mm,
    h.rain_mm > 0                                            as is_wet_hour,
    case
        when h.rain_mm is null or h.rain_mm = 0 then 'Dry'
        when h.rain_mm < 1  then 'Light rain'
        when h.rain_mm < 4  then 'Moderate rain'
        else 'Heavy rain'
    end                                                      as rain_band,
    case
        when h.max_air_temp_c >= 35 then 'Hot (35C+)'
        when h.max_air_temp_c >= 25 then 'Warm'
        when h.max_air_temp_c >= 12 then 'Mild'
        else 'Cold'
    end                                                      as temperature_band
from hourly h
inner join {{ ref('dim_weather_station') }} ws
    on ws.station_id = h.station_id
