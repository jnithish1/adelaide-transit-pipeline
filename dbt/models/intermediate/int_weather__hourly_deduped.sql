-- One row per location per UTC hour. The same hour arrives in up to three
-- daily files; the most recently loaded value wins, because the provider
-- revises recent hours as more observations come in.
--
-- Deduplicated on UTC, not local time: when daylight saving ends, two
-- different UTC hours both map to local 02:00, and they're real, separate hours.
select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'hour_start_utc']) }} as weather_observation_id,
    station_id,
    station_name,
    station_lat,
    station_lon,
    hour_start_utc,
    hour_start_local,
    air_temp_c,
    apparent_temp_c,
    relative_humidity_pct,
    precipitation_mm,
    rain_mm,
    weather_code,
    wind_speed_kmh,
    wind_gust_kmh
from {{ ref('stg_weather__hourly') }}
qualify row_number() over (
    partition by station_id, hour_start_utc
    order by loaded_at desc
) = 1
