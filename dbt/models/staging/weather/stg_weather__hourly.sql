-- Open-Meteo hourly weather, typed. Still contains duplicates because each
-- landed file repeats the previous 3 days; see int_weather__hourly_deduped.
--
-- Open-Meteo stamps each hour at its END: precipitation at 14:00 is the rain
-- that fell 13:00-14:00. We shift it back one hour so hour_start lines up
-- with the transit fact (an arrival at 13:20 belongs to the 13:00 hour).
with source as (
    select * from {{ source('raw', 'weather_hourly') }}
),

renamed as (
    select
        record:location_id::varchar                                   as station_id,
        record:location_name::varchar                                 as station_name,
        record:latitude::float                                        as station_lat,
        record:longitude::float                                       as station_lon,
        dateadd(hour, -1, to_timestamp_ntz(record:time_epoch_utc::number)) as hour_start_utc,
        record:temperature_2m::float                                  as air_temp_c,
        record:apparent_temperature::float                            as apparent_temp_c,
        record:relative_humidity_2m::number                           as relative_humidity_pct,
        record:precipitation::float                                   as precipitation_mm,
        record:rain::float                                            as rain_mm,
        record:weather_code::number                                   as weather_code,
        record:wind_speed_10m::float                                  as wind_speed_kmh,
        record:wind_gusts_10m::float                                  as wind_gust_kmh,
        _source_file                                                  as source_file,
        _loaded_at                                                    as loaded_at
    from source
)

select
    *,
    convert_timezone('UTC', 'Australia/Adelaide', hour_start_utc)     as hour_start_local
from renamed
where station_id is not null and hour_start_utc is not null
