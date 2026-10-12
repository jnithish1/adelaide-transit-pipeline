-- BOM half-hourly observations. Each landed file is a rolling 72h window, so
-- the same observation appears in up to three files. Deduplicated in
-- int_bom__observations_deduped.
with source as (
    select * from {{ source('raw', 'bom_observations') }}
),

renamed as (
    select
        record:wmo::varchar                                                  as station_id,
        record:_station_name::varchar                                        as station_name,
        try_to_timestamp_ntz(record:local_date_time_full::varchar, 'YYYYMMDDHH24MISS') as observed_at_local,
        try_to_timestamp_ntz(record:aifstime_utc::varchar, 'YYYYMMDDHH24MISS')         as observed_at_utc,
        record:air_temp::float                                               as air_temp_c,
        record:apparent_t::float                                             as apparent_temp_c,
        record:rel_hum::number                                               as relative_humidity_pct,
        record:wind_spd_kmh::number                                          as wind_speed_kmh,
        record:gust_kmh::number                                              as wind_gust_kmh,
        nullif(record:wind_dir::varchar, '-')                                as wind_direction,
        record:press_msl::float                                              as pressure_msl_hpa,
        -- "Rain since 9am", cumulative, in mm. BOM uses '-' for missing.
        try_to_double(record:rain_trace::varchar)                            as rain_since_9am_mm,
        nullif(record:weather::varchar, '-')                                 as weather_description,
        record:lat::float                                                    as station_lat,
        record:lon::float                                                    as station_lon,
        _source_file                                                         as source_file,
        _loaded_at                                                           as loaded_at
    from source
)

select * from renamed
where station_id is not null and observed_at_local is not null
