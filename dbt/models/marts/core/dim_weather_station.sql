-- The weather locations we collect for (one by default: Adelaide CBD).
select
    {{ dbt_utils.generate_surrogate_key(['station_id']) }} as station_key,
    station_id,
    any_value(station_name)                                as station_name,
    any_value(station_lat)                                 as station_lat,
    any_value(station_lon)                                 as station_lon
from {{ ref('int_weather__hourly_deduped') }}
group by station_id
