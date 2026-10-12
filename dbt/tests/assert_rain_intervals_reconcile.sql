-- The per-interval rain we derive must add back up to BOM's own cumulative
-- "rain since 9am" total. Returns rain days where they disagree by > 0.2mm
-- (BOM's rounding). Only complete rain days are checked.
with daily as (
    select
        station_id,
        rain_day,
        sum(rain_interval_mm)   as derived_total_mm,
        max(rain_since_9am_mm)  as bom_total_mm,
        count(*)                as readings
    from {{ ref('int_bom__observations_deduped') }}
    group by 1, 2
)

select *
from daily
where readings >= 46               -- a full day has 48 half-hourly readings
  and abs(derived_total_mm - bom_total_mm) > 0.2
