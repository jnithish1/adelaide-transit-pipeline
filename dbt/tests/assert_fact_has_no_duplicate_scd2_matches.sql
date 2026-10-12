-- The fact must have exactly one row per stop visit coming out of the
-- intermediate model - no fan-out from the type 2 joins, no rows lost.
-- Checked over the lookback window only, to keep the test cheap.
with fact as (
    select service_date, count(*) as fact_rows
    from {{ ref('fct_stop_arrival') }}
    where service_date >= dateadd(day, -{{ var('lookback_days') }}, current_date())
    group by 1
),
source as (
    select service_date, count(*) as source_rows
    from {{ ref('int_gtfs_rt__stop_events') }}
    where service_date >= dateadd(day, -{{ var('lookback_days') }}, current_date())
    group by 1
)

select coalesce(f.service_date, s.service_date) as service_date, f.fact_rows, s.source_rows
from fact f
full outer join source s on s.service_date = f.service_date
where coalesce(f.fact_rows, 0) <> coalesce(s.source_rows, 0)
