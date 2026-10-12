-- Pipeline health check: yesterday should have a reasonable number of realtime
-- snapshots. At one every 15 minutes we expect ~96; fewer than 80 means the
-- capture schedule missed runs (GitHub Actions cron can skip under load).
{{ config(severity='warn') }}

select
    count(distinct source_file) as snapshots_captured
from {{ ref('stg_gtfs_rt__trip_updates') }}
where observed_at_local::date = dateadd(day, -1, convert_timezone('Australia/Adelaide', current_timestamp())::date)
having count(distinct source_file) < 80
