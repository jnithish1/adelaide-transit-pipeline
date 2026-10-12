-- =============================================================================
-- metrics.sql - the numbers for your resume, measured from Snowflake itself.
-- Run in a Snowflake worksheet after the pipeline has run for a few weeks.
-- Role: TRANSFORMER (has IMPORTED PRIVILEGES on the SNOWFLAKE database).
-- Note: ACCOUNT_USAGE views lag by up to ~3 hours.
-- =============================================================================
USE ROLE TRANSFORMER;
USE WAREHOUSE TRANSIT_WH;
USE DATABASE TRANSIT;

-- 1. "X million rows processed" ----------------------------------------------
-- Rows loaded into RAW, total and per day, from the pipeline's own log.
SELECT
    SUM(rows_loaded)                                  AS total_rows_loaded,
    ROUND(SUM(rows_loaded) / 1e6, 2)                  AS total_rows_millions,
    COUNT(DISTINCT run_id)                            AS load_runs,
    ROUND(SUM(rows_loaded) / NULLIF(COUNT(DISTINCT TO_DATE(started_at)), 0)) AS avg_rows_per_day,
    MIN(started_at)                                   AS first_load,
    MAX(started_at)                                   AS last_load
FROM OPS.LOAD_LOG;

-- Per source table
SELECT table_name, SUM(rows_loaded) AS rows_loaded, SUM(files_loaded) AS files_loaded
FROM OPS.LOAD_LOG
GROUP BY 1
ORDER BY 2 DESC;

-- Current size of the star schema
SELECT 'fct_stop_arrival' AS model, COUNT(*) AS row_count FROM ANALYTICS_MARTS.FCT_STOP_ARRIVAL
UNION ALL SELECT 'fct_weather_hourly', COUNT(*) FROM ANALYTICS_MARTS.FCT_WEATHER_HOURLY
UNION ALL SELECT 'dim_stop',  COUNT(*) FROM ANALYTICS_MARTS.DIM_STOP
UNION ALL SELECT 'dim_route', COUNT(*) FROM ANALYTICS_MARTS.DIM_ROUTE
UNION ALL SELECT 'dim_trip',  COUNT(*) FROM ANALYTICS_MARTS.DIM_TRIP;

-- 2. "Y dbt tests" and pass rate ------------------------------------------------
SELECT
    MAX(dbt_tests_run)                                         AS tests_per_run,
    COUNT(*)                                                   AS pipeline_runs,
    COUNT_IF(status = 'success')                               AS successful_runs,
    ROUND(100 * COUNT_IF(status = 'success') / COUNT(*), 1)    AS success_rate_pct,
    SUM(dbt_tests_failed)                                      AS total_test_failures_caught
FROM OPS.PIPELINE_RUNS;

-- 3. "daily run in Z minutes" -----------------------------------------------
SELECT
    orchestrator,
    COUNT(*)                                                   AS runs,
    ROUND(MEDIAN(duration_seconds) / 60, 1)                    AS median_minutes,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duration_seconds) / 60, 1) AS p90_minutes,
    ROUND(MAX(duration_seconds) / 60, 1)                       AS max_minutes
FROM OPS.PIPELINE_RUNS
WHERE status = 'success'
GROUP BY 1;

-- 4. "costing about $N per month" -------------------------------------------
-- Credits used by the pipeline's warehouse, per month.
SELECT
    DATE_TRUNC('month', start_time)::DATE                      AS month,
    ROUND(SUM(credits_used), 3)                                AS warehouse_credits,
    ROUND(SUM(credits_used_cloud_services), 3)                 AS cloud_services_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'TRANSIT_WH'
GROUP BY 1
ORDER BY 1 DESC;
-- Multiply credits by your account's price per credit, which Snowflake shows
-- under Admin > Cost Management (it depends on edition and region).
-- Storage is separate and tiny here:
SELECT
    DATE_TRUNC('month', usage_date)::DATE                      AS month,
    ROUND(AVG(storage_bytes + stage_bytes + failsafe_bytes) / POWER(1024, 3), 3) AS avg_storage_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
GROUP BY 1
ORDER BY 1 DESC;

-- Which steps cost the most (by query tag set in load + dbt profiles)
SELECT
    query_tag,
    COUNT(*)                                                   AS queries,
    ROUND(SUM(total_elapsed_time) / 1000 / 60, 1)              AS total_minutes,
    ROUND(SUM(bytes_scanned) / POWER(1024, 3), 2)              AS gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name = 'TRANSIT_WH'
  AND start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 3 DESC;

-- 5. A finding to talk about in interviews --------------------------------
SELECT * FROM ANALYTICS_MARTS.MART_DELAY_BY_WEATHER
ORDER BY route_mode, rain_band, day_period;
