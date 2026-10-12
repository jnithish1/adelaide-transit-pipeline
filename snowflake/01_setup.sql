-- =============================================================================
-- 01_setup.sql  -  one-off Snowflake setup. Run as ACCOUNTADMIN in a worksheet.
--
-- Creates: a cost-capped warehouse, the TRANSIT database with RAW / OPS /
-- dev & prod schemas, a least-privilege role, and a key-pair service user.
-- Run 02_s3_integration.sql next.
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------------
-- Cost guardrail first. 10 credits/month is plenty for this project; the
-- warehouse is suspended automatically if it is ever reached.
-- ---------------------------------------------------------------------------
CREATE RESOURCE MONITOR IF NOT EXISTS TRANSIT_MONTHLY_CAP
  WITH CREDIT_QUOTA = 10
       FREQUENCY = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
       TRIGGERS ON 50 PERCENT DO NOTIFY
                ON 80 PERCENT DO NOTIFY
                ON 100 PERCENT DO SUSPEND;

CREATE WAREHOUSE IF NOT EXISTS TRANSIT_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60          -- seconds idle before it stops billing
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  RESOURCE_MONITOR = TRANSIT_MONTHLY_CAP
  COMMENT = 'Adelaide transit pipeline - loads + dbt';

-- ---------------------------------------------------------------------------
-- Database and schemas
--   RAW         landed data, untouched (VARIANT + lineage columns)
--   OPS         pipeline run logs used for the metrics you quote
--   DBT_DEV     your personal dbt target
--   ANALYTICS   production dbt target (staging/intermediate/marts suffixes
--               are added by dbt: ANALYTICS_STAGING, ANALYTICS_MARTS ...)
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS TRANSIT;
CREATE SCHEMA IF NOT EXISTS TRANSIT.RAW;
CREATE SCHEMA IF NOT EXISTS TRANSIT.OPS;

-- ---------------------------------------------------------------------------
-- Role: one functional role that can load RAW and build dbt models.
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS TRANSFORMER;
GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;

GRANT USAGE, OPERATE ON WAREHOUSE TRANSIT_WH TO ROLE TRANSFORMER;
GRANT USAGE, CREATE SCHEMA ON DATABASE TRANSIT TO ROLE TRANSFORMER;
GRANT ALL ON SCHEMA TRANSIT.RAW TO ROLE TRANSFORMER;
GRANT ALL ON SCHEMA TRANSIT.OPS TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE TABLES IN SCHEMA TRANSIT.RAW TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE TABLES IN SCHEMA TRANSIT.OPS TO ROLE TRANSFORMER;
-- Lets you read cost/usage views for the "$N per month" number:
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE TRANSFORMER;

-- Give yourself the role for interactive work (replace with your username).
-- GRANT ROLE TRANSFORMER TO USER <YOUR_USERNAME>;

-- ---------------------------------------------------------------------------
-- Service user for Airflow / GitHub Actions. Key-pair auth only.
-- Generate the key locally (see README "Snowflake key pair"), then paste the
-- public key body (no header/footer lines) below.
-- ---------------------------------------------------------------------------
CREATE USER IF NOT EXISTS SVC_TRANSIT_PIPELINE
  TYPE = SERVICE
  DEFAULT_ROLE = TRANSFORMER
  DEFAULT_WAREHOUSE = TRANSIT_WH
  DEFAULT_NAMESPACE = TRANSIT.RAW
  COMMENT = 'Airflow / GitHub Actions';
-- ALTER USER SVC_TRANSIT_PIPELINE SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqh...';
GRANT ROLE TRANSFORMER TO USER SVC_TRANSIT_PIPELINE;

-- ---------------------------------------------------------------------------
-- RAW tables. Same shape for every source: the record as VARIANT plus
-- lineage. Typing happens in dbt staging models, so a new column upstream
-- never breaks the load.
-- ---------------------------------------------------------------------------
USE ROLE TRANSFORMER;
USE WAREHOUSE TRANSIT_WH;
USE SCHEMA TRANSIT.RAW;

CREATE FILE FORMAT IF NOT EXISTS NDJSON_GZ
  TYPE = JSON
  COMPRESSION = GZIP
  STRIP_OUTER_ARRAY = FALSE;

CREATE TABLE IF NOT EXISTS GTFS_AGENCY          (record VARIANT, _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_LTZ);
CREATE TABLE IF NOT EXISTS GTFS_ROUTES          LIKE GTFS_AGENCY;
CREATE TABLE IF NOT EXISTS GTFS_STOPS           LIKE GTFS_AGENCY;
CREATE TABLE IF NOT EXISTS GTFS_TRIPS           LIKE GTFS_AGENCY;
CREATE TABLE IF NOT EXISTS GTFS_STOP_TIMES      LIKE GTFS_AGENCY;
CREATE TABLE IF NOT EXISTS GTFS_CALENDAR        LIKE GTFS_AGENCY;
CREATE TABLE IF NOT EXISTS GTFS_CALENDAR_DATES  LIKE GTFS_AGENCY;
CREATE TABLE IF NOT EXISTS GTFS_RT_TRIP_UPDATES LIKE GTFS_AGENCY;
CREATE TABLE IF NOT EXISTS BOM_OBSERVATIONS     LIKE GTFS_AGENCY;

-- Deliberately NOT clustered: automatic clustering bills credits in the
-- background. Data arrives in load-time order anyway, so micro-partition
-- pruning on _loaded_at already works well for the incremental models.

-- ---------------------------------------------------------------------------
-- OPS tables: written by the pipeline, read by scripts/metrics.sql
-- ---------------------------------------------------------------------------
USE SCHEMA TRANSIT.OPS;

CREATE TABLE IF NOT EXISTS LOAD_LOG (
  run_id        VARCHAR,
  table_name    VARCHAR,
  files_loaded  NUMBER,
  rows_loaded   NUMBER,
  started_at    TIMESTAMP_LTZ,
  finished_at   TIMESTAMP_LTZ
);

CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (
  run_id            VARCHAR,
  orchestrator      VARCHAR,       -- 'airflow' | 'github_actions' | 'manual'
  started_at        TIMESTAMP_LTZ,
  finished_at       TIMESTAMP_LTZ,
  duration_seconds  NUMBER,
  status            VARCHAR,
  dbt_models_run    NUMBER,
  dbt_tests_run     NUMBER,
  dbt_tests_passed  NUMBER,
  dbt_tests_warned  NUMBER,
  dbt_tests_failed  NUMBER
);
