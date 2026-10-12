-- =============================================================================
-- 02_s3_integration.sql  -  connect Snowflake to your S3 bucket securely.
--
-- Uses a storage integration (an IAM role Snowflake assumes), so no AWS keys
-- are ever stored in Snowflake. This is a two-step handshake with AWS:
--
--   1. Create the IAM role in AWS (infra/aws/README.md), note its ARN.
--   2. Run step A below with your bucket + role ARN.
--   3. Run step B, copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
--      into the IAM role's trust policy (infra/aws/snowflake-trust-policy.json).
--   4. Run step C to create the stage and test it.
-- =============================================================================
USE ROLE ACCOUNTADMIN;

-- Step A ---------------------------------------------------------------------
CREATE STORAGE INTEGRATION IF NOT EXISTS TRANSIT_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake-transit-reader'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_BUCKET>/raw/');

GRANT USAGE ON INTEGRATION TRANSIT_S3_INT TO ROLE TRANSFORMER;

-- Step B ---------------------------------------------------------------------
DESC INTEGRATION TRANSIT_S3_INT;
-- -> copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into the trust policy

-- Step C ---------------------------------------------------------------------
USE ROLE TRANSFORMER;
USE SCHEMA TRANSIT.RAW;

CREATE STAGE IF NOT EXISTS LAKE_STAGE
  URL = 's3://<YOUR_BUCKET>/raw/'
  STORAGE_INTEGRATION = TRANSIT_S3_INT
  FILE_FORMAT = NDJSON_GZ;

-- Should list the files your extractors have landed:
LIST @LAKE_STAGE;

-- Peek at a few records without loading anything:
SELECT $1, METADATA$FILENAME
FROM @LAKE_STAGE/bom/observations/ (FILE_FORMAT => 'NDJSON_GZ')
LIMIT 5;
