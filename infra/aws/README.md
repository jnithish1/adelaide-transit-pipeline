# AWS setup

Everything here fits in the AWS free tier except S3 storage, which for this
project is a few hundred MB a month (cents). Region: `ap-southeast-2` (Sydney),
the same region you should pick for your Snowflake account so data transfer
between S3 and Snowflake is free.

Replace `<YOUR_BUCKET>`, `<AWS_ACCOUNT_ID>` and `<GITHUB_USER>` in the JSON files
first. Run these with the AWS CLI logged in as your admin user.

## 1. Bucket

```bash
export BUCKET=<YOUR_BUCKET>          # globally unique, e.g. nithish-adl-transit-lake
aws s3api create-bucket --bucket $BUCKET --region ap-southeast-2 \
  --create-bucket-configuration LocationConstraint=ap-southeast-2
aws s3api put-public-access-block --bucket $BUCKET \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

# Optional: move raw files older than 90 days to cheaper storage
aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET --lifecycle-configuration '{
  "Rules":[{"ID":"raw-to-ia","Status":"Enabled","Filter":{"Prefix":"raw/"},
  "Transitions":[{"Days":90,"StorageClass":"STANDARD_IA"}]}]}'
```

## 2. Role Snowflake assumes to read the bucket

```bash
# Temporary trust policy so the role can be created; you replace it in step 3.
aws iam create-role --role-name snowflake-transit-reader \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::<AWS_ACCOUNT_ID>:root"},"Action":"sts:AssumeRole"}]}'
aws iam put-role-policy --role-name snowflake-transit-reader \
  --policy-name read-raw --policy-document file://snowflake-read-policy.json
```

## 3. Finish the Snowflake handshake

Run steps A and B of `snowflake/02_s3_integration.sql`, paste the two values
from `DESC INTEGRATION` into `snowflake-trust-policy.json`, then:

```bash
aws iam update-assume-role-policy --role-name snowflake-transit-reader \
  --policy-document file://snowflake-trust-policy.json
```

Then run step C. `LIST @LAKE_STAGE;` should succeed.

## 4. Credentials for the pipeline to write to S3

**GitHub Actions (recommended, no long-lived keys):** OIDC role.

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com
aws iam create-role --role-name github-transit-pipeline \
  --assume-role-policy-document file://github-oidc-trust-policy.json
aws iam put-role-policy --role-name github-transit-pipeline \
  --policy-name write-raw --policy-document file://pipeline-writer-policy.json
```

Put the role ARN in the GitHub secret `AWS_ROLE_ARN`.

**Airflow running locally:** create an IAM user with only
`pipeline-writer-policy.json` attached and put its access key in `.env`
(never commit it).

## 5. Optional: realtime capture as a Lambda

If you'd rather not run a 15-minute schedule in GitHub Actions, package
`ingestion/` with its dependencies and deploy `extract_gtfs_realtime.lambda_handler`
on an EventBridge rule `rate(15 minutes)`. That's ~2,900 invocations a month,
well inside the Lambda free tier.
