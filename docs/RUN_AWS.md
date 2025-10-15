# Running on AWS

## Prerequisites

- AWS CLI configured
- EMR Serverless application created
- S3 buckets for data lake
- IAM roles with proper permissions

## Setup

1. Install AWS dependencies:
```bash
pip install -e .[aws]
```

2. Configure environment:
```bash
export ENV=aws
export AWS_DEFAULT_REGION=us-east-1
```

## Deployment

### EMR Serverless

1. Create EMR Serverless application:
```bash
aws emr-serverless create-application \
  --name "pdi-etl" \
  --type SPARK \
  --release-label emr-6.15.0
```

2. Submit job:
```bash
aws emr-serverless start-job-run \
  --application-id <app-id> \
  --execution-role-arn <role-arn> \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "pdi",
      "entryPointArguments": ["--proc_date", "2024-01-01", "--env", "aws"]
    }
  }'
```

### Airflow (MWAA)

1. Deploy DAGs to `aws/dags/`
2. Set Airflow variables:
   - `EMR_SERVERLESS_APP`: Application ID
   - `EMR_JOB_ROLE`: Execution role ARN

## S3 Layout

```
s3://my-prod-lake/
├── bronze/
├── silver/
├── gold/
├── _checkpoints/
└── _artifacts/
```







