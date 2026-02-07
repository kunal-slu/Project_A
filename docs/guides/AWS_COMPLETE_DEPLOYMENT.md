# Complete AWS Deployment Guide

## Overview

This guide covers the complete end-to-end deployment of the data platform on AWS.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform 1.0+ installed
- Python 3.10+ with dependencies
- EMR Serverless application created

## Step-by-Step Deployment

### 1. Infrastructure Setup

```bash
cd aws/infra/terraform
terraform init
terraform apply
```

This creates:
- S3 buckets for Bronze/Silver/Gold layers
- EMR Serverless application
- IAM roles and policies
- Secrets Manager secrets
- CloudWatch log groups
- Glue catalog for Delta tables

### 2. Upload Code and Config

```bash
# Build wheel
python -m build

# Upload to artifacts bucket
aws s3 cp dist/*.whl s3://$ARTIFACTS_BUCKET/dist/
aws s3 sync src/ s3://$ARTIFACTS_BUCKET/src/
aws s3 sync jobs/ s3://$ARTIFACTS_BUCKET/jobs/
aws s3 sync config/ s3://$ARTIFACTS_BUCKET/config/
```

### 3. Deploy DAGs to MWAA

```bash
# Upload DAGs
aws s3 sync dags/ s3://$MWAA_BUCKET/dags/
```

### 4. Run First Job

```bash
# Submit via EMR Serverless
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN \
  --job-driver '{"sparkSubmit": {"entryPoint": "s3://.../jobs/hubspot_to_bronze.py"}}'
```

### 5. Verify Data

```bash
# Check Bronze layer
aws s3 ls s3://$LAKE_BUCKET/bronze/hubspot/contacts/

# Query via Athena
aws athena start-query-execution \
  --query-string "SELECT * FROM bronze.hubspot_contacts LIMIT 10"
```

## Post-Deployment

### Monitoring
- CloudWatch Logs: `/aws/emr-serverless/spark`
- Data Quality: `logs/dq_checks.log`
- Lineage: Emitted to OpenLineage

### Troubleshooting
See `docs/runbooks/RUNBOOK_AWS_2025.md`

## Production Hardening

- ✅ Enable encryption at rest
- ✅ Enable encryption in transit
- ✅ Enable VPC endpoints
- ✅ Set up alerts
- ✅ Configure backup/retention
- ✅ Document runbooks

