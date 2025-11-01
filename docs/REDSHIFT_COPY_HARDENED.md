# Redshift COPY - Hardened Security Setup

## Overview

This document describes the production-ready security setup for loading data from S3 into Redshift using COPY commands with IAM role-based authentication.

## Problem Statement

In development, we temporarily made S3 objects public to test Redshift COPY. For production, we must:
1. Remove public access
2. Use IAM roles for authentication
3. Enable encryption in transit and at rest
4. Implement least-privilege access

## Solution: IAM Role-Based Access

### Step 1: Create IAM Role for Redshift

```bash
# Create IAM role
aws iam create-role \
  --role-name RedshiftS3ReadRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "redshift.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  }'
```

### Step 2: Attach S3 Read Policy

```bash
# Create policy document
cat > redshift-s3-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-etl-lake-demo/*",
        "arn:aws:s3:::my-etl-lake-demo"
      ]
    }
  ]
}
EOF

# Attach policy to role
aws iam put-role-policy \
  --role-name RedshiftS3ReadRole \
  --policy-name RedshiftS3ReadPolicy \
  --policy-document file://redshift-s3-policy.json
```

### Step 3: Attach Role to Redshift Namespace

**Important**: The `ALTER NAMESPACE` SQL command may not work in all Redshift versions. Use the AWS Console or CLI:

**Via AWS Console**:
1. Go to Redshift → Namespaces
2. Select your namespace
3. Click "Manage IAM roles"
4. Attach `RedshiftS3ReadRole`

**Via AWS CLI**:
```bash
aws redshift-serverless associate-iam-role \
  --namespace-name your-namespace \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/RedshiftS3ReadRole
```

### Step 4: Update S3 Bucket Policy

**Remove public access** and add explicit Redshift role access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowRedshiftRead",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::ACCOUNT_ID:role/RedshiftS3ReadRole"
      },
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-etl-lake-demo/*",
        "arn:aws:s3:::my-etl-lake-demo"
      ]
    }
  ]
}
```

### Step 5: Update COPY Command

```sql
COPY analytics.customer_behavior
FROM 's3://my-etl-lake-demo/bronze/redshift/behavior/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftS3ReadRole'
FORMAT PARQUET
COMPUPDATE OFF
STATUPDATE ON;
```

**Alternative**: Use `CREDENTIALS 'aws_iam_role=...'` if role is attached to cluster/namespace.

### Step 6: Remove Public Access

```bash
# Block public access
aws s3api put-public-access-block \
  --bucket my-etl-lake-demo \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Remove any public ACLs
aws s3api put-bucket-acl \
  --bucket my-etl-lake-demo \
  --acl private
```

## Encryption

### Encryption at Rest

Redshift tables are encrypted by default with AWS KMS. Ensure KMS key exists:

```sql
-- Verify encryption
SELECT * FROM pg_table_def WHERE schemaname = 'analytics';
```

### Encryption in Transit

Enable SSL for Redshift connections:

```python
# In JDBC connection string
jdbc:redshift://host:5439/database?ssl=true&sslmode=require
```

## Troubleshooting

### Error: "Access Denied"

1. **Check IAM role**: Verify `RedshiftS3ReadRole` exists and has S3 permissions
2. **Check namespace attachment**: Verify role is attached to namespace
3. **Check bucket policy**: Ensure bucket allows the role ARN
4. **Check object ACLs**: Ensure objects don't require public access

### Error: "Invalid IAM Role"

1. Trust relationship: Role must trust `redshift.amazonaws.com`
2. Role ARN format: Must match exactly (include account ID, region if applicable)

### Verification Query

```sql
-- Check if COPY succeeded
SELECT COUNT(*) FROM analytics.customer_behavior;

-- Check for errors
SELECT * FROM stl_load_errors 
WHERE query = pg_last_copy_id() 
ORDER BY starttime DESC;
```

## Terraform Implementation

See `aws/terraform/redshift_iam.tf`:

```hcl
resource "aws_iam_role" "redshift_s3_read" {
  name = "RedshiftS3ReadRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "redshift.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3_read" {
  name = "RedshiftS3ReadPolicy"
  role = aws_iam_role.redshift_s3_read.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket"
      ]
      Resource = [
        "arn:aws:s3:::${var.data_lake_bucket}/*",
        "arn:aws:s3:::${var.data_lake_bucket}"
      ]
    }]
  })
}
```

## Lake Formation Integration

For Lake Formation managed tables, use LF permissions instead of direct IAM:

```sql
-- Grant LF permissions
GRANT SELECT ON TABLE bronze.behavior TO ROLE redshift_analyst_role;
```

Then use Lake Formation credentials in COPY:

```sql
COPY analytics.customer_behavior
FROM 's3://my-etl-lake-demo/bronze/redshift/behavior/'
LAKE_FORMATION_ROLE 'arn:aws:iam::ACCOUNT_ID:role/LakeFormationDataRole'
FORMAT PARQUET;
```

## Summary

✅ **IAM Role**: `RedshiftS3ReadRole` with S3 read permissions  
✅ **Attached to Namespace**: Via console/CLI (not SQL)  
✅ **Bucket Policy**: Explicit allow for role ARN  
✅ **Public Access**: Blocked  
✅ **Encryption**: KMS at rest, SSL in transit  
✅ **Least Privilege**: Only `s3:GetObject` and `s3:ListBucket` on data lake bucket

---

**Next Steps**: See `docs/guides/DATA_GOVERNANCE.md` for Lake Formation setup.

