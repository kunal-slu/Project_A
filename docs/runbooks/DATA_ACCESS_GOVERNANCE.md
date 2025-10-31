# Data Access Governance - Lake Formation Permissions

## Overview

This document describes how AWS Lake Formation (LF) enforces fine-grained access control across the data lake, ensuring that different roles have appropriate permissions while protecting PII and sensitive data.

## Access Control Model

### Layer-Based Access

```
Bronze Layer (Raw Data)
├── Role: data_engineer
│   └── Access: READ/WRITE all tables
│
Silver Layer (Cleaned Data)
├── Role: data_engineer
│   └── Access: READ/WRITE all tables
├── Role: data_analyst
│   └── Access: READ with PII masking
│   └── Access: READ non-PII columns only
├── Role: data_scientist
│   └── Access: READ with PII masking
│   └── Access: READ non-PII columns
│
Gold Layer (Business-Ready)
├── Role: data_analyst
│   └── Access: READ all tables (no PII in Gold)
├── Role: data_scientist
│   └── Access: READ all tables
├── Role: finance_analyst
│   └── Access: READ revenue/financial metrics only
```

## Lake Formation Tags (LF-Tags)

LF-Tags are metadata labels attached to tables and columns to enforce permissions.

### Tag Definitions

| Tag Name | Tag Values | Purpose |
|----------|-----------|---------|
| `data_classification` | `public`, `internal`, `confidential`, `restricted` | Data sensitivity level |
| `contains_pii` | `true`, `false` | PII indicator |
| `layer` | `bronze`, `silver`, `gold` | Data lake layer |
| `source_system` | `crm`, `snowflake`, `redshift`, `kafka`, `fx` | Source system origin |
| `business_unit` | `sales`, `marketing`, `finance`, `operations` | Business ownership |

### Tag Attachment Examples

#### Bronze Layer Tables
```sql
-- CRM contacts table
ALTER TABLE bronze.crm_contacts 
SET TBLPROPERTIES (
    'lf_tags' = 'layer=bronze,data_classification=confidential,contains_pii=true,source_system=crm'
);
```

#### Silver Layer Tables (with Column-Level Tags)
```sql
-- Silver customers table
ALTER TABLE silver.dim_customer 
SET TBLPROPERTIES (
    'lf_tags' = 'layer=silver,data_classification=internal,contains_pii=true'
);

-- Tag specific columns
ALTER TABLE silver.dim_customer 
ALTER COLUMN email SET TBLPROPERTIES (
    'lf_tags' = 'contains_pii=true,data_classification=confidential'
);
```

## IAM Role Mapping

### Role Definitions

#### 1. Data Engineer Role (`DataEngineerRole`)
**Purpose**: Full access to all layers for pipeline operations

**Permissions**:
- ✅ READ/WRITE bronze, silver, gold tables
- ✅ CREATE/DELETE tables
- ✅ ALTER table schemas
- ✅ Access to raw source data

**IAM Policy Example**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::company-data-lake-*/*"
      ]
    }
  ]
}
```

**Lake Formation Grants**:
```sql
-- Grant to data engineer role
GRANT ALL ON DATABASE bronze TO ROLE 'DataEngineerRole';
GRANT ALL ON DATABASE silver TO ROLE 'DataEngineerRole';
GRANT ALL ON DATABASE gold TO ROLE 'DataEngineerRole';
```

#### 2. Data Analyst Role (`DataAnalystRole`)
**Purpose**: Read-only access to Silver and Gold with PII masking

**Permissions**:
- ✅ READ gold tables (all columns)
- ✅ READ silver tables (PII columns masked)
- ❌ No access to bronze layer
- ❌ No write permissions

**Lake Formation Grants**:
```sql
-- Grant read on Gold
GRANT SELECT ON DATABASE gold TO ROLE 'DataAnalystRole';

-- Grant read on Silver with column exclusion for PII
GRANT SELECT ON DATABASE silver TO ROLE 'DataAnalystRole'
WITH GRANT OPTION FALSE;

-- Explicitly deny access to bronze
REVOKE ALL ON DATABASE bronze FROM ROLE 'DataAnalystRole';
```

**Column-Level Masking**:
```sql
-- Create view with masked PII
CREATE VIEW silver.dim_customer_masked AS
SELECT 
    customer_id,
    first_name,
    -- Mask email: replace with hash
    CONCAT(SUBSTRING(email, 1, 2), '***', SUBSTRING(email, POSITION('@' IN email))) AS email,
    -- Mask phone: show last 4 digits only
    CONCAT('***-***-', RIGHT(phone, 4)) AS phone,
    account_id,
    created_date
FROM silver.dim_customer;
```

#### 3. Data Scientist Role (`DataScientistRole`)
**Purpose**: Access to Silver and Gold with controlled PII access

**Permissions**:
- ✅ READ gold tables
- ✅ READ silver tables (with PII masking by default)
- ✅ READ unmasked PII with approval workflow
- ❌ No access to bronze layer
- ❌ No write permissions

**Lake Formation Grants**:
```sql
-- Grant read on Gold and Silver
GRANT SELECT ON DATABASE gold TO ROLE 'DataScientistRole';
GRANT SELECT ON DATABASE silver TO ROLE 'DataScientistRole';

-- Grant access to masked views only
GRANT SELECT ON silver.dim_customer_masked TO ROLE 'DataScientistRole';
```

#### 4. Finance Analyst Role (`FinanceAnalystRole`)
**Purpose**: Access to financial metrics only

**Permissions**:
- ✅ READ gold.fact_sales (revenue columns only)
- ✅ READ gold marketing_attribution
- ❌ No access to customer PII
- ❌ No access to bronze/silver layers

**Lake Formation Grants**:
```sql
-- Grant access to specific Gold tables only
GRANT SELECT (
    order_id,
    order_date,
    total_amount,
    quantity,
    order_year,
    order_quarter
) ON gold.fact_sales TO ROLE 'FinanceAnalystRole';

-- Deny access to customer identifying columns
REVOKE SELECT (
    customer_id,
    product_id
) ON gold.fact_sales FROM ROLE 'FinanceAnalystRole';
```

## Row-Level Security (RLS)

### Region-Based Filtering

Example: Restrict access by geographic region

```sql
-- Create filtered view for US analysts only
CREATE VIEW gold.fact_sales_us AS
SELECT *
FROM gold.fact_sales
WHERE shipping_country = 'United States';

-- Grant access to filtered view
GRANT SELECT ON gold.fact_sales_us TO ROLE 'USAnalystRole';
```

### Time-Based Filtering

Example: Restrict access to recent data only

```sql
-- Create view with 30-day lookback
CREATE VIEW gold.fact_sales_recent AS
SELECT *
FROM gold.fact_sales
WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY;

GRANT SELECT ON gold.fact_sales_recent TO ROLE 'AnalystRole';
```

## Implementation in Terraform

See `aws/terraform/lake_formation.tf` for complete infrastructure as code:

```hcl
# Lake Formation database permissions
resource "aws_lakeformation_permissions" "data_analyst_gold" {
  principal   = aws_iam_role.data_analyst_role.arn
  database {
    name = aws_glue_catalog_database.gold.name
  }
  permissions = ["SELECT"]
}

# Column-level permissions with tag-based access
resource "aws_lakeformation_permissions" "data_analyst_silver_masked" {
  principal   = aws_iam_role.data_analyst_role.arn
  table {
    database_name = aws_glue_catalog_database.silver.name
    name          = "dim_customer_masked"
  }
  permissions = ["SELECT"]
  table_with_columns {
    database_name = aws_glue_catalog_database.silver.name
    name          = "dim_customer_masked"
    column_names  = ["customer_id", "first_name", "last_name", "account_id"]
  }
}
```

## Tagging Strategy Script

Use `aws/scripts/lf_tags_seed.py` to automatically tag all tables:

```bash
python aws/scripts/lf_tags_seed.py \
  --env prod \
  --tag-layer bronze \
  --tag-classification confidential \
  --tag-contains-pii true
```

## Access Request Workflow

For unmasked PII access:

1. **Request**: Data scientist requests unmasked PII via ServiceNow/Jira
2. **Approval**: Data governance team reviews and approves
3. **Temporary Grant**: LF permission granted for 30 days
4. **Audit**: Access logged to CloudTrail
5. **Expiration**: Automatic revocation after expiration

## Audit and Compliance

### CloudTrail Integration

All LF operations are logged to CloudTrail:
- Permission grants/revocations
- Tag assignments
- Table access attempts

### Access Logging

Query access logs:
```sql
-- Query Athena access logs
SELECT 
    useridentity.arn,
    eventsource,
    eventname,
    awsregion,
    requestparameters.tablename,
    eventtime
FROM cloudtrail_logs
WHERE eventsource = 'lakeformation.amazonaws.com'
  AND eventtime >= CURRENT_DATE - 7
ORDER BY eventtime DESC;
```

## Troubleshooting

### Issue: "Access Denied" when querying table

**Check**:
1. Verify IAM role has LF permissions
2. Check table/column tags match role grants
3. Verify database-level permissions

**Solution**:
```sql
-- Check grants for role
SHOW GRANTS ON TABLE silver.dim_customer FOR ROLE 'DataAnalystRole';

-- Check LF tags on table
SHOW TBLPROPERTIES silver.dim_customer;
```

### Issue: PII columns visible when should be masked

**Check**:
1. Verify masked view exists
2. Confirm querying masked view, not source table
3. Check column-level permissions

**Solution**:
```sql
-- Ensure using masked view
SELECT * FROM silver.dim_customer_masked;  -- ✅ Correct
SELECT * FROM silver.dim_customer;         -- ❌ Wrong (direct access)
```

## Best Practices

1. **Principle of Least Privilege**: Grant minimum required access
2. **Tag Consistently**: Apply LF-tags to all tables and sensitive columns
3. **Use Views for Masking**: Create masked views rather than modifying source data
4. **Regular Audits**: Review permissions quarterly
5. **Document Exceptions**: Track temporary elevated access grants

## Support

- **Data Governance Team**: data-governance@company.com
- **Access Requests**: ServiceNow ticket
- **Emergency Access**: Contact on-call data engineer

---

**Last Updated**: 2024-01-15  
**Maintained By**: Data Governance Team

