# Backfill and Recovery Runbook

This document describes procedures for controlled data backfill and recovery operations.

## Overview

When production issues occur or data corruption is detected, you need to selectively reprocess data without affecting the entire pipeline.

## Backfill Operations

### Bronze Layer Backfill

Reprocess raw data for a specific date and source.

#### Command:
```bash
./aws/scripts/backfill_bronze_for_date.sh <source> <date>
```

#### Examples:
```bash
# Backfill HubSpot contacts for October 26, 2025
./aws/scripts/backfill_bronze_for_date.sh hubspot 2025-10-26

# Backfill all sources for October 27, 2025
./aws/scripts/backfill_bronze_for_date.sh all 2025-10-27
```

#### What it does:
1. Submits EMR job with date filter
2. Reprocesses only the specified date partition
3. Writes to bronze layer with same partitioning
4. Does NOT touch other dates

#### Precautions:
- Verify source data is available for that date
- Check S3 permissions
- Monitor EMR job logs

### Silver Layer Rebuild

Rebuild Silver tables for specific date range.

#### Command:
```bash
./aws/scripts/rebuild_silver_for_date.sh --table <table> --date <date> --dry-run
```

#### Examples:
```bash
# Rebuild customers_clean for October 26
./aws/scripts/rebuild_silver_for_date.sh --table customers_clean --date 2025-10-26

# Dry run to preview changes
./aws/scripts/rebuild_silver_for_date.sh --table orders_clean --date 2025-10-26 --dry-run
```

#### What it does:
1. Reads Bronze data for specified date
2. Applies cleaning and standardization
3. Validates against schema contracts
4. Writes to Silver layer
5. Verifies data quality

### Gold Layer Refresh

Refresh Gold tables from Silver.

#### Command:
```bash
./aws/scripts/refresh_gold_for_date.sh --table <table> --date <date>
```

#### What it does:
1. Reads Silver data for specified date
2. Applies business logic and enrichment
3. Applies data masking (PII protection)
4. Writes to Gold layer
5. Registers to Glue Catalog

## Recovery Procedures

### Scenario 1: Missing Partition

**Symptoms**: Partition missing in Bronze/Silver/Gold

**Resolution**:
```bash
# Identify missing partition
aws s3 ls s3://company-data-lake-ACCOUNT_ID/bronze/orders/day=2025-10-26/

# Backfill specific partition
./aws/scripts/backfill_bronze_for_date.sh orders 2025-10-26
```

### Scenario 2: Data Corruption

**Symptoms**: Data quality checks fail, schema mismatches

**Resolution**:
```bash
# 1. Stop pipeline
# 2. Identify corrupted partitions
# 3. Delete corrupted data
# 4. Backfill from source
./aws/scripts/backfill_bronze_for_date.sh <source> <date>
```

### Scenario 3: Late-Arriving Data

**Symptoms**: Data arrives days after expected date

**Resolution**:
```bash
# Backfill bronze
./aws/scripts/backfill_bronze_for_date.sh <source> <date>

# Rebuild downstream
./aws/scripts/rebuild_silver_for_date.sh --table <table> --date <date>
./aws/scripts/refresh_gold_for_date.sh --table <table> --date <date>
```

## Validation Steps

After any backfill operation:

1. Check data quality:
   ```bash
   aws emr-serverless start-job-run \
       --application-id $EMR_APP_ID \
       --execution-role-arn $EXECUTION_ROLE \
       --job-driver file://dq_check.json
   ```

2. Verify row counts:
   ```sql
   SELECT COUNT(*) FROM bronze.orders WHERE day='2025-10-26';
   ```

3. Check lineage:
   ```bash
   aws logs filter-log-events \
       --log-group-name /aws/etl/lineage \
       --filter-pattern "2025-10-26"
   ```

## Monitoring

### CloudWatch Metrics
- `BackfillJobsStarted`
- `BackfillJobsSucceeded`
- `BackfillJobsFailed`
- `BackfillDurationSeconds`

### Logs Location
- S3: `s3://company-logs-ACCOUNT_ID/emr-serverless/backfill/`
- CloudWatch: `/aws/etl/backfill`

## Best Practices

1. **Always use --dry-run first**
2. **Monitor logs in real-time**
3. **Verify downstream dependencies**
4. **Document backfill reason**
5. **Update runbooks after incidents**

## Emergency Contacts

- **Data Engineering Lead**: data-eng-lead@company.com
- **Platform Team**: platform@company.com
- **On-Call Engineer**: Check PagerDuty

## Post-Incident

After backfill completion:

1. Update incident log
2. Update data quality report
3. Notify stakeholders
4. Schedule post-mortem if needed
