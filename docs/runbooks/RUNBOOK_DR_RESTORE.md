# Disaster Recovery Restore Runbook

## Overview

This runbook provides step-by-step instructions for restoring the data lake from the DR region (`us-west-2`) to the primary region (`us-east-1`) in the event of a disaster.

## Prerequisites

- AWS CLI configured with appropriate permissions
- Access to both primary and DR regions
- Terraform access for infrastructure recreation
- Data engineering team on-call contact

## Disaster Scenarios

### Scenario 1: Primary Region S3 Bucket Corruption
- **Symptom**: Data corruption detected in primary bucket
- **Action**: Restore from DR snapshot

### Scenario 2: Primary Region Outage
- **Symptom**: `us-east-1` region unavailable
- **Action**: Point Athena/EMR to DR bucket in `us-west-2`

### Scenario 3: Accidental Deletion
- **Symptom**: Critical tables deleted in primary
- **Action**: Restore specific tables from DR

## Recovery Procedures

### Option A: Point to DR Bucket (Fastest - Read-Only)

**Use Case**: Primary region outage, need immediate read access

**Steps**:

1. **Verify DR Snapshot Exists**
   ```bash
   aws s3 ls s3://company-data-lake-dr-ACCOUNT_ID/dr_snapshots/latest_snapshot_metadata.json --region us-west-2
   ```

2. **Update Glue Catalog to Point to DR Bucket**
   ```bash
   # Run catalog registration against DR bucket
   python aws/scripts/register_glue_tables.py \
     --db silver \
     --root s3://company-data-lake-dr-ACCOUNT_ID/silver \
     --region us-west-2
   ```

3. **Update EMR/Athena Configuration**
   ```bash
   # In EMR cluster config or Athena workgroup, update:
   # lake.root = s3://company-data-lake-dr-ACCOUNT_ID
   ```

4. **Test Access**
   ```sql
   -- Query from Athena pointing to DR region
   SELECT COUNT(*) FROM silver.dim_customer;
   SELECT COUNT(*) FROM gold.fact_sales;
   ```

5. **Notify Stakeholders**
   - Email: data-eng@company.com
   - Slack: #data-pipeline-alerts
   - Status: "Data lake now serving from DR region (us-west-2)"

**Duration**: ~15 minutes  
**Downtime**: Minimal (only writes affected, reads continue)

### Option B: Full Restore to Primary Region

**Use Case**: Need full restore including write capability

**Steps**:

1. **Stop All Pipeline Jobs**
   ```bash
   # Pause Airflow DAGs
   airflow dags pause daily_batch_pipeline
   airflow dags pause salesforce_ingestion_dag
   ```

2. **Verify DR Snapshot Metadata**
   ```bash
   aws s3 cp s3://company-data-lake-dr-ACCOUNT_ID/dr_snapshots/latest_snapshot_metadata.json - --region us-west-2
   ```

3. **Restore Silver Layer**
   ```bash
   # Copy from DR to primary
   aws s3 sync \
     s3://company-data-lake-dr-ACCOUNT_ID/silver \
     s3://company-data-lake-ACCOUNT_ID/silver \
     --region us-west-2 \
     --source-region us-west-2
   ```

4. **Restore Gold Layer**
   ```bash
   aws s3 sync \
     s3://company-data-lake-dr-ACCOUNT_ID/gold \
     s3://company-data-lake-ACCOUNT_ID/gold \
     --region us-west-2 \
     --source-region us-west-2
   ```

5. **Register Glue Tables in Primary Region**
   ```bash
   python aws/scripts/register_glue_tables.py \
     --db silver \
     --root s3://company-data-lake-ACCOUNT_ID/silver \
     --region us-east-1
     
   python aws/scripts/register_glue_tables.py \
     --db gold \
     --root s3://company-data-lake-ACCOUNT_ID/gold \
     --region us-east-1
   ```

6. **Verify Data Integrity**
   ```sql
   -- Run data quality checks
   python aws/jobs/dq_check_silver.py --config config/prod.yaml
   python aws/jobs/dq_check_gold.py --config config/prod.yaml
   ```

7. **Resume Pipeline Jobs**
   ```bash
   airflow dags unpause daily_batch_pipeline
   airflow dags unpause salesforce_ingestion_dag
   ```

8. **Monitor First Run**
   - Check Airflow DAG execution
   - Verify Delta table consistency
   - Monitor CloudWatch metrics

**Duration**: 2-4 hours (depends on data volume)  
**Downtime**: Complete downtime during restore

### Option C: Selective Table Restore

**Use Case**: Only specific tables corrupted/deleted

**Steps**:

1. **Identify Tables to Restore**
   ```bash
   # List available tables in DR
   aws s3 ls s3://company-data-lake-dr-ACCOUNT_ID/silver/ --recursive --region us-west-2
   ```

2. **Restore Specific Table**
   ```bash
   # Example: Restore dim_customer
   aws s3 sync \
     s3://company-data-lake-dr-ACCOUNT_ID/silver/dim_customer \
     s3://company-data-lake-ACCOUNT_ID/silver/dim_customer \
     --region us-west-2 \
     --source-region us-west-2
   ```

3. **Update Glue Table Metadata**
   ```bash
   python aws/scripts/register_glue_tables.py \
     --db silver \
     --root s3://company-data-lake-ACCOUNT_ID/silver \
     --tables dim_customer \
     --region us-east-1
   ```

4. **Verify and Test**
   ```sql
   SELECT COUNT(*), MAX(updated_at) FROM silver.dim_customer;
   ```

**Duration**: 30 minutes - 2 hours (per table)  
**Downtime**: Minimal (only affected table unavailable)

## Automation Script

Use `aws/scripts/dr_restore.py` for automated restore:

```bash
# Full restore
python aws/scripts/dr_restore.py \
  --mode full \
  --dr-bucket s3://company-data-lake-dr-ACCOUNT_ID \
  --primary-bucket s3://company-data-lake-ACCOUNT_ID \
  --layers silver gold \
  --config config/prod.yaml

# Selective restore
python aws/scripts/dr_restore.py \
  --mode selective \
  --tables silver.dim_customer gold.fact_sales \
  --config config/prod.yaml
```

## Post-Restore Validation

### Data Integrity Checks

1. **Record Count Validation**
   ```sql
   -- Compare record counts
   SELECT 
     (SELECT COUNT(*) FROM silver.dim_customer) as silver_count,
     (SELECT COUNT(*) FROM gold.fact_sales) as gold_count;
   ```

2. **Schema Validation**
   ```bash
   python aws/jobs/dq_check_silver.py --config config/prod.yaml
   ```

3. **Freshness Check**
   ```sql
   SELECT 
     MAX(updated_at) as latest_update,
     CURRENT_TIMESTAMP as now,
     DATEDIFF(day, MAX(updated_at), CURRENT_TIMESTAMP) as days_behind
   FROM gold.fact_sales;
   ```

### Pipeline Health Check

1. **Run Sample Pipeline Job**
   ```bash
   python aws/jobs/dq_check_bronze.py --config config/prod.yaml
   ```

2. **Monitor CloudWatch Metrics**
   - Job runtime
   - Records processed
   - Error rates

3. **Check Airflow DAG Status**
   ```bash
   airflow dags list-runs daily_batch_pipeline --state success --limit 5
   ```

## Communication Plan

### Immediate (Within 15 minutes)
- [ ] Notify data engineering team
- [ ] Post to #data-pipeline-alerts Slack channel
- [ ] Create incident ticket

### During Restore
- [ ] Provide status updates every 30 minutes
- [ ] Document any issues encountered
- [ ] Estimate completion time

### Post-Restore
- [ ] Verify data access restored
- [ ] Confirm pipeline jobs running
- [ ] Close incident ticket
- [ ] Post-mortem scheduled (within 48 hours)

## Prevention and Monitoring

### Regular DR Snapshots

DR snapshots are created:
- **Daily**: Full Silver/Gold export (runs at 2 AM UTC)
- **Weekly**: Complete snapshot with metadata
- **On-Demand**: Before major changes

### Monitoring

CloudWatch alarms monitor:
- DR snapshot job success/failure
- DR bucket size and growth
- Cross-region sync latency

### Testing

Quarterly DR drill:
1. Test restore procedure (Option C - selective)
2. Verify data integrity
3. Document lessons learned
4. Update runbook based on findings

## Rollback Procedure

If restore fails or issues discovered:

1. **Stop Restore Process**
   ```bash
   # Cancel in-progress S3 sync
   # (No direct cancel - wait or let complete)
   ```

2. **Investigate Issue**
   - Check CloudWatch logs
   - Verify DR snapshot integrity
   - Check IAM permissions

3. **Retry with Different Approach**
   - Try Option A (point to DR) instead
   - Or selective restore (Option C)

4. **Escalate if Needed**
   - Contact AWS support
   - Engage data engineering lead

## Common Issues and Solutions

### Issue: "Access Denied" during S3 sync
**Solution**: Verify IAM role has cross-region S3 permissions:
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::company-data-lake-dr-*",
    "arn:aws:s3:::company-data-lake-dr-*/*"
  ]
}
```

### Issue: Glue table registration fails
**Solution**: Check table schemas match DR region schemas:
```bash
aws glue get-table --database-name silver --name dim_customer --region us-west-2 > dr_table.json
aws glue get-table --database-name silver --name dim_customer --region us-east-1 > primary_table.json
diff dr_table.json primary_table.json
```

### Issue: Data looks corrupted after restore
**Solution**: 
1. Re-check DR snapshot metadata for export time
2. Verify Delta table log files copied correctly
3. Run Delta table repair:
```sql
REPAIR TABLE silver.dim_customer;
```

## Contact Information

**On-Call Engineer**: Check PagerDuty rotation  
**Data Engineering Lead**: data-eng-lead@company.com  
**AWS Support**: enterprise-support@amazon.com  
**Emergency Escalation**: CTO office

## Appendix

### DR Snapshot Locations

- **Silver Layer**: `s3://company-data-lake-dr-ACCOUNT_ID/silver/`
- **Gold Layer**: `s3://company-data-lake-dr-ACCOUNT_ID/gold/`
- **Metadata**: `s3://company-data-lake-dr-ACCOUNT_ID/dr_snapshots/`

### Related Documentation

- [Data Access Governance](./DATA_ACCESS_GOVERNANCE.md)
- [Pipeline Runbook](./RUNBOOK_AWS_2025.md)
- [Cost and Retention](./COST_AND_RETENTION.md)

---

**Last Updated**: 2024-01-15  
**Reviewed By**: Data Engineering Team  
**Next Review**: 2024-04-15

