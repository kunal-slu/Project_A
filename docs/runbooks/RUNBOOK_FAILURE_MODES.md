# Runbook: Failure Modes and Recovery

This document outlines common failure modes in the data pipeline and procedures for recovery.
For **job-by-job troubleshooting steps and rerun commands**, see
`docs/runbooks/JOB_SPECIFIC_FAILURES.md`.

## Failure Categories

### 1. Data Quality Failures

**Symptoms**:
- DQ checks fail in `dq_watchdog_dag`
- Quality gate tests fail
- Data missing expected columns or values

**Recovery**:
1. Check DQ failure details in CloudWatch logs
2. Identify which expectations failed
3. If upstream issue:
   - Notify source system owners
   - Backfill from corrected source
4. If transformation issue:
   - Fix transformation logic
   - Re-run affected date range via backfill scripts
5. Verify fix with quality gate tests

**Prevention**:
- Enable quality gates in DAGs
- Set appropriate DQ thresholds
- Monitor DQ trends over time

### 2. Schema Evolution Failures

**Symptoms**:
- `SchemaValidationException`
- Column type mismatches
- Missing required fields

**Recovery**:
1. Check schema registry for expected schema
2. Compare with actual data schema
3. Apply schema evolution (add null defaults, widen types)
4. Re-process affected data
5. Update schema contracts if intentional change

**Prevention**:
- Validate schemas at Bronze ingestion
- Follow schema evolution policy
- Test schema changes in dev first

### 3. Upstream Source Failures

**Symptoms**:
- No data arriving from source
- Source API timeouts
- Invalid data format

**Recovery**:
1. Verify source system status
2. Check source authentication (Secrets Manager)
3. Review source logs/API response
4. Contact source system owners if external
5. Once resolved, backfill missing date range

**Prevention**:
- Implement retry logic with exponential backoff
- Monitor source data freshness
- Set up alerts for missing data

### 4. Resource Exhaustion (EMR/Airflow)

**Symptoms**:
- EMR jobs fail with memory errors
- Airflow tasks timeout
- Spark OOM errors

**Recovery**:
1. Check CloudWatch metrics for resource usage
2. Increase EMR serverless driver/executor memory
3. Optimize Spark job (reduce data scanned, repartition)
4. Consider splitting large jobs
5. Retry job

**Prevention**:
- Right-size EMR applications
- Monitor job resource usage
- Set appropriate Spark configurations
- Use incremental processing

### 5. Network/Connectivity Issues

**Symptoms**:
- S3 access denied errors
- Secrets Manager timeout
- External API failures

**Recovery**:
1. Verify IAM permissions
2. Check VPC/networking configuration
3. Verify security groups allow required traffic
4. Check AWS service status
5. Retry operation (may be transient)

**Prevention**:
- Use VPC endpoints for S3/Glue
- Implement proper retry logic
- Monitor network health

## Dead Letter Queues (DLQs)

### S3 DLQ Structure
```
s3://{bucket}/_errors/{dataset}/{dt=YYYY-MM-DD}/
  ├── rejected_rows.parquet    # Bad rows
  ├── error_reasons.json        # Error metadata
  └── _SUCCESS                  # Completion marker
```

### Error Metadata Format
```json
{
  "error_type": "schema_validation",
  "error_message": "Column 'email' expected string, got null",
  "row_count": 150,
  "timestamp": "2024-01-15T10:30:00Z",
  "source_path": "s3://bucket/bronze/crm/contacts/dt=2024-01-15/"
}
```

### DLQ Processing
1. Monitor `_errors/` prefix in S3
2. Alert on DLQ entries
3. Investigate and fix root cause
4. Re-process DLQ data once fixed
5. Archive processed DLQ data

## Retry and Backoff Policy

### EMR Serverless Jobs
- **Max Retries**: 3
- **Initial Backoff**: 30 seconds
- **Max Backoff**: 5 minutes
- **Backoff Multiplier**: 2x

### Airflow Tasks
- **Retries**: 3
- **Retry Delay**: 5 minutes
- **Timeout**: 1 hour (configurable per task)

### Implementation
```python
# In DAG tasks
task = BashOperator(
    task_id="ingest_bronze",
    bash_command="python aws/jobs/ingest/crm_accounts_ingest.py",
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(hours=1)
)
```

## Quarantine Process

### When to Quarantine
- Data fails DQ checks repeatedly
- Schema violations that can't be auto-fixed
- Suspicious data patterns (anomalies)

### Quarantine Location
```
s3://{bucket}/_quarantine/{dataset}/{dt=YYYY-MM-DD}/{reason}/
```

### Quarantine Workflow
1. Move data to quarantine path
2. Log quarantine reason
3. Alert data engineering team
4. Investigate root cause
5. After resolution:
   - Release from quarantine (if fixable)
   - Or archive permanently (if unfixable)

## Alerting Thresholds

### Critical (Immediate Response)
- Quality gate failure
- No data for > 1 hour
- EMR job failure rate > 50%
- DLQ entries > 1000 rows

### Warning (Monitor)
- Quality pass rate < 99%
- Job duration > 2x average
- DLQ entries > 100 rows
- Schema drift detected

### Info (Logging)
- Job completion
- Normal quality checks passing
- Successful backfills

## Recovery Procedures by Layer

### Bronze Layer
1. Re-run ingestion for affected date range
2. Verify source data is available
3. Check schema compatibility
4. Move to DLQ if unfixable

### Silver Layer
1. Identify affected transformations
2. Re-run transformation jobs
3. Verify upstream Bronze data
4. Validate output schema

### Gold Layer
1. Re-run analytics jobs
2. Verify upstream Silver data
3. Check business logic
4. Validate output metrics

## Post-Recovery Verification

After recovery, verify:
1. ✅ Data quality checks pass
2. ✅ Record counts match expected
3. ✅ Data freshness is within SLA
4. ✅ No new errors in logs
5. ✅ Downstream consumers can query data

---

**Last Updated**: 2024-01-15  
**Version**: 1.0.0  
**Maintained By**: Data Engineering Team
