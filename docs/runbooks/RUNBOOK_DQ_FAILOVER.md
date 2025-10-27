# Data Quality Failover Runbook

## When DQ Fails

### Immediate Actions

1. **Stop Downstream Jobs**
   - Pause Airflow DAG if auto-pause enabled
   - Manually stop if needed

2. **Investigate Root Cause**
   ```bash
   aws logs filter-log-events \
     --log-group-name /aws/data-quality/failures \
     --start-time $(date -u -d '24 hours ago' +%s)000
   ```

3. **Check Last Known Good State**
   - Query Delta history: `DESCRIBE HISTORY silver.orders`
   - Check when last successful load occurred

### DQ Failure Types

#### Null Check Failure

**Symptoms**: More than 5% null values in critical column

**Investigation**:
1. Check upstream data source
2. Review recent schema changes
3. Check data pipeline logs

**Fix**:
- If upstream issue: Contact data provider
- If schema change: Update expectations
- If transient: Adjust threshold temporarily

**Resolution**: Re-run job after fix

#### Uniqueness Failure

**Symptoms**: Duplicate records detected in primary key

**Investigation**:
1. Query: `SELECT COUNT(*) FROM (SELECT pk, COUNT(*) as cnt FROM table GROUP BY pk HAVING cnt > 1)`
2. Check upstream deduplication logic
3. Review incremental loading logic

**Fix**:
- Drop duplicates: `df.drop_duplicates(['pk'])`
- Fix upstream issue
- Update deduplication logic

**Resolution**: Re-run job with fixed logic

#### Freshness Failure

**Symptoms**: Data not updated within expected time window

**Investigation**:
1. Check last ingest timestamp
2. Verify source system availability
3. Check EMR job status

**Fix**:
- Restart extraction job
- Fix upstream source
- Update ingestion schedule

**Resolution**: Verify fresh data loaded

## Post-Failure Recovery

### Verify Data Quality

```bash
python aws/scripts/run_ge_checks.py --suite silver_orders.yml --verbose
```

### Re-run Pipeline

```bash
# Trigger Airflow DAG
aws mwaa create-cli-token \
  --name $MWAA_ENV \
  --region $AWS_REGION

# Or submit directly
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN \
  --job-driver '{"sparkSubmit": {...}}'
```

### Monitor

- Watch CloudWatch logs
- Verify DQ checks pass
- Check downstream jobs resume
- Alert stakeholders if needed

## Prevention

1. **Strict Validation**: Don't allow bad data to propagate
2. **Early Detection**: Run DQ checks immediately after loads
3. **Automated Pause**: Configure DAG to auto-pause on DQ failure
4. **Alerting**: Set up CloudWatch alarms
5. **Documentation**: Update runbooks with lessons learned

