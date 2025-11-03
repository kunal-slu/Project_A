# Runbook: Streaming Checkpoint Reset Procedure

## Purpose

Reset Kafka streaming checkpoints when:
- Stream is stuck/frozen
- Checkpoint corruption detected
- Need to reprocess historical data
- Schema changes require reprocessing

---

## Pre-Reset Checklist

- [ ] Confirm stream is actually stuck (check metrics)
- [ ] Check for checkpoint corruption
- [ ] Backup existing checkpoints
- [ ] Notify stakeholders if reprocessing historical data
- [ ] Document reason for reset

---

## Procedure

### Step 1: Verify Stream Status

```bash
# Check CloudWatch metrics
aws cloudwatch get-metric-statistics \
  --namespace ETLPipelineMetrics \
  --metric-name records_processed_total \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum

# Check checkpoint directory
aws s3 ls s3://bucket/_checkpoints/streaming/orders/ --recursive

# Check for errors in logs
aws logs tail /aws/emr-serverless/application-logs --follow
```

### Step 2: Backup Checkpoints

```bash
# Create backup
BACKUP_DATE=$(date +%Y%m%d_%H%M%S)
aws s3 sync \
  s3://bucket/_checkpoints/streaming/orders/ \
  s3://bucket/_checkpoints/streaming/orders_backup_${BACKUP_DATE}/

echo "✅ Checkpoints backed up to: orders_backup_${BACKUP_DATE}"
```

### Step 3: Stop Streaming Job

```bash
# In Airflow, mark task as failed/upstream_failed
# Or via EMR Serverless API
aws emr-serverless cancel-job-run \
  --application-id $EMR_APP_ID \
  --job-run-id $JOB_RUN_ID
```

### Step 4: Reset Checkpoint (Choose Option)

**Option A: Reset to Latest (Resume from Now)**

```bash
# Delete checkpoint directory
aws s3 rm s3://bucket/_checkpoints/streaming/orders/ --recursive

# Streaming will resume from latest Kafka offset
```

**Option B: Reset to Specific Timestamp**

```bash
# Delete checkpoint
aws s3 rm s3://bucket/_checkpoints/streaming/orders/ --recursive

# In streaming job config, set:
startingOffsets: '2025-01-15 00:00:00'  # Specific timestamp

# Or for Kafka:
startingOffsets: '{"orders_events":{"0":12345}}'  # Specific offset
```

**Option C: Reprocess All Data**

```bash
# Delete checkpoint
aws s3 rm s3://bucket/_checkpoints/streaming/orders/ --recursive

# Set to earliest
startingOffsets: 'earliest'

# WARNING: This will reprocess ALL historical data!
```

### Step 5: Restart Streaming Job

```bash
# Via Airflow
# Re-run streaming task

# Or via EMR Serverless
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://bucket/jobs/kafka_orders_stream.py",
      "sparkSubmitParameters": "--conf spark.sql.streaming.checkpointLocation=s3://bucket/_checkpoints/streaming/orders/"
    }
  }'
```

### Step 6: Verify Stream Health

```bash
# Check metrics (should see new records)
aws cloudwatch get-metric-statistics ...

# Check logs for errors
aws logs tail /aws/emr-serverless/application-logs --follow

# Verify data in bronze
aws s3 ls s3://bucket/bronze/kafka/orders/ --recursive | tail -10
```

---

## Troubleshooting

### Stream Still Stuck After Reset

```bash
# 1. Check Kafka connectivity
aws kafka describe-cluster --cluster-arn $KAFKA_CLUSTER_ARN

# 2. Check IAM permissions
aws iam get-role-policy --role-name EmrServerlessJobExecutionRole --policy-name KafkaAccess

# 3. Check checkpoint permissions
aws s3api head-object --bucket bucket --key _checkpoints/streaming/orders/
```

### Checkpoint Corruption

```bash
# Check checkpoint metadata
aws s3 cp s3://bucket/_checkpoints/streaming/orders/metadata - | jq .

# If corrupted, restore from backup:
aws s3 sync \
  s3://bucket/_checkpoints/streaming/orders_backup_${BACKUP_DATE}/ \
  s3://bucket/_checkpoints/streaming/orders/
```

### High Backlog After Reset

```bash
# If reprocessing creates large backlog:
# 1. Increase parallelism
# spark.sql.streaming.maxFilesPerTrigger: 1000

# 2. Increase executor memory
# spark.executor.memory: 16g

# 3. Process in batches
# Process last 7 days first, then backfill older
```

---

## Recovery Time Objectives (RTO)

| Scenario | Expected Recovery Time |
|----------|----------------------|
| Simple checkpoint reset | 5-10 minutes |
| Reset + reprocess 24h | 30-60 minutes |
| Reset + reprocess 7d | 2-4 hours |
| Full reprocess | 8+ hours (depends on volume) |

---

## Prevention

**Best Practices:**
1. ✅ Monitor checkpoint health (CloudWatch metrics)
2. ✅ Set up alerts for stream lag
3. ✅ Regular checkpoint validation
4. ✅ Backup checkpoints weekly
5. ✅ Test checkpoint recovery quarterly

---

## Contact

- **Streaming Team:** streaming-oncall@company.com
- **PagerDuty:** Check for "Streaming" escalation policy
- **Emergency:** +1-XXX-XXX-XXXX (Data Engineering on-call)

---

**Last Updated:** 2025-01-15  
**Owner:** Data Engineering - Streaming Team

