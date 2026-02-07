# Streaming Recovery Runbook

## Kafka/Kinesis Streaming Issues

### Consumer Lag Detected

#### Symptoms
- CloudWatch metric `Lag` increases
- Alert triggered: `KafkaLagThreshold`
- Recent data missing

#### Investigation

1. **Check Current Lag**
   ```bash
   aws kinesis get-records \
     --shard-iterator $ITERATOR \
     --limit 100
   ```

2. **Check Processing Job Status**
   ```bash
   aws emr-serverless list-job-runs \
     --application-id $EMR_APP_ID \
     --states RUNNING
   ```

3. **Check Spark Streaming Checkpoint**
   - Location: `s3://$CHECKPOINT_BUCKET/kafka-stream/`
   - Verify checkpoint directory exists
   - Check last processed offset

#### Recovery

**Case 1: Job Crashed**
```bash
# Restart streaming job from last checkpoint
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://.../jobs/kafka_orders_to_bronze.py"
    }
  }'
```

**Case 2: Slow Processing**
- Scale up: Increase executor memory/cores
- Optimize: Check for data skew
- Parallelize: Increase partition count

### Data Loss Scenario

#### Check Last Good Offset
```bash
# Query Kafka broker
kafka-consumer-groups.sh \
  --bootstrap-server $BOOTSTRAP \
  --group $CONSUMER_GROUP \
  --describe
```

#### Replay from Offset
```bash
# Update checkpoint to earlier offset
aws s3 cp s3://$CHECKPOINT_BUCKET/kafka-stream/checkpoint \
  --metadata offset:$TARGET_OFFSET

# Restart job
aws emr-serverless start-job-run ...
```

### Checkpoint Corruption

#### Symptoms
- Job fails on startup
- Error: "Cannot load checkpoint"
- Checkpoint directory empty/corrupted

#### Recovery

1. **Backup Current State**
   ```bash
   aws s3 cp s3://$CHECKPOINT_BUCKET/kafka-stream/ \
     s3://$BACKUP_BUCKET/checkpoint-backup/$(date +%Y%m%d)/
   ```

2. **Find Last Good Checkpoint**
   - Query downstream Delta tables
   - Find latest `ingest_timestamp`
   - Calculate offset from timestamp

3. **Reset Checkpoint**
   ```bash
   # Remove corrupted checkpoint
   aws s3 rm s3://$CHECKPOINT_BUCKET/kafka-stream/checkpoint --recursive
   
   # Create fresh checkpoint from known good offset
   aws s3 cp checkpoint_init.json \
     s3://$CHECKPOINT_BUCKET/kafka-stream/checkpoint/
   ```

4. **Restart Job**
   ```bash
   aws emr-serverless start-job-run ...
   ```

## Kinesis Processing Issues

### Shard Processing Failure

#### Symptoms
- One shard not processing
- Job keeps restarting
- CloudWatch errors on specific shard

#### Recovery

1. **Identify Failed Shard**
   ```bash
   aws kinesis get-records \
     --shard-iterator $ITERATOR \
     --limit 10
   ```

2. **Check Shard State**
   - Active/Inactive
   - Recent events in shard
   - Consumer group status

3. **Replay Shard**
   - Reset checkpoint for that shard
   - Restart job
   - Monitor for success

## Prevention

1. **Checkpoint Regularly**: Every 5 minutes
2. **Monitor Lag**: CloudWatch alarm on lag threshold
3. **Scale Proactively**: Before lag grows too large
4. **Test Recovery**: Regular disaster recovery drills
5. **Documentation**: Update this runbook with learnings

## Post-Recovery Verification

```bash
# Verify new data flowing
aws athena start-query-execution \
  --query-string "SELECT MAX(ingest_timestamp) FROM bronze.kafka_orders"

# Check processing rate
aws cloudwatch get-metric-statistics \
  --namespace KafkaConsumer \
  --metric-name RecordsPerSecond

# Verify no data loss
python tests/verify_streaming_completeness.py
```

