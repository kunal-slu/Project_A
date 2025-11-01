# Cost Optimization Guide

## Overview

This document outlines cost optimization strategies for the data platform, covering compute, storage, and network costs.

## Cost Breakdown by Component

### 1. EMR Serverless (Compute)

**Current**: ~$0.10/hour per DPU (default 4 DPUs = $0.40/hour)

**Optimizations**:

1. **Use Spot Instances** (non-prod):
   ```bash
   aws emr-serverless start-job-run \
     --application-id app-xxxxx \
     --execution-role-arn arn:aws:iam::... \
     --job-driver '{"sparkSubmit": {...}}' \
     --configuration-overrides '{
       "applicationConfiguration": [{
         "classification": "spark-defaults",
         "properties": {
           "spark.executor.instanceTypes": "spot"
         }
       }]
     }'
   ```
   **Savings**: 70% reduction (~$0.12/hour)

2. **Right-size DPUs**:
   - Small jobs: 2 DPUs
   - Medium: 4 DPUs (default)
   - Large: 8+ DPUs
   - Use auto-scaling: `maxCapacity: 10, minCapacity: 2`

3. **Idle Timeout**:
   ```python
   # In EMR Serverless config
   "autoStartConfiguration": {
     "enabled": true,
     "idleTimeoutMinutes": 5  # Stop after 5 min idle
   }
   ```
   **Savings**: Prevents unnecessary idle time

### 2. S3 Storage (Data Lake)

**Current**: ~$0.023/GB/month (Standard)

**Optimizations**:

1. **Lifecycle Policies**:
   ```json
   {
     "Rules": [
       {
         "Id": "BronzeToIA",
         "Status": "Enabled",
         "Prefix": "bronze/",
         "Transitions": [
           {
             "Days": 30,
             "StorageClass": "STANDARD_IA"  // $0.0125/GB
           },
           {
             "Days": 90,
             "StorageClass": "GLACIER"  // $0.004/GB
           }
         ]
       },
       {
         "Id": "SilverToIA",
         "Status": "Enabled",
         "Prefix": "silver/",
         "Transitions": [
           {
             "Days": 60,
             "StorageClass": "STANDARD_IA"
           }
         ]
       }
     ]
   }
   ```
   **Savings**: 45-80% on storage after 30 days

2. **Delete Old Partitions**:
   ```sql
   -- Delta VACUUM removes files older than retention period
   VACUUM delta.`s3://my-etl-lake-demo/bronze/behavior` 
   RETAIN 30 DAYS;
   ```
   **Savings**: Reduces storage by 30%+ for append-only tables

3. **Compression**:
   - Use Parquet (Snappy): ~10:1 compression
   - Delta: Built-in Z-ordering reduces scan size
   - **Savings**: 90% storage reduction vs CSV

### 3. Delta Lake OPTIMIZE

**Schedule**: Daily for high-traffic tables, weekly for others

```python
# In maintenance_dag.py
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://my-etl-lake-demo/silver/orders")
delta_table.optimize().executeCompaction()

# Z-order for query performance
delta_table.optimize().executeZOrderBy(["customer_id", "order_date"])
```

**Benefits**:
- Reduces small files (fewer S3 requests)
- Faster queries (data co-location)
- **Cost Impact**: 30-50% reduction in query costs

### 4. Redshift Serverless

**Current**: ~$0.375/hour per RPU (compute + storage)

**Optimizations**:

1. **Auto-pause**:
   ```sql
   ALTER NAMESPACE analytics SET AUTO_RESUME = TRUE;
   ALTER NAMESPACE analytics SET AUTO_PAUSE = 300;  -- 5 min
   ```
   **Savings**: Pay only when querying

2. **Workload Management**:
   - Use query queues to limit concurrent queries
   - Prevent runaway queries from consuming all RPUs

### 5. Data Transfer Costs

**Current**: $0.09/GB (S3 → Redshift), $0.01/GB (cross-region)

**Optimizations**:

1. **Co-locate Resources**:
   - EMR, S3, Redshift in same region (us-east-1)
   - **Savings**: No data transfer costs

2. **Use External Tables** (Snowflake):
   ```sql
   CREATE EXTERNAL TABLE analytics.customer_360
   WITH LOCATION = 's3://my-etl-lake-demo/gold/customer_360'
   FILE_FORMAT = (TYPE = PARQUET);
   ```
   - Query S3 directly, no COPY costs
   - **Trade-off**: Slower queries vs materialized tables

3. **Batch Transfers**:
   - COPY full partitions, not incremental files
   - Use manifest files for multi-file loads
   - **Savings**: Fewer API calls = lower costs

## Cost Monitoring

### CloudWatch Metrics

```python
# Emit cost estimate in metrics
from pyspark_interview_project.monitoring.metrics_collector import emit_metrics

# Calculate estimated cost
dpus_used = 4
hours_runtime = duration_seconds / 3600
cost_estimate = dpus_used * hours_runtime * 0.10  # $0.10/DPU-hour

emit_metrics(
    job_name="build_customer_360",
    rows_in=rows_in,
    rows_out=rows_out,
    duration_seconds=duration_seconds,
    dq_status="pass",
    metadata={"cost_estimate_usd": cost_estimate}
)
```

### Cost Dashboard

**Metrics to Track**:
- EMR Serverless: `DPUHours` per job
- S3: Storage by class (Standard, IA, Glacier)
- Redshift: RPU hours
- Data Transfer: GB transferred

**Alerts**:
- Job cost > $10: Alert
- Monthly spend > budget: Alert
- Unusual spike: Alert

## Example Monthly Cost Estimate

**Assumptions**:
- 100 Spark jobs/day, avg 10 min runtime, 4 DPUs
- 10 TB data in S3 (5 TB Bronze, 3 TB Silver, 2 TB Gold)
- Redshift: 2 RPUs, 50% utilization

**Costs**:

| Component | Calculation | Monthly Cost |
|-----------|------------|--------------|
| EMR Serverless | 100 jobs × 0.17h × 4 DPUs × $0.10 | $680 |
| S3 Storage (Standard) | 5 TB × $23/TB | $115 |
| S3 Storage (IA) | 3 TB × $12.50/TB | $38 |
| S3 Storage (Glacier) | 2 TB × $4/TB | $8 |
| Redshift | 2 RPUs × 720h × 0.5 × $0.375 | $270 |
| **Total** | | **~$1,111/month** |

**With Optimizations**:
- Spot instances (70% off): EMR = $204
- Lifecycle policies: S3 = $80
- **Optimized Total**: **~$554/month (50% reduction)**

## Best Practices

1. **Right-size Resources**: Monitor actual usage, downsize if possible
2. **Use Spot**: Non-prod jobs should always use spot
3. **Lifecycle Policies**: Automate S3 transitions
4. **Delta OPTIMIZE**: Schedule regularly for performance + cost
5. **Monitor Costs**: CloudWatch dashboards + budgets
6. **Review Monthly**: Identify unused resources

## Cost Control Scripts

See `aws/scripts/cost_analysis.py`:
- Analyze S3 storage by prefix
- Estimate EMR costs per job
- Generate monthly cost reports

---

**Next Steps**: Set up CloudWatch dashboards and budgets in AWS Billing.

