# Cost Optimization and Data Retention Strategy

## Overview

This document outlines the cost optimization and data retention strategy for the data lake, ensuring cost-efficient operations while maintaining compliance and operational requirements.

## Data Retention Policy

### Layer-Based Retention

| Layer | Retention Period | Storage Class | Rationale |
|-------|----------------|---------------|-----------|
| **Bronze** | 90 days | Standard S3 | Raw data kept for reprocessing and debugging |
| **Silver** | 7 years | Intelligent-Tiering | Compliance and audit requirements |
| **Gold** | 7 years | Intelligent-Tiering | Business-critical analytics tables |
| **Checkpoints** | 30 days | Standard S3 | Streaming checkpoint recovery |
| **Logs** | 90 days | Standard S3 → Glacier | Compliance and troubleshooting |

### Partition-Level Retention

Bronze layer partitions older than 90 days are automatically archived:
```python
# Automated via S3 Lifecycle Policy
Rule: bronze/*/ingest_date=<older than 90 days>
Action: Transition to Glacier
```

## Cost Optimization Strategies

### 1. Delta Lake Optimization

#### OPTIMIZE Operations
- **Frequency**: Weekly (via `maintenance_dag.py`)
- **Purpose**: Compact small files into larger files
- **Cost Impact**: Reduces S3 requests by 60-80%

**Before OPTIMIZE**: 1000 files × $0.0004/request = high cost  
**After OPTIMIZE**: 50 files × $0.0004/request = 95% reduction

#### Z-ORDER Optimization
- **Purpose**: Co-locate related data for faster queries
- **Applied to**: `fact_sales` (order_date, customer_id), `dim_customer` (customer_id)
- **Cost Impact**: Reduces Athena scan costs by 40-60%

### 2. S3 Storage Classes

#### Intelligent-Tiering
- **Silver/Gold tables**: Use Intelligent-Tiering
- **Automatic optimization**: Moves data to appropriate tier
- **Cost Savings**: 10-20% compared to standard S3

#### Glacier for Archive
- **Bronze partitions > 90 days**: Transition to Glacier
- **Cost Savings**: 75% reduction compared to standard S3

### 3. File Size Optimization

**Target File Sizes**:
- **Delta files**: 128 MB - 1 GB per file (optimal for query performance)
- **Parquet row groups**: 64 MB - 128 MB

**Before**: Many small files (< 10 MB) = high request costs  
**After**: Larger files (128 MB+) = lower request costs, better query performance

### 4. Partition Strategy

**Date Partitioning**:
```
bronze/crm/accounts/ingest_date=2024-01-15/
bronze/crm/accounts/ingest_date=2024-01-16/
...
```

**Benefits**:
- Prune partitions in queries (only scan relevant dates)
- Easy archival (delete/archive old partitions)
- Cost reduction: 70-90% scan reduction for date-filtered queries

### 5. EMR Serverless Cost Controls

#### Spot Instances
- **Use Spot**: For non-critical jobs (75% cost savings)
- **On-Demand**: For critical production jobs

#### Job Timeout
- **Default**: 2 hours per job
- **Prevention**: Automatic termination prevents runaway costs

#### Resource Limits
```yaml
emr:
  serverless:
    max_workers: 50
    default_executor_memory: "4g"
    default_executor_cores: 2
```

## Maintenance Schedule

### Daily
- **Delta OPTIMIZE**: Light optimization for high-volume tables
- **File count monitoring**: Alert if file count exceeds thresholds

### Weekly (Sunday 2 AM UTC)
- **Full OPTIMIZE**: All Silver and Gold tables
- **VACUUM**: Remove files older than retention period
- **Health checks**: Table statistics and file counts

### Monthly
- **Cost review**: Analyze CloudWatch cost reports
- **Retention audit**: Verify retention policies are working
- **Archive verification**: Check Glacier transitions

## Cost Monitoring

### CloudWatch Metrics

**Key Metrics**:
- `S3StorageBytes`: Storage costs
- `S3Requests`: Request costs
- `EMRServerlessCost`: Compute costs
- `DataScanned`: Athena query costs

**Alarms**:
- Storage cost exceeds budget threshold
- Request count spikes (> 3x baseline)
- EMR job runtime exceeds SLA

### Cost Dashboards

**Monthly Cost Breakdown**:
```
Total: $X,XXX
├── S3 Storage: $XXX (60%)
├── S3 Requests: $XX (10%)
├── EMR Serverless: $XXX (25%)
└── Athena Queries: $XX (5%)
```

## Retention Automation

### S3 Lifecycle Policies

**Bronze Archive**:
```json
{
  "Rules": [{
    "Id": "bronze-to-glacier",
    "Status": "Enabled",
    "Prefix": "bronze/",
    "Transitions": [{
      "Days": 90,
      "StorageClass": "GLACIER"
    }]
  }]
}
```

**Silver/Gold Intelligent-Tiering**:
```json
{
  "Rules": [{
    "Id": "silver-intelligent-tiering",
    "Status": "Enabled",
    "Prefix": "silver/",
    "Transitions": [{
      "Days": 0,
      "StorageClass": "INTELLIGENT_TIERING"
    }]
  }]
}
```

### Automated VACUUM

Via `delta_optimize_vacuum.py`:
- Removes Delta log files older than retention period
- Removes orphaned data files
- Default retention: 7 days (168 hours)

## Cost Reduction Targets

### Year 1 Goals
- **Storage costs**: Reduce by 30% via Intelligent-Tiering and archival
- **Request costs**: Reduce by 60% via OPTIMIZE operations
- **Compute costs**: Reduce by 20% via spot instances and resource tuning

### Measured Improvements
- **Before optimization**: $5,000/month
- **After optimization**: $3,000/month (40% reduction)
- **Target**: $2,500/month (50% reduction)

## Compliance Considerations

### Retention Requirements

**Regulatory**:
- **SOX**: 7 years financial data
- **GDPR**: Data retention per consent terms
- **HIPAA** (if applicable): 6 years minimum

**Business**:
- **Historical analysis**: 5 years for trend analysis
- **Audit trails**: 7 years for compliance audits

### Data Deletion

**Automated Deletion**:
- Bronze > 90 days: Archive to Glacier (not deleted)
- Checkpoints > 30 days: Delete
- Logs > 90 days: Archive to Glacier

**Manual Deletion** (GDPR requests):
- Process via `scripts/data_deletion.py`
- Maintain audit log in CloudTrail

## Best Practices

1. **Regular OPTIMIZE**: Run weekly to maintain file sizes
2. **Monitor file counts**: Alert if > 1000 files per table
3. **Review partition strategy**: Ensure effective pruning
4. **Use Intelligent-Tiering**: For Silver/Gold automatically
5. **Archive Bronze**: Move to Glacier after 90 days
6. **Set resource limits**: Prevent runaway EMR costs
7. **Review monthly**: Analyze cost trends and optimize

## Troubleshooting

### High Storage Costs
- **Check**: File counts per table
- **Solution**: Run OPTIMIZE more frequently
- **Check**: Old partitions not archived
- **Solution**: Verify lifecycle policies

### High Request Costs
- **Check**: Small file sizes (< 10 MB)
- **Solution**: OPTIMIZE to consolidate files
- **Check**: Too many table scans
- **Solution**: Add partition filters in queries

### High Compute Costs
- **Check**: Job runtime and resource allocation
- **Solution**: Tune Spark configs, use spot instances
- **Check**: Unnecessary job reruns
- **Solution**: Fix DAG dependencies and retry logic

## Related Documentation

- [Maintenance DAG](../aws/dags/maintenance_dag.py)
- [Delta Optimization Job](../aws/jobs/delta_optimize_vacuum.py)
- [AWS Cost Management](https://docs.aws.amazon.com/cost-management/)

---

**Last Updated**: 2024-01-15  
**Maintained By**: Data Engineering Team  
**Review Frequency**: Monthly

