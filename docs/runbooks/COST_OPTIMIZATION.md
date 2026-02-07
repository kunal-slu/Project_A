# Cost Optimization Runbook

This document outlines strategies for optimizing AWS costs for the data engineering platform.

## Cost Breakdown

### Monthly Cost Components

| Component | Estimated Cost | % of Total |
|-----------|---------------|------------|
| EMR Serverless | $500-2000 | 40-50% |
| S3 Storage | $200-500 | 15-25% |
| S3 Data Transfer | $50-200 | 5-15% |
| Glue/Athena | $100-300 | 10-15% |
| MWAA | $150-300 | 10-15% |
| CloudWatch | $50-100 | 5-10% |
| **Total** | **$1050-3400** | **100%** |

---

## Optimization Strategies

### 1. EMR Serverless Cost Optimization

#### Right-Size Applications

**Current Configuration**:
```yaml
emr:
  driver:
    memory: 4g
    cores: 2
  executor:
    memory: 8g
    cores: 4
```

**Optimization**:
- Monitor actual resource usage
- Reduce memory if jobs use < 50% consistently
- Use spot instances where possible (if using EMR clusters)

#### Incremental Processing

**Strategy**: Process only new/changed data

```python
# Use watermarks for incremental loads
from pyspark_interview_project.utils.watermark import WatermarkManager

watermark = WatermarkManager("crm_contacts", config)
latest = watermark.get_latest_watermark()

# Only process new records
df = spark.read.format("delta").load(bronze_path) \
    .filter(col("_ingestion_ts") > latest)
```

**Cost Impact**: 50-80% reduction in processing costs

---

### 2. S3 Storage Optimization

#### Compression

**Current**: Parquet (Snappy)  
**Optimized**: Delta with ZSTD compression

```python
df.write.format("delta") \
    .option("delta.compressionCodec", "zstd") \
    .save("path")
```

**Storage Reduction**: 30-50% smaller files

#### S3 Lifecycle Policies

**Policy**:
1. **Bronze layer**: Move to Glacier after 90 days
2. **Silver layer**: Move to Glacier after 180 days
3. **Gold layer**: Standard storage (frequently accessed)
4. **Archived data**: Delete after retention period (7 years for compliance)

**Terraform Configuration**:
```hcl
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-to-glacier"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}
```

#### Delta OPTIMIZE/VACUUM

**Schedule**: Weekly maintenance job

```python
# Compact small files
delta_table.optimize().executeCompaction()

# Remove old files (retain 168 hours)
delta_table.vacuum(retentionHours=168)
```

**Cost Impact**: Reduces storage and query costs

---

### 3. Data Transfer Costs

#### VPC Endpoints

**Use VPC endpoints** for S3, Glue, Secrets Manager:
- Reduces data transfer costs
- Improves security

**Configuration**: See `aws/terraform/networking.tf`

#### Cross-Region Replication

**Only replicate** critical Gold tables to DR region:
- Bronze/Silver: Single region (cheaper)
- Gold: Cross-region replication (for DR)

---

### 4. Glue/Athena Cost Optimization

#### Partition Pruning

Always filter by partition columns:
```sql
-- ✅ Efficient: Uses partition pruning
SELECT * FROM gold.fact_sales 
WHERE order_year = 2024 AND order_month = 1;

-- ❌ Expensive: Scans all partitions
SELECT * FROM gold.fact_sales 
WHERE customer_id = '12345';
```

#### Column Projection

Select only needed columns:
```sql
-- ✅ Efficient
SELECT order_id, total_amount FROM gold.fact_sales;

-- ❌ Expensive: Reads all columns
SELECT * FROM gold.fact_sales;
```

#### Table Format

**Use Delta format** (already implemented):
- Better query performance
- Lower data scan costs

---

### 5. Monitoring & Alerting

#### Cost Alerts

**CloudWatch Billing Alarms**:
```yaml
# Alert if monthly cost exceeds $2000
billing_alarm:
  threshold: 2000
  metric: EstimatedCharges
  period: 86400  # Daily
```

#### Cost Tracking

**Track metrics**:
- Cost per TB processed
- Cost per job
- Storage growth rate

**Dashboard**: See `monitoring/cloudwatch/dashboards/cost_metrics.json`

---

## Optimization Checklist

### Immediate (High Impact, Low Effort)
- [ ] Enable Delta ZSTD compression
- [ ] Implement S3 lifecycle policies
- [ ] Schedule OPTIMIZE/VACUUM weekly
- [ ] Add partition filters to all queries

### Short-term (High Impact, Medium Effort)
- [ ] Implement incremental processing for all jobs
- [ ] Right-size EMR applications based on usage
- [ ] Set up cost alerts in CloudWatch
- [ ] Monitor and optimize query patterns

### Long-term (Medium Impact, High Effort)
- [ ] Migrate old data to Glacier
- [ ] Implement data retention policies
- [ ] Optimize join strategies
- [ ] Cache frequently accessed data

---

## Cost Targets

| Metric | Target | Current |
|--------|--------|---------|
| Cost per TB processed | < $5/TB | TBD |
| Monthly storage cost | < $500 | TBD |
| EMR cost per job | < $10 | TBD |
| Query cost (Athena) | < $0.01/query | TBD |

---

## Monthly Cost Review

### Review Process

1. **Extract cost data** from Cost Explorer
2. **Break down by service** (EMR, S3, Glue, etc.)
3. **Compare to previous month**
4. **Identify anomalies** (unexpected spikes)
5. **Optimize** top cost drivers
6. **Document** optimizations

### Cost Optimization Log

Keep track of optimizations:

| Date | Optimization | Cost Reduction | Impact |
|------|-------------|----------------|--------|
| 2024-01-15 | Enabled ZSTD compression | -30% storage | High |
| 2024-01-20 | Implemented incremental loads | -60% EMR costs | High |

---

**Last Updated**: 2024-01-15  
**Maintained By**: Data Engineering & Finance Teams

