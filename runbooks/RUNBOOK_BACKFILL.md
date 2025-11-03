# Runbook: Backfill and Historical Reprocessing

## Purpose

Guide for backfilling historical data or reprocessing specific date ranges.

---

## Use Cases

1. **Historical Load:** Initial load of historical data
2. **Reprocessing:** Fix data quality issues for specific dates
3. **Schema Changes:** Reprocess after schema evolution
4. **Business Correction:** Adjust business logic retrospectively

---

## Pre-Backfill Checklist

- [ ] Identify date range
- [ ] Estimate data volume and processing time
- [ ] Check available EMR capacity
- [ ] Notify stakeholders (if affecting production)
- [ ] Create backfill ticket
- [ ] Document reason for backfill

---

## Procedure

### Step 1: Verify Data Availability

```bash
# Check source data availability
aws s3 ls s3://source-bucket/data/ --recursive | grep "2025-10"

# Verify watermark state
python scripts/maintenance/check_watermark.py --source snowflake_orders
```

### Step 2: Estimate Resources

```bash
# Check data volume for date range
python scripts/maintenance/estimate_backfill.py \
  --source snowflake_orders \
  --start 2025-10-01 \
  --end 2025-10-31

# Expected output:
# Total records: 1,234,567
# Estimated processing time: 2 hours
# Estimated cost: $15.50
```

### Step 3: Run Backfill

```bash
# Dry run first (no changes)
python scripts/maintenance/backfill_range.py \
  --table orders \
  --start 2025-10-01 \
  --end 2025-10-31 \
  --dry-run \
  --config config/prod.yaml

# Actual backfill (requires --confirm)
python scripts/maintenance/backfill_range.py \
  --table orders \
  --start 2025-10-01 \
  --end 2025-10-31 \
  --confirm \
  --config config/prod.yaml
```

### Step 4: Monitor Progress

```bash
# Watch job logs
aws logs tail /aws/emr-serverless/application-logs --follow

# Check metrics
aws cloudwatch get-metric-statistics \
  --namespace ETLPipelineMetrics \
  --metric-name records_processed_total \
  --dimensions Name=job,Value=backfill_orders
```

### Step 5: Verify Results

```bash
# Check row counts
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM bronze.orders WHERE _run_date BETWEEN '2025-10-01' AND '2025-10-31'"

# Validate data quality
python scripts/maintenance/validate_backfill.py \
  --table orders \
  --start 2025-10-01 \
  --end 2025-10-31
```

---

## Point-in-Time Reprocessing

**Important:** Backfill writes to `as_of_date` partitions, not overwriting all data.

### How It Works

```python
# Backfill writes to:
s3://bucket/bronze/orders/dt=2025-10-01/  # Original partition
s3://bucket/bronze/orders/as_of_date=2025-10-15/  # Reprocessed partition

# Latest view selects most recent as_of_date
```

### Example

```bash
# Reprocess October 2025 with fixed logic
python scripts/maintenance/backfill_range.py \
  --table orders \
  --start 2025-10-01 \
  --end 2025-10-31 \
  --as-of-date 2025-10-15 \
  --confirm
```

---

## Troubleshooting

### Backfill Taking Too Long

```bash
# 1. Increase parallelism
# spark.sql.shuffle.partitions: 400

# 2. Process in smaller batches
# Split into weekly chunks

# 3. Use EMR Spot instances
# Reduce cost and increase parallelism
```

### Out of Memory Errors

```bash
# 1. Increase executor memory
# spark.executor.memory: 16g

# 2. Process smaller date ranges
# --start 2025-10-01 --end 2025-10-07

# 3. Add memory overhead
# spark.executor.memoryOverhead: 4g
```

### Data Quality Issues After Backfill

```bash
# 1. Check source data
# Verify source hasn't changed

# 2. Compare with original
python scripts/maintenance/compare_backfill.py \
  --table orders \
  --date 2025-10-15 \
  --original-partition dt=2025-10-15 \
  --reprocessed-partition as_of_date=2025-10-15

# 3. Rollback if needed
aws s3 rm s3://bucket/bronze/orders/as_of_date=2025-10-15/ --recursive
```

---

## Best Practices

1. ✅ **Always dry-run first**
2. ✅ **Start small** (test with 1-2 days)
3. ✅ **Monitor closely** (watch metrics and logs)
4. ✅ **Validate results** (compare counts, sample data)
5. ✅ **Document everything** (reason, method, results)
6. ✅ **Use as_of_date partitions** (don't overwrite historical data)
7. ✅ **Notify stakeholders** (if affecting production queries)

---

## Cost Considerations

**Typical Costs:**
- Bronze backfill: $0.50 - $2.00 per million rows
- Silver reprocess: $1.00 - $3.00 per million rows
- Gold rebuild: $0.50 - $1.50 per million rows

**Ways to Reduce Cost:**
- Use EMR Spot instances (60-90% savings)
- Process during off-peak hours
- Batch multiple tables together
- Use smaller date ranges

---

## Approval Matrix

| Date Range | Approval Required | Notification |
|------------|-------------------|--------------|
| < 7 days | Team Lead | Optional |
| 7-30 days | Data Eng Lead | Required |
| > 30 days | Manager + Data Eng Lead | Required |
| Full reprocess | Director Approval | Required + SLA |

---

## Contact

- **Backfill Support:** backfill-support@company.com
- **Data Engineering:** data-eng@company.com
- **On-Call:** Check PagerDuty

---

**Last Updated:** 2025-01-15  
**Owner:** Data Engineering Team

