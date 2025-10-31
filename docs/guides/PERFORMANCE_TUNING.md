# Performance Tuning Guide

This guide provides comprehensive strategies for optimizing Spark/Delta Lake performance and reducing costs.

## Table of Contents

1. [Partitioning Strategy](#partitioning-strategy)
2. [Z-Ordering Optimization](#z-ordering-optimization)
3. [Broadcast Joins](#broadcast-joins)
4. [Spark Configuration Tuning](#spark-configuration-tuning)
5. [Cost Optimization](#cost-optimization)
6. [Benchmarks](#benchmarks)

---

## Partitioning Strategy

### When to Partition

**Partition by Date** (Recommended for facts):
- ✅ Time-series data (orders, events, transactions)
- ✅ Frequent time-based queries
- ✅ Large tables (> 10GB)

**Example**:
```python
# Bronze/Silver: Partition by ingestion date
df.write.format("delta").partitionBy("dt", "source") \
    .save("s3://bucket/bronze/crm/accounts")

# Gold facts: Partition by order date
df.write.format("delta").partitionBy("order_year", "order_month") \
    .save("s3://bucket/gold/fact_sales")
```

**Partition by Category** (Selective partitions only):
- ✅ When category has high selectivity (few distinct values)
- ✅ Category is frequently used in WHERE clauses
- ⚠️ Avoid: Too many partitions (> 10K partitions can hurt performance)

**Avoid Partitioning By**:
- ❌ High cardinality columns (customer_id, order_id)
- ❌ Frequently changing values
- ❌ More than 2-3 partition columns (unless necessary)

### Partition Pruning

Ensure partition columns are in WHERE clauses:

```python
# ✅ Good: Partition pruning works
spark.read.format("delta").load("gold/fact_sales") \
    .filter(col("order_year") == 2024) \
    .filter(col("order_month") == 1)

# ❌ Bad: Full table scan
spark.read.format("delta").load("gold/fact_sales") \
    .filter(col("customer_id") == "12345")  # No partition filter
```

---

## Z-Ordering Optimization

### What is Z-Ordering?

Z-ordering co-locates related data in the same files, improving query performance for multi-column filters.

### When to Z-Order

**Recommended for Gold Layer**:
- ✅ Frequently filtered columns together
- ✅ Join keys (if joined on multiple columns)
- ✅ Columns with good distribution

**Example**:
```python
from delta.tables import DeltaTable

# Z-order by columns frequently queried together
delta_table = DeltaTable.forPath(spark, "gold/fact_sales")
delta_table.optimize().executeZOrderBy(["order_year", "order_month", "customer_segment"])
```

### Z-Ordering Best Practices

1. **Order matters**: Most selective columns first
2. **Limit columns**: 3-5 columns max (diminishing returns)
3. **Run OPTIMIZE first**: Before Z-ordering

```python
# OPTIMIZE first, then Z-order
delta_table.optimize() \
    .executeZOrderBy(["order_date", "product_category"])
```

---

## Broadcast Joins

### When to Broadcast

Broadcast small lookup tables (< 10MB after compression) to all nodes:

```python
from pyspark.sql.functions import broadcast

# Broadcast small dimension table
large_df.join(
    broadcast(small_dim_df),
    "dim_id"
)
```

### Broadcast Threshold

Configure in `spark-defaults.conf`:
```properties
spark.sql.autoBroadcastJoinThreshold=10485760  # 10MB
```

### Memory Considerations

- Driver memory must accommodate broadcast table
- Increase driver memory if needed: `spark.driver.memory=4g`

**Example from Codebase**:
```python
# In build_sales_fact_table.py
products_dim = spark.read.format("delta").load(silver_products_path)
sales_fact = sales_raw.join(
    broadcast(products_dim),
    sales_raw.product_id == products_dim.product_id,
    "left"
)
```

---

## Spark Configuration Tuning

### Memory Allocation

**Driver Memory**:
```properties
spark.driver.memory=4g
spark.driver.maxResultSize=2g
```

**Executor Memory**:
```properties
spark.executor.memory=8g
spark.executor.memoryFraction=0.8  # 80% for data, 20% for overhead
spark.memory.fraction=0.8
```

### Shuffle Partitions

**Rule of thumb**: 2-3x number of CPU cores

```properties
# For 200 cores
spark.sql.shuffle.partitions=400
```

**Dynamic adjustment**:
```python
# Adjust per job based on data size
spark.conf.set("spark.sql.shuffle.partitions", 
               max(200, df.rdd.getNumPartitions() * 2))
```

### Dynamic Allocation

Enable for EMR Serverless (handled automatically) or clusters:

```properties
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=20
spark.dynamicAllocation.initialExecutors=4
```

### Delta Lake Optimizations

```properties
# Enable Delta optimizations
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# Delta-specific
spark.databricks.delta.autoOptimize.optimizeWrite=true
spark.databricks.delta.autoOptimize.autoCompact=true
```

---

## Cost Optimization

### Storage Optimization

1. **Compression**:
```python
# Use ZSTD compression (best compression ratio)
df.write.format("delta") \
    .option("delta.compressionCodec", "zstd") \
    .save("path")
```

2. **S3 Lifecycle Policies**:
   - Move old partitions to Glacier after 90 days
   - Delete data older than retention policy

3. **Delta OPTIMIZE/VACUUM**:
```python
# Run weekly to compact small files
delta_table.optimize().executeCompaction()

# Vacuum old files (retain 168 hours = 7 days)
delta_table.vacuum(retentionHours=168)
```

### Processing Optimization

1. **Incremental Processing**:
   - Process only new/changed data using watermarks
   - Avoid full table scans

2. **Partition Pruning**:
   - Always filter by partition columns first
   - Use date ranges in WHERE clauses

3. **Projection Pushdown**:
   - Select only needed columns early
   ```python
   df.select("order_id", "total_amount", "order_date")
   ```

### Cost Metrics

Track and optimize:
- **Cost per TB processed**: Target < $5/TB
- **Storage cost**: Monitor S3 storage growth
- **EMR costs**: Right-size applications

**Example Cost Analysis**:
```
Monthly EMR costs: $500
Data processed: 100 TB
Cost per TB: $5/TB
```

---

## Benchmarks

### Performance Targets

| Metric | Target | Current |
|--------|--------|---------|
| Bronze ingestion | < 5 min/TB | TBD |
| Silver transformation | < 10 min/TB | TBD |
| Gold aggregation | < 15 min/TB | TBD |
| Query latency (Gold) | < 10s | TBD |

### Benchmarking Script

```python
# scripts/performance/benchmark_job.py
import time
from pyspark.sql import SparkSession

def benchmark_job(spark, job_func, *args, **kwargs):
    start = time.time()
    result = job_func(*args, **kwargs)
    duration = time.time() - start
    
    records = result.count() if hasattr(result, 'count') else 0
    throughput = records / duration if duration > 0 else 0
    
    return {
        "duration_seconds": duration,
        "records_processed": records,
        "throughput_rps": throughput
    }
```

---

## Best Practices Summary

1. ✅ **Partition by date** for time-series data
2. ✅ **Z-order** frequently queried columns together
3. ✅ **Broadcast** small lookup tables
4. ✅ **Tune shuffle partitions** (2-3x cores)
5. ✅ **Use OPTIMIZE/VACUUM** weekly
6. ✅ **Enable Delta auto-optimize** for writes
7. ✅ **Filter early** (partition columns first)
8. ✅ **Select only needed columns**
9. ✅ **Monitor costs** per TB processed
10. ✅ **Right-size** EMR resources

---

**Last Updated**: 2024-01-15  
**Maintained By**: Data Engineering Team

