# Apache Iceberg Integration Guide

## Overview

Project_A uses **Apache Iceberg** for curated data layers (Silver/Gold) where ACID guarantees and schema evolution are critical.

## Architecture Decision

### Where We Use Iceberg

**Silver Layer - orders_iceberg** ✅
- **Why**: High update frequency (refunds, order corrections)
- **Benefits**: ACID transactions, merge operations, time travel
- **Format**: Iceberg with Parquet storage

**Gold Layer - Optional** 
- Can be used for complex fact tables requiring frequent updates
- Currently uses Delta Lake (also provides ACID)

### Where We DON'T Use Iceberg

**Bronze Layer** ❌
- **Format**: Raw Parquet files
- **Why**: Immutable, append-only data doesn't need ACID overhead
- **Strategy**: Simple, fast ingestion

## Why Iceberg vs Delta Lake?

| Feature | Iceberg | Delta Lake | Our Choice |
|---------|---------|------------|------------|
| **ACID Transactions** | ✅ | ✅ | Both good |
| **Schema Evolution** | ✅ | ✅ | Both good |
| **Time Travel** | ✅ | ✅ | Both good |
| **Hidden Partitioning** | ✅ | ❌ | **Iceberg wins** |
| **Multi-Engine Support** | ✅ Excellent | 🟡 Limited | **Iceberg wins** |
| **Spark Integration** | ✅ Good | ✅ Excellent | Delta wins |
| **Operational Complexity** | 🟡 Higher | ✅ Lower | Delta wins |

**Decision**: 
- Use **Iceberg** for `orders_silver` (high-value use case needing ACID + multi-engine)
- Keep **Delta Lake** for other Silver tables (simpler operations)
- This demonstrates understanding of both tools

## Iceberg Implementation

### 1. Catalog Configuration

```python
from project_a.iceberg_utils import IcebergConfig, initialize_iceberg_spark

# Initialize Spark with Iceberg
spark = initialize_iceberg_spark(
    app_name="project_a",
    catalog_type=IcebergConfig.HADOOP_CATALOG,
    warehouse_path="data/iceberg-warehouse"
)
```

**Catalog Types Supported**:
- `HADOOP_CATALOG`: Local/HDFS filesystem (dev/testing)
- `HIVE_CATALOG`: Hive Metastore integration
- `GLUE_CATALOG`: AWS Glue for production

### 2. Writing Data

```python
from project_a.iceberg_utils import IcebergWriter

writer = IcebergWriter(spark, catalog_name="local")

# Initial creation
writer.create_table(
    df=orders_df,
    table_name="silver.orders_iceberg",
    partition_by=["order_date"],
    properties={
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy"
    }
)

# Incremental merge (handles late data)
writer.write_merge(
    df=new_orders_df,
    table_name="silver.orders_iceberg",
    merge_key="order_id",
    update_cols=["total_amount", "status", "_updated_at"]
)
```

### 3. Reading Data

```python
from project_a.iceberg_utils import IcebergReader

reader = IcebergReader(spark, catalog_name="local")

# Read current snapshot
orders = reader.read_current("silver.orders_iceberg")

# Time travel - read as of yesterday
orders_yesterday = reader.read_as_of_timestamp(
    "silver.orders_iceberg",
    "2024-01-15 00:00:00"
)

# Get snapshot history (audit trail)
snapshots = reader.get_snapshots("silver.orders_iceberg")
```

## Incremental Processing with Iceberg

### Rolling Window Strategy

**Problem**: Orders can be updated after initial load (refunds, corrections)

**Solution**: 3-day rolling window with MERGE

```python
from datetime import datetime, timedelta

# Calculate lookback window
lookback_days = 3
cutoff_date = datetime.now().date() - timedelta(days=lookback_days)

# Read recent orders
recent_orders = spark.read.parquet(source_path) \
    .filter(F.col("order_date") >= F.lit(cutoff_date))

# MERGE into Iceberg (upsert)
writer.write_merge(
    df=recent_orders,
    table_name="silver.orders_iceberg",
    merge_key="order_id"
)
```

**How It Works**:
1. Every run reprocesses last 3 days
2. MERGE updates existing records + inserts new ones
3. Idempotent - safe to re-run
4. Handles late-arriving data gracefully

### MERGE Behavior

```sql
MERGE INTO silver.orders_iceberg t
USING updates s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET t.* = s.*
WHEN NOT MATCHED THEN INSERT *
```

**Why This Matters**:
- Orders can be corrected (wrong amount, wrong product)
- Refunds can arrive days later
- Payment failures update order status
- MERGE handles all these scenarios atomically

## Schema Evolution

Iceberg supports non-breaking schema changes:

```python
# Add new column (safe)
spark.sql("""
ALTER TABLE local.silver.orders_iceberg
ADD COLUMN shipping_method STRING
""")

# Rename column (safe)
spark.sql("""
ALTER TABLE local.silver.orders_iceberg
RENAME COLUMN total_amount TO order_amount
""")

# Change column type (requires rewrite)
spark.sql("""
ALTER TABLE local.silver.orders_iceberg
ALTER COLUMN order_amount TYPE DECIMAL(12,2)
""")
```

**Evolution Rules**:
- Adding columns: ✅ Always safe
- Renaming columns: ✅ Safe with Iceberg metadata
- Changing types: ⚠️ Requires validation
- Removing columns: ❌ Breaking change (deprecate first)

## Time Travel & Auditing

### Use Cases

**1. Debugging**
```python
# What did this order look like yesterday?
order_yesterday = reader.read_as_of_timestamp(
    "silver.orders_iceberg",
    "2024-01-15 10:00:00"
).filter("order_id = 'ORD-12345'")
```

**2. Compliance**
```python
# Prove what data we had at end of quarter
q4_data = reader.read_as_of_timestamp(
    "silver.orders_iceberg",
    "2023-12-31 23:59:59"
)
```

**3. Rollback**
```python
# Rollback to previous snapshot if bad data loaded
snapshots = reader.get_snapshots("silver.orders_iceberg")
previous_snapshot_id = snapshots.orderBy("committed_at", ascending=False) \
    .limit(2).tail(1)[0].snapshot_id

rollback_data = reader.read_snapshot("silver.orders_iceberg", previous_snapshot_id)
```

## Partition Evolution

Iceberg supports **hidden partitioning** - users don't specify partitions in queries:

```python
# Iceberg handles partition pruning automatically
orders = spark.sql("""
SELECT * FROM local.silver.orders_iceberg
WHERE order_date = '2024-01-15'
""")
# Iceberg: Only reads relevant partition (efficient)
# Parquet: Would require explicit partition in path
```

**Benefits**:
- Queries are simpler
- Partition strategy can change without breaking queries
- Better for analysts/BI tools

## Performance Optimization

### Table Maintenance

```python
# Compact small files
spark.sql("CALL local.system.rewrite_data_files('silver.orders_iceberg')")

# Remove old snapshots (free up space)
spark.sql("""
CALL local.system.expire_snapshots(
    table => 'silver.orders_iceberg',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 10
)
""")

# Update statistics
spark.sql("ANALYZE TABLE local.silver.orders_iceberg COMPUTE STATISTICS")
```

## Job Integration

### Running the Iceberg Job

```bash
# Initial migration
python jobs/iceberg/orders_silver_to_iceberg.py \
    --env dev \
    --config config/dev.yaml

# Scheduled incremental updates (cron/Airflow)
# Runs hourly to merge new/updated orders
0 * * * * python jobs/iceberg/orders_silver_to_iceberg.py --env prod --config config/prod.yaml
```

### Job Flow

```
Parquet Source (orders_silver)
         ↓
[Check if Iceberg table exists]
         ↓
    Yes → Incremental Update (MERGE with 3-day window)
    No  → Initial Migration (CREATE TABLE)
         ↓
Iceberg Table (silver.orders_iceberg)
         ↓
[dbt reads from Iceberg for Gold layer]
```

## Monitoring

### Key Metrics

```python
# Track Iceberg table health
snapshots = reader.get_snapshots("silver.orders_iceberg")

metrics = {
    "total_snapshots": snapshots.count(),
    "table_size_mb": spark.sql("DESCRIBE DETAIL local.silver.orders_iceberg")
                          .select("sizeInBytes").first()[0] / 1024 / 1024,
    "num_partitions": spark.sql("SHOW PARTITIONS local.silver.orders_iceberg").count(),
    "last_modified": snapshots.orderBy("committed_at", ascending=False).first().committed_at
}
```

### Alerts

Monitor for:
- ⚠️ Snapshot count growing too large (> 100)
- ⚠️ Table size increasing unexpectedly
- ⚠️ MERGE duration exceeding SLA
- ❌ Schema evolution failures

## Production Considerations

### AWS Deployment

```python
# Use Glue Catalog for production
iceberg_config = IcebergConfig.get_spark_config(
    catalog_type=IcebergConfig.GLUE_CATALOG,
    warehouse_path="s3://my-bucket/iceberg-warehouse"
)
```

### Cost Optimization

- **Expire old snapshots**: Retain only last 30 days
- **Compact small files**: Run weekly
- **Partition wisely**: Use date partitions for time-series data
- **Monitor storage**: Iceberg metadata adds overhead

### Multi-Engine Access

Iceberg enables:
- **Spark**: ETL processing
- **Trino/Presto**: Ad-hoc analytics
- **Flink**: Stream processing
- **Dremio**: BI acceleration

All engines see consistent view of data (ACID)!

## Interview Talking Points

**"Why did you choose Iceberg?"**  
→ "We needed ACID guarantees for orders_silver due to high update frequency. Iceberg's merge capabilities handle late-arriving data elegantly with a 3-day rolling window strategy."

**"Iceberg vs Delta Lake?"**  
→ "Both are excellent. We use Iceberg for orders_silver where multi-engine access and hidden partitioning provide value. Delta Lake for other tables where Spark-only access is sufficient."

**"How do you handle schema evolution?"**  
→ "Iceberg supports additive schema changes natively. We add columns safely, test backward compatibility, and use time travel to validate changes."

**"Late data strategy?"**  
→ "3-day rolling window with MERGE INTO. Every run reprocesses recent data, updating existing records atomically. Idempotent and handles corrections gracefully."

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Table Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)

---

**Document Owner**: Data Engineering Team  
**Last Updated**: 2026-02-06  
**Next Review**: 2026-03-06
