# Lakehouse Storage Formats Guide

This document explains when and how to use different storage formats (Parquet, Delta Lake, Apache Iceberg) in our data platform.

## Overview

Our platform supports three storage formats, configurable via `config/*.yaml`:

- **Parquet**: Plain Parquet files (Bronze layer, simple use cases)
- **Delta Lake**: ACID transactions, time travel, schema evolution (Databricks-friendly)
- **Apache Iceberg**: Open table format with Glue catalog integration (AWS-native, Athena/Redshift compatible)

## Configuration

### Format Selection

Set the format in your config file:

```yaml
# config/local.yaml or config/prod.yaml
storage:
  warehouse: "s3://my-etl-lake-demo"  # Base warehouse path
  format: "iceberg"  # options: parquet | delta | iceberg
  catalog: "glue_catalog"  # Iceberg catalog name (only for format=iceberg)
```

### Bronze Layer

**Always uses Parquet** - Bronze layer stores raw, unprocessed data and doesn't need ACID transactions.

```python
# Bronze writes are always Parquet
df.write.parquet("s3://bucket/bronze/source/table")
```

### Silver & Gold Layers

Use the configured format:

- **Iceberg** (recommended for AWS): Tables registered in Glue, queryable from Athena and Redshift Serverless
- **Delta**: Best for Databricks environments or local development
- **Parquet**: Simple use cases without ACID needs

## When to Use Each Format

### Parquet

**Use for:**
- Bronze layer (raw data landing)
- Simple batch processing
- Data that doesn't need ACID guarantees
- Archive/backup storage

**Example:**
```python
# Simple Parquet write
df.write.mode("overwrite").parquet("s3://bucket/silver/table")
```

### Delta Lake

**Use for:**
- Databricks environments
- Local development and testing
- When you need time travel and schema evolution
- Stream processing with Delta streaming

**Example:**
```yaml
# config/local.yaml
storage:
  format: "delta"
```

```python
# Delta write (automatic via write_table)
from pyspark_interview_project.io.write_table import write_table
write_table(df, "silver.customer_behavior", cfg=config)
```

### Apache Iceberg

**Use for:**
- AWS production environments
- Querying from Athena
- Redshift Serverless integration
- Multi-engine access (Spark, Trino, Flink)
- Schema evolution and time travel
- Hidden partitioning

**Example:**
```yaml
# config/prod.yaml
storage:
  format: "iceberg"
  catalog: "glue_catalog"
  warehouse: "s3://company-data-lake-123456789012"
```

```python
# Iceberg write (automatic via write_table)
from pyspark_interview_project.io.write_table import write_table
write_table(df, "silver.customer_behavior", cfg=config)
# Creates: glue_catalog.silver.customer_behavior
```

## Querying Tables

### Iceberg Tables (Athena & Redshift)

#### Athena

```sql
-- Query Iceberg table
SELECT * FROM glue_catalog.silver.customer_behavior
ORDER BY event_ts DESC
LIMIT 100;

-- Time travel query
SELECT * FROM glue_catalog.silver.customer_behavior
FOR VERSION AS OF 3;

-- Time travel by timestamp
SELECT * FROM glue_catalog.silver.customer_behavior
FOR TIMESTAMP AS OF TIMESTAMP '2025-01-15 10:00:00';

-- Show table history
SELECT * FROM glue_catalog.silver.customer_behavior.history;
```

#### Redshift Serverless

```sql
-- Create external schema pointing to Glue catalog
CREATE EXTERNAL SCHEMA silver
FROM DATA CATALOG
DATABASE 'glue_catalog'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftSpectrumRole';

-- Query Iceberg table
SELECT * FROM silver.customer_behavior
WHERE event_ts > '2025-01-01'
ORDER BY event_ts DESC;
```

### Delta Tables

```python
# Spark SQL
spark.sql("SELECT * FROM delta.`s3://bucket/silver/customer_behavior` WHERE version < 5")

# Time travel
spark.read.format("delta").option("versionAsOf", 3).load("s3://bucket/silver/table")

# Show history
spark.sql("DESCRIBE HISTORY delta.`s3://bucket/silver/table`")
```

### Parquet Tables

```python
# Direct read
df = spark.read.parquet("s3://bucket/silver/table")

# Athena (via Glue table registration)
SELECT * FROM silver_customer_behavior
WHERE event_date = '2025-01-01';
```

## Implementation Details

### Spark Session Configuration

The `build_spark()` function in `src/pyspark_interview_project/utils/spark_session.py` automatically configures Spark based on `storage.format`:

- **Iceberg**: Registers Glue catalog, enables Iceberg extensions
- **Delta**: Enables Delta extensions
- **Parquet**: Plain Spark, no extensions

### Write Abstraction

All writes use `write_table()` from `src/pyspark_interview_project/io/write_table.py`:

```python
from pyspark_interview_project.io.write_table import write_table

# Automatically uses configured format
write_table(
    df=df_silver,
    table_name="silver.customer_behavior",  # For Iceberg: creates glue_catalog.silver.customer_behavior
    mode="overwrite",
    cfg=config,
    spark=spark
)
```

### Table Naming

- **Iceberg**: `layer.table_name` → `glue_catalog.layer.table_name`
  - Example: `"silver.customer_behavior"` → `glue_catalog.silver.customer_behavior`
- **Delta/Parquet**: `layer.table_name` → `s3://warehouse/layer/table_name`
  - Example: `"silver.customer_behavior"` → `s3://bucket/silver/customer_behavior`

## Migration Between Formats

### Delta → Iceberg

1. Update config: `storage.format: "iceberg"`
2. Rewrite tables using `write_table()`
3. Tables will be registered in Glue automatically

### Parquet → Iceberg

1. Update config: `storage.format: "iceberg"`
2. Read existing Parquet tables
3. Write using `write_table()` to convert to Iceberg

## EMR Configuration

For EMR clusters, Iceberg requires:

- **EMR 6.15+**: Has Iceberg bundled
- **Spark defaults**: Configured in `aws/emr_configs/spark-defaults.conf`
- **IAM permissions**: EMR role needs Glue catalog read/write permissions

## Benefits by Format

### Iceberg Benefits

✅ **Open standard** - Works with multiple engines (Spark, Trino, Flink)  
✅ **Glue integration** - Automatic catalog registration  
✅ **Athena/Redshift native** - Query directly without data movement  
✅ **Hidden partitioning** - Automatic partition evolution  
✅ **Time travel** - Query historical versions  
✅ **Schema evolution** - Safe schema changes  
✅ **ACID transactions** - Consistent reads/writes  

### Delta Benefits

✅ **Databricks optimized** - Best performance on Databricks  
✅ **Time travel** - Query historical versions  
✅ **Schema evolution** - Merge schema on write  
✅ **Streaming** - Delta streaming support  
✅ **Change data feed** - Track row-level changes  

### Parquet Benefits

✅ **Simple** - No dependencies, universal support  
✅ **Fast reads** - Columnar format, efficient compression  
✅ **Portable** - Works everywhere  
✅ **Small overhead** - Minimal metadata  

## Query Performance

### Iceberg

- **Athena**: Optimized partition pruning, metadata caching
- **Redshift**: Direct querying, no data movement
- **Spark**: Partition evolution, predicate pushdown

### Delta

- **Databricks**: Optimized file layout, Z-ordering
- **Spark**: File skipping, statistics

### Parquet

- **All engines**: Standard columnar format, good compression

## Best Practices

1. **Bronze**: Always Parquet (raw data doesn't need ACID)
2. **Silver**: Iceberg (production) or Delta (development)
3. **Gold**: Iceberg (for analytics tools) or Delta (for Spark-only)
4. **Test locally**: Use Delta or Parquet
5. **Production AWS**: Use Iceberg for Glue/Athena integration

## Troubleshooting

### Iceberg Tables Not Visible in Athena

1. Check Glue catalog permissions
2. Verify table registration: `SHOW TABLES IN glue_catalog.silver`
3. Check IAM role has `glue:GetTable`, `glue:GetDatabase` permissions

### Format Switch Not Working

1. Ensure config is loaded: `config = load_conf("config/prod.yaml")`
2. Check Spark session uses config: `spark = build_spark(config=config)`
3. Verify `storage.format` is set correctly

### Time Travel Not Available

- **Iceberg**: Use `FOR VERSION AS OF` or `FOR TIMESTAMP AS OF`
- **Delta**: Use `.option("versionAsOf", n)` when reading
- **Parquet**: Not supported (no versioning)

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [AWS Glue Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- [Athena Iceberg Support](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
- [Delta Lake Documentation](https://delta.io/)


