# Apache Iceberg Quick Start

## Quick Configuration

### Enable Iceberg (Production)

Edit `config/prod.yaml`:
```yaml
storage:
  format: "iceberg"
  catalog: "glue_catalog"
  warehouse: "s3://company-data-lake-123456789012"
```

### Use Delta (Local/Development)

Edit `config/local.yaml`:
```yaml
storage:
  format: "delta"
  warehouse: "data/lakehouse_delta"
```

## Using write_table()

All Silver and Gold writes now automatically use the configured format:

```python
from pyspark_interview_project.io.write_table import write_table
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf

# Load config
config = load_conf("config/prod.yaml")

# Build Spark (Iceberg-aware if format=iceberg)
spark = build_spark("my_app", config=config)

# Write - automatically uses Iceberg if configured
write_table(
    df=df_silver,
    table_name="silver.customer_behavior",
    mode="overwrite",
    cfg=config,
    spark=spark
)
```

## Query from Athena

Once written as Iceberg, tables are queryable from Athena:

```sql
-- Query Iceberg table
SELECT * FROM glue_catalog.silver.customer_behavior
WHERE event_ts > '2025-01-01'
LIMIT 100;

-- Time travel
SELECT * FROM glue_catalog.silver.customer_behavior
FOR VERSION AS OF 3;
```

## What Changed

✅ **No code changes needed** - Existing jobs automatically use Iceberg when `format: "iceberg"` is set  
✅ **Bronze unchanged** - Still uses Parquet  
✅ **Backward compatible** - Switch between formats via config  
✅ **Production ready** - All transform jobs updated  

## Verification

1. Check Glue catalog has tables: `silver.customer_behavior`, `gold.customer_360`
2. Query from Athena: `SELECT * FROM glue_catalog.silver.customer_behavior LIMIT 10`
3. Check Spark logs show: "Writing to Iceberg table: glue_catalog.silver.customer_behavior"


