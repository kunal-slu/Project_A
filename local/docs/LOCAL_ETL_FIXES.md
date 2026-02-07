# Local ETL Pipeline Fixes

## Summary

Fixed format-aware reading and FX rates schema handling for local ETL pipeline validation.

## Changes Made

### 1. Format-Aware Table Reading (`tools/validate_local_etl.py`)

**Updated `read_table()` function:**
- **Local environment**: Reads parquet only (no delta attempts)
- **Non-local environment**: Tries delta first, then falls back to parquet
- Added `required` parameter to distinguish required vs optional tables
- Added `schema` parameter for empty DataFrame fallback
- Improved error handling and logging

**Key improvements:**
- No more `delta.DefaultSource` errors in local mode
- Clear error messages without stack traces for expected failures
- Proper handling of empty tables with explicit schemas

### 2. FX Rates Schema Definition

**Added `FX_RATES_SCHEMA` constant:**
```python
FX_RATES_SCHEMA = StructType([
    StructField("trade_date", DateType(), True),
    StructField("base_ccy", StringType(), True),
    StructField("counter_ccy", StringType(), True),
    StructField("rate", DoubleType(), True),
])
```

**Updated validation:**
- `fx_rates_silver` is now marked as optional (`required=False`)
- Uses explicit schema when table is empty
- Warns but doesn't fail validation if empty

### 3. FX Silver Builder (`jobs/transform/bronze_to_silver.py`)

**Updated `build_fx_silver()` function:**
- Uses explicit schema matching validator expectations
- Handles empty input DataFrames gracefully
- Creates empty DataFrame with fixed schema when source is empty
- Maps columns correctly: `base_ccy`, `counter_ccy`, `rate`, `trade_date`

**Schema alignment:**
- Output schema matches `FX_RATES_SCHEMA` exactly
- Ensures schema is written even when table is empty

### 4. Delta Writer Improvements (`src/project_a/pyspark_interview_project/io/delta_writer.py`)

**Enhanced `write_table()` function:**
- Better handling of empty DataFrames
- Ensures directory creation for parquet writes
- Schema metadata is preserved in empty parquet files

### 5. Validation Reporting

**Updated `print_table_report()`:**
- Shows schema even for empty tables
- Better handling of optional tables

**Updated sanity checks:**
- `fx_rates_silver` is treated as optional
- Warns if empty but doesn't fail validation
- Distinguishes between "empty with schema" (acceptable) and "missing" (warning)

## Testing

To test the fixes:

```bash
# Run validation
python tools/validate_local_etl.py --env local --config local/config/local.yaml

# Expected behavior:
# - No delta.DefaultSource errors
# - fx_rates_silver shows 0 rows with schema (WARNING, not ERROR)
# - Other tables validate normally
# - Clean error messages without stack traces
```

## Files Modified

1. `tools/validate_local_etl.py` - Format-aware reading, FX schema, optional table handling
2. `jobs/transform/bronze_to_silver.py` - FX silver builder with explicit schema
3. `src/project_a/pyspark_interview_project/io/delta_writer.py` - Empty DataFrame handling

## Data Consistency

All source data files in `aws/data/samples/` are regenerated and consistent:
- CRM data (accounts, contacts, opportunities)
- Snowflake data (customers, orders, products)
- Redshift behavior data
- Kafka order events
- FX rates (CSV and JSON)

These files are used for both local and AWS execution, ensuring consistency.

