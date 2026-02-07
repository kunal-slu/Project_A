# Local ETL Pipeline Validation

This document describes how to validate your local ETL pipeline to ensure it's working correctly.

## Overview

The validation script (`tools/validate_local_etl.py`) checks that:
1. Silver and Gold output tables are created and populated
2. Row counts are reasonable
3. Schemas match expectations
4. Key columns have acceptable null rates
5. Referential integrity is maintained (fact → dimension joins)
6. Key aggregations make sense

## Prerequisites

1. **Run the ETL pipeline first**:
   ```bash
   # Run Bronze → Silver
   python jobs/transform/bronze_to_silver.py --env local --config local/config/local.yaml
   
   # Run Silver → Gold
   python jobs/transform/silver_to_gold.py --env local --config local/config/local.yaml
   ```

2. **Ensure you have**:
   - Python 3.10+
   - PySpark installed
   - Java 8 or 11 installed
   - Local config file at `local/config/local.yaml` (or specify with `--config`)

## Usage

### Basic Usage

```bash
# Validate both Silver and Gold layers
python tools/validate_local_etl.py --env local --config local/config/local.yaml
```

### Options

```bash
# Validate only Silver layer
python tools/validate_local_etl.py --env local --config local/config/local.yaml --skip-gold

# Validate only Gold layer
python tools/validate_local_etl.py --env local --config local/config/local.yaml --skip-silver

# Use different config file
python tools/validate_local_etl.py --env local --config config/local.yaml
```

## What Gets Validated

### Silver Layer

The script validates these Silver tables:
- `customers_silver` - Customer data from CRM + behavior
- `orders_silver` - Order data with FX normalization
- `products_silver` - Product catalog
- `customer_behavior_silver` - Aggregated behavior metrics
- `fx_rates_silver` - FX exchange rates
- `order_events_silver` - Kafka event stream data

**Checks performed:**
- ✓ Row counts > 0 for critical tables (customers, orders, products)
- ✓ `customer_id` null percentage < 5% in orders
- ✓ `order_id` null percentage = 0% in orders
- ✓ Schema matches expected structure

### Gold Layer

The script validates these Gold tables:
- `fact_orders` - Fact table with surrogate keys
- `dim_customer` - Customer dimension
- `dim_product` - Product dimension
- `dim_date` - Date dimension
- `customer_360` - Customer analytics view
- `product_performance` - Product analytics view

**Checks performed:**
- ✓ Row counts > 0 for critical tables (fact_orders, dim_customer, dim_product)
- ✓ Referential integrity: `fact_orders.customer_sk` and `fact_orders.product_sk` are valid (not -1)
- ✓ Total revenue > 0 in fact_orders
- ✓ Schema matches expected structure

## Output

The script prints:

1. **Table Reports** for each table:
   - Row count
   - Schema (columns, types, nullability)
   - Sample rows (5-10 records)

2. **Sanity Checks**:
   - Pass/fail status for each check
   - Warnings for non-critical issues
   - Errors for critical failures

3. **Summary**:
   - Overall pass/fail count
   - Exit code (0 = success, 1 = failure)

### Example Output

```
================================================================================
🚀 LOCAL ETL PIPELINE VALIDATION
================================================================================
Environment: local
Config: local/config/local.yaml
Silver Root: file:///Users/kunal/IdeaProjects/Project_A/data/silver
Gold Root: file:///Users/kunal/IdeaProjects/Project_A/data/gold

================================================================================
🔍 VALIDATING SILVER LAYER
================================================================================
  ✓ customers_silver: 1,234 rows (parquet)
  ✓ orders_silver: 5,678 rows (parquet)
  ✓ products_silver: 890 rows (parquet)

================================================================================
📊 CUSTOMERS_SILVER
================================================================================
Row Count: 1,234

Schema (8 columns):
  • customer_id: StringType (not null)
  • customer_name: StringType (nullable)
  • segment: StringType (nullable)
  ...

Sample Rows (showing 5 of 1,234):
  [1] customer_id=ACC001, customer_name=Acme Corp, segment=ENTERPRISE, ...
  [2] customer_id=ACC002, customer_name=TechStart Inc, segment=SMB, ...

================================================================================
🔍 SILVER LAYER SANITY CHECKS
================================================================================
✓ customers_silver: 1,234 rows (PASS)
✓ orders_silver: 5,678 rows (PASS)
✓ products_silver: 890 rows (PASS)
✓ customers_silver.customer_id: 0% null (PASS)
✓ orders_silver.order_id: 0% null (PASS)
✓ orders_silver.customer_id: 2.3% null (PASS)

Silver Checks: 6/6 passed

[... Gold layer validation ...]

================================================================================
📋 VALIDATION SUMMARY
================================================================================
Silver Layer: 6/6 checks passed
Gold Layer: 5/5 checks passed

Overall: 11/11 checks passed
✅ All checks passed!
```

## Exit Codes

- `0` - All checks passed (or 80%+ passed with warnings)
- `1` - Critical checks failed (< 80% passed)

## Troubleshooting

### "Table not found" errors

**Problem**: Script can't find Silver/Gold tables

**Solutions**:
1. Ensure you've run the ETL pipeline first:
   ```bash
   python jobs/transform/bronze_to_silver.py --env local
   python jobs/transform/silver_to_gold.py --env local
   ```

2. Check that paths in config match actual data locations:
   ```yaml
   paths:
     silver_root: "data/silver"  # Should match where data is written
     gold_root: "data/gold"
   ```

3. Verify data files exist:
   ```bash
   ls -la data/silver/customers_silver/
   ls -la data/gold/fact_orders/
   ```

### "Empty table" warnings

**Problem**: Tables exist but have 0 rows

**Solutions**:
1. Check input data exists:
   ```bash
   ls -la aws/data/samples/snowflake/
   ls -la aws/data/samples/crm/
   ```

2. Check ETL job logs for errors:
   ```bash
   python jobs/transform/bronze_to_silver.py --env local 2>&1 | tee etl.log
   ```

3. Verify config paths point to correct source files

### "High null percentage" warnings

**Problem**: Key columns have many nulls

**Solutions**:
1. Review source data quality
2. Check transformation logic in `silver_builders.py` and `gold_builders.py`
3. Verify join conditions are correct

### "Orphan rows" in fact_orders

**Problem**: `fact_orders` has rows with `customer_sk = -1` or `product_sk = -1`

**Solutions**:
1. Check that `dim_customer` and `dim_product` have all required keys
2. Verify join logic in `build_fact_orders()` in `gold_builders.py`
3. Review source data for missing customer/product IDs

## Integration with CI/CD

You can integrate this validation into your CI/CD pipeline:

```yaml
# Example GitHub Actions workflow
- name: Run ETL Pipeline
  run: |
    python jobs/transform/bronze_to_silver.py --env local
    python jobs/transform/silver_to_gold.py --env local

- name: Validate ETL Output
  run: |
    python tools/validate_local_etl.py --env local --config local/config/local.yaml
```

## Next Steps

After validation passes:
1. Review the sample rows to ensure data looks correct
2. Check aggregations make business sense
3. Run data quality checks (if configured)
4. Proceed with publishing to downstream systems

## Related Documentation

- [Running Local ETL](docs/RUN_FULL_ETL.md)
- [Data Model](docs/DATA_MODEL.md)
- [Data Quality Framework](docs/DATA_QUALITY_FRAMEWORK.md)

