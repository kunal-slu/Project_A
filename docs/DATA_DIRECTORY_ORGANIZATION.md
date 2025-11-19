# Data Directory Organization

**Date:** 2025-01-17  
**Status:** ✅ Complete

## Summary

The `data/` directory has been reorganized to match the structure of `aws/data/`, ensuring consistency between local and AWS environments.

## Structure

### Source Data (`data/samples/`)

Matches `aws/data/samples/` structure:

```
data/samples/
├── crm/
│   ├── accounts.csv
│   ├── contacts.csv
│   └── opportunities.csv
├── snowflake/
│   ├── snowflake_customers_50000.csv
│   ├── snowflake_orders_100000.csv
│   └── snowflake_products_10000.csv
├── redshift/
│   └── redshift_customer_behavior_50000.csv
├── fx/
│   ├── fx_rates_historical.json          # Matches aws/data/samples/fx/
│   ├── fx_rates_historical_730_days.csv  # Additional (not in aws)
│   └── financial_metrics_24_months.csv   # Additional (not in aws)
└── kafka/
    └── stream_kafka_events_100000.csv
```

### ETL Output Directories

These directories are preserved for ETL pipeline output:

```
data/
├── bronze/        # Bronze layer output (Delta/Parquet)
├── silver/        # Silver layer output (Delta/Parquet)
├── gold/          # Gold layer output (Delta/Parquet)
├── checkpoints/   # Spark checkpoints
└── metrics/       # Pipeline metrics
```

## Changes Made

1. ✅ Created `data/samples/` directory structure
2. ✅ Moved all source CSV/JSON files to `data/samples/{source}/`
3. ✅ Removed empty source directories (`data/crm/`, `data/snowflake/`, etc.)
4. ✅ Copied `fx_rates_historical.json` from `aws/data/samples/fx/`
5. ✅ Preserved ETL output directories (`bronze/`, `silver/`, `gold/`)

## File Mapping

| Source Location | Target Location | Status |
|----------------|-----------------|--------|
| `data/crm/*.csv` | `data/samples/crm/*.csv` | ✅ Moved |
| `data/snowflake/*.csv` | `data/samples/snowflake/*.csv` | ✅ Moved |
| `data/redshift/*.csv` | `data/samples/redshift/*.csv` | ✅ Moved |
| `data/kafka/*.csv` | `data/samples/kafka/*.csv` | ✅ Moved |
| `data/fx/*.csv` | `data/samples/fx/*.csv` | ✅ Moved |
| `aws/data/samples/fx/fx_rates_historical.json` | `data/samples/fx/fx_rates_historical.json` | ✅ Copied |

## Additional Files

The following files are in `data/samples/` but not in `aws/data/samples/`:

- `fx/fx_rates_historical_730_days.csv` - Additional FX data (useful for testing)
- `fx/financial_metrics_24_months.csv` - Additional metrics data (useful for testing)
- `data_quality_report.json` - DQ report (can be moved to `data/metrics/` if needed)

These are kept as they provide additional test data.

## Verification

```bash
# Compare structures
find data/samples -type f | sort
find aws/data/samples -type f | sort

# Verify file counts
ls -1 data/samples/crm/*.csv | wc -l    # Should be 3
ls -1 data/samples/snowflake/*.csv | wc -l  # Should be 3
ls -1 data/samples/redshift/*.csv | wc -l  # Should be 1
ls -1 data/samples/kafka/*.csv | wc -l     # Should be 1
ls -1 data/samples/fx/*.{json,csv} | wc -l # Should be 3
```

## Benefits

1. ✅ **Consistency** - Local and AWS use the same structure
2. ✅ **Clarity** - Source data clearly separated from ETL output
3. ✅ **Maintainability** - Easy to sync between local and AWS
4. ✅ **Organization** - Clean, industry-standard structure

## Next Steps

1. ✅ Structure organized
2. ⏳ Update config files if needed to reference `data/samples/`
3. ⏳ Verify ETL jobs can find source files in new locations

