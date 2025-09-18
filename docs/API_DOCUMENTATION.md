# PySpark Data Engineering Project - API Documentation

## Overview

This project implements a comprehensive ETL pipeline using PySpark and Delta Lake for processing customer, product, and order data. The architecture follows modern data engineering best practices with a lakehouse pattern (Bronze, Silver, Gold layers).

## Table of Contents

1. [Core Modules](#core-modules)
2. [Data Extraction](#data-extraction)
3. [Data Transformation](#data-transformation)
4. [Data Loading](#data-loading)
5. [Data Quality](#data-quality)
6. [Utilities](#utilities)
7. [Configuration](#configuration)

## Core Modules

### Pipeline Orchestration

#### `pipeline.py`

Main orchestration module that coordinates the entire ETL process.

**Key Functions:**

- `run_pipeline(spark, cfg)`: Main pipeline execution
- `main(config_path)`: Entry point with configuration loading

**Usage:**
```python
from pyspark_interview_project.pipeline import main
main("config/config-dev.yaml")
```

### Data Extraction

#### `extract.py`

Handles data extraction from various sources with schema enforcement.

**Functions:**

##### `extract_customers(spark, path)`
Extracts customer data from CSV with schema validation.

**Parameters:**
- `spark`: SparkSession instance
- `path`: Path to customers CSV file

**Returns:** DataFrame with customer schema

**Schema:**
```python
StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
])
```

##### `extract_products(spark, path)`
Extracts product data from CSV with schema validation.

**Parameters:**
- `spark`: SparkSession instance
- `path`: Path to products CSV file

**Returns:** DataFrame with product schema

##### `extract_orders_json(spark, path)`
Extracts order data from JSON with nested payment information.

**Parameters:**
- `spark`: SparkSession instance
- `path`: Path to orders JSON file

**Returns:** DataFrame with order schema including payment map

## Data Transformation

#### `transform.py`

Core transformation logic with performance optimizations and business logic.

**Key Functions:**

##### `join_examples(customers, products, orders)`
Performs optimized joins between orders, customers, and products.

**Performance Features:**
- Automatic caching of dimension tables
- Broadcast join optimization for small tables
- Memory management with unpersist()

**Returns:** Denormalized order view with customer and product information

##### `enrich_customers(df)`
Adds derived fields for customer analytics.

**Added Columns:**
- `email_domain`: Extracted from email address
- `age_bucket`: Age grouped into 10-year buckets
- `region`: Geographic region based on US state

##### `enrich_products(df)`
Enhances product data with derived fields.

**Added Columns:**
- `tags_array`: Parsed array of product tags
- `price_band`: Price categorization (low/mid/high)

##### `build_customers_scd2(customers, changes)`
Implements Slowly Changing Dimension Type 2 for customer address changes.

**Features:**
- Comprehensive error handling
- Input validation
- Historical versioning with effective dates
- Current record flagging

**Returns:** SCD2 customer dimension with effective_from, effective_to, is_current

##### `optimize_for_analytics(df)`
Applies performance optimizations for analytical queries.

**Optimizations:**
- DataFrame caching
- Adaptive partitioning
- Broadcast join hints

##### `adaptive_join_strategy(df1, df2, join_key)`
Intelligent join strategy selection based on table characteristics.

**Strategies:**
- Broadcast join for small tables
- Sort-merge join for large tables
- Automatic size-based selection

## Data Loading

#### `load.py`

Handles data persistence with multiple formats and Delta Lake integration.

**Functions:**

##### `write_delta(df, path, mode, partition_by)`
Writes DataFrame to Delta Lake format.

**Parameters:**
- `df`: DataFrame to write
- `path`: Output path
- `mode`: Write mode (overwrite/append)
- `partition_by`: List of partition columns

##### `write_parquet(df, path)`
Writes DataFrame to Parquet format.

##### `read_delta(spark, path)`
Reads DataFrame from Delta Lake format.

## Data Quality

#### `dq_checks.py`

Comprehensive data quality validation framework.

**Class:** `DQChecks`

**Methods:**

##### `assert_non_null(df, col_name)`
Validates that a column contains no null values.

**Raises:** ValueError if nulls found

##### `assert_unique(df, col_name)`
Validates that a column contains unique values.

**Raises:** ValueError if duplicates found

##### `assert_referential_integrity(child_df, child_key, parent_df, parent_key)`
Validates foreign key relationships.

**Returns:** Count of integrity violations

##### `assert_age_range(df, min_age, max_age)`
Validates age values within specified range.

**Returns:** Count of out-of-range values

##### `assert_email_valid(df, email_regex)`
Validates email format using regex pattern.

**Returns:** Count of invalid emails

## Utilities

#### `utils.py`

Core utility functions for Spark configuration and setup.

**Functions:**

##### `get_spark_session(cfg)`
Creates optimized SparkSession with Delta Lake configuration.

**Features:**
- Delta Lake extensions
- Adaptive query execution
- Broadcast join optimization
- Kafka integration (optional)

##### `load_config(config_path)`
Loads YAML configuration file.

**Returns:** Dictionary with pipeline configuration

#### `delta_utils.py`

Delta Lake specific utilities for table management.

**Functions:**

##### `merge_upsert(spark, incoming_df, target_path, keys, partition_by)`
Performs idempotent upsert operations on Delta tables.

**Features:**
- Automatic table creation
- MERGE operation for updates/inserts
- Partition support

##### `table_exists(spark, path)`
Checks if Delta table exists at specified path.

## Configuration

### Configuration Structure

The project uses YAML configuration files for environment-specific settings.

**Example Configuration:**
```yaml
input:
  customer_path: data/input_data/customers.csv
  product_path: data/input_data/products.csv
  orders_path: data/input_data/orders.json

output:
  parquet_path: data/output_data/final.parquet
  delta_path: data/output_data/final_delta
  bronze_path: data/lakehouse/bronze
  silver_path: data/lakehouse/silver
  gold_path: data/lakehouse/gold

spark:
  app_name: "PDEInterviewPipeline"
  shuffle_partitions: 8

dq_rules:
  min_age: 18
  max_age: 99
  email_regex: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
```

### Environment-Specific Configs

- `config-dev.yaml`: Development environment settings
- `config-prod.yaml`: Production environment settings

## Performance Optimizations

### Caching Strategy
- Dimension tables are cached for multiple join operations
- Results are cached for downstream processing
- Memory is managed with unpersist() calls

### Join Optimization
- Automatic broadcast join detection for small tables
- Adaptive join strategy selection
- Skew mitigation with salting techniques

### Partitioning
- Year-month partitioning for time-series data
- Customer-based partitioning for dimension tables
- Adaptive partitioning based on cluster size

## Error Handling

### Exception Types
- `ValueError`: Configuration and validation errors
- `FileNotFoundError`: Missing input files
- `AnalysisException`: Spark operation failures
- `TypeError`: Data type mismatches

### Graceful Degradation
- Optional streaming components
- Maintenance operations
- Non-critical pipeline steps

## Testing

### Test Coverage
- Unit tests for all transformation functions
- Integration tests for pipeline components
- Delta Lake format validation
- Data quality rule validation

### Test Data
- Realistic test datasets generated with Faker
- Multiple scenarios (normal, edge cases, errors)
- Performance benchmarking capabilities

## Monitoring and Observability

### Metrics Collection
- Pipeline execution time
- Record counts per stage
- Data quality violation counts
- Performance metrics

### Logging
- Structured logging throughout pipeline
- Error tracking with context
- Performance monitoring
- Data lineage tracking

## Best Practices

### Code Quality
- Comprehensive docstrings
- Type hints for all functions
- Error handling with specific exceptions
- Performance optimization strategies

### Data Engineering
- Lakehouse architecture (Bronze/Silver/Gold)
- Schema enforcement and validation
- Data quality monitoring
- Idempotent operations

### Production Readiness
- Configuration-driven design
- Comprehensive testing
- Error handling and recovery
- Performance optimization
- Monitoring and observability

## Usage Examples

### Basic Pipeline Execution
```python
from pyspark_interview_project.pipeline import main
main("config/config-dev.yaml")
```

### Custom Transformation
```python
from pyspark_interview_project.transform import enrich_customers, optimize_for_analytics

# Enrich customer data
enriched_customers = enrich_customers(customers_df)

# Optimize for analytics
optimized_df = optimize_for_analytics(enriched_customers)
```

### Data Quality Validation
```python
from pyspark_interview_project.dq_checks import DQChecks

dq = DQChecks()
dq.assert_non_null(customers_df, "customer_id")
dq.assert_unique(customers_df, "customer_id")
```

### Delta Lake Operations
```python
from pyspark_interview_project.delta_utils import merge_upsert

# Upsert data to Delta table
merge_upsert(spark, new_data, "path/to/table", ["id"], ["partition_col"])
```

## Troubleshooting

### Common Issues

1. **Memory Issues**: Reduce partition count or increase executor memory
2. **Broadcast Join Failures**: Check table sizes and adjust broadcast threshold
3. **Schema Mismatches**: Validate input data against expected schemas
4. **Performance Issues**: Use optimize_for_analytics() and adaptive joins

### Debugging Tips

1. Use `df.explain()` to analyze query plans
2. Check partition distribution with `df.rdd.getNumPartitions()`
3. Monitor memory usage with Spark UI
4. Validate data quality with DQ checks

## Contributing

### Code Standards
- Follow PEP 8 style guidelines
- Add comprehensive docstrings
- Include type hints
- Write unit tests for new functions

### Testing
- Run full test suite before submitting
- Add tests for new functionality
- Validate performance improvements
- Test error handling scenarios
