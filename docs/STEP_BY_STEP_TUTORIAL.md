# PySpark Data Engineering Tutorial

## 🚀 Getting Started

### Step 1: Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Run simple pipeline
python3 simple_pipeline.py

# Run Delta Lake pipeline
python3 delta_pipeline.py
```

### Step 2: Understanding the Data
- **HubSpot CRM**: Customer contacts and deals
- **Snowflake Warehouse**: Orders, customers, products
- **Redshift Analytics**: Customer behavior data
- **Stream Data**: Real-time events
- **FX Rates**: Exchange rate data

## 📊 Data Analysis Workflow

### Step 3: Basic Analysis
```python
# Load data
orders_df = spark.read.csv('aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv', 
                          header=True, inferSchema=True)

# Basic statistics
orders_df.describe().show()
orders_df.groupBy('status').count().show()
```

### Step 4: Advanced Analytics
```python
# Customer lifetime value
customer_ltv = orders_df.groupBy('customer_id')     .agg(sum('total_amount').alias('lifetime_value'))     .orderBy(desc('lifetime_value'))

# Product performance
product_performance = orders_df.groupBy('product_id')     .agg(
        count('*').alias('orders'),
        sum('quantity').alias('total_quantity'),
        sum('total_amount').alias('revenue')
    )     .orderBy(desc('revenue'))
```

## 🏗️ ETL Pipeline Development

### Step 5: Building ETL Pipelines
```python
# Extract
raw_data = spark.read.csv('data/raw/*.csv', header=True, inferSchema=True)

# Transform
cleaned_data = raw_data.filter(col('amount') > 0)     .withColumn('processed_date', current_timestamp())

# Load
cleaned_data.write.mode('overwrite').parquet('data/processed/')
```

## 🔍 Data Quality and Validation

### Step 6: Data Quality Checks
```python
# Check for nulls
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# Check for duplicates
duplicate_count = df.count() - df.dropDuplicates().count()
print(f"Duplicate records: {duplicate_count}")
```

## 📈 Performance Optimization

### Step 7: Optimizing Performance
```python
# Caching
df.cache()

# Partitioning
df.write.partitionBy('date').parquet('data/partitioned/')

# Broadcast joins
small_df = spark.table('dimension_table')
large_df.join(broadcast(small_df), 'key')
```

## 🎯 Best Practices

### Step 8: Production Readiness
- Use Delta Lake for ACID transactions
- Implement proper error handling
- Add monitoring and logging
- Use configuration management
- Implement data quality checks

## 🚀 Next Steps

1. **Practice**: Work through all exercises
2. **Experiment**: Try different data transformations
3. **Optimize**: Focus on performance improvements
4. **Scale**: Test with larger datasets
5. **Deploy**: Learn about production deployment
