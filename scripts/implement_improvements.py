#!/usr/bin/env python3
"""
Implement Critical Improvements
Fix data quality issues and add essential learning features.
"""

import pandas as pd
import os
import yaml
from datetime import datetime, timedelta
import random

def fix_data_quality_issues():
    """Fix critical data quality issues"""
    print("🔧 FIXING DATA QUALITY ISSUES")
    print("=============================")
    print()
    
    # Fix HubSpot Contacts
    print("1️⃣ Fixing HubSpot Contacts...")
    contacts_df = pd.read_csv('aws/data_fixed/01_hubspot_crm/hubspot_contacts_25000.csv')
    
    # Fill null notes with realistic values
    contacts_df['notes'] = contacts_df['notes'].fillna('No additional notes')
    
    # Ensure unique customer_ids
    contacts_df = contacts_df.drop_duplicates(subset=['customer_id'], keep='first')
    
    contacts_df.to_csv('aws/data_fixed/01_hubspot_crm/hubspot_contacts_25000.csv', index=False)
    print(f"   ✅ Fixed: {len(contacts_df)} records")
    
    # Fix HubSpot Deals
    print("\n2️⃣ Fixing HubSpot Deals...")
    deals_df = pd.read_csv('aws/data_fixed/01_hubspot_crm/hubspot_deals_30000.csv')
    
    # Fill null notes with realistic values
    deals_df['notes'] = deals_df['notes'].fillna('Deal in progress')
    
    # Ensure unique customer_ids
    deals_df = deals_df.drop_duplicates(subset=['customer_id'], keep='first')
    
    deals_df.to_csv('aws/data_fixed/01_hubspot_crm/hubspot_deals_30000.csv', index=False)
    print(f"   ✅ Fixed: {len(deals_df)} records")
    
    # Fix Snowflake Orders (customers can have multiple orders, so this is OK)
    print("\n3️⃣ Verifying Snowflake Orders...")
    orders_df = pd.read_csv('aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv')
    
    # Check for duplicate order_ids (should be unique)
    duplicate_orders = orders_df.duplicated(subset=['order_id']).sum()
    if duplicate_orders > 0:
        orders_df = orders_df.drop_duplicates(subset=['order_id'], keep='first')
        orders_df.to_csv('aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv', index=False)
        print(f"   ✅ Removed {duplicate_orders} duplicate order_ids")
    else:
        print("   ✅ No duplicate order_ids found")
    
    print(f"   ✅ Orders: {len(orders_df)} records")

def add_learning_exercises():
    """Create comprehensive learning exercises"""
    print("\n📚 CREATING LEARNING EXERCISES")
    print("=============================")
    print()
    
    exercises_content = """# PySpark Learning Exercises

## 🎯 Beginner Level

### Exercise 1: Basic Data Operations
```python
# Read data from all sources
orders_df = spark.read.csv('aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv', header=True, inferSchema=True)
customers_df = spark.read.csv('aws/data_fixed/02_snowflake_warehouse/snowflake_customers_50000.csv', header=True, inferSchema=True)

# Basic operations
orders_df.show(5)
orders_df.count()
orders_df.printSchema()
```

### Exercise 2: Data Filtering and Selection
```python
# Filter high-value orders
high_value_orders = orders_df.filter(col('total_amount') > 1000)
high_value_orders.count()

# Select specific columns
customer_info = customers_df.select('customer_id', 'first_name', 'last_name', 'email')
```

## 🎯 Intermediate Level

### Exercise 3: Aggregations and Grouping
```python
# Revenue by customer
revenue_by_customer = orders_df.groupBy('customer_id') \
    .agg(sum('total_amount').alias('total_revenue')) \
    .orderBy(desc('total_revenue'))

# Monthly revenue trend
monthly_revenue = orders_df.withColumn('month', col('order_date').substr(1, 7)) \
    .groupBy('month') \
    .agg(sum('total_amount').alias('monthly_revenue'))
```

### Exercise 4: Joins and Relationships
```python
# Join orders with customers
orders_with_customers = orders_df.join(customers_df, 'customer_id', 'inner')
orders_with_customers.show(5)
```

## 🎯 Advanced Level

### Exercise 5: Window Functions
```python
from pyspark.sql.window import Window

# Running total by customer
window_spec = Window.partitionBy('customer_id').orderBy('order_date')
orders_with_running_total = orders_df.withColumn('running_total', 
    sum('total_amount').over(window_spec))
```

### Exercise 6: Complex Analytics
```python
# Customer segmentation
customer_segments = orders_df.groupBy('customer_id') \
    .agg(
        count('*').alias('order_count'),
        sum('total_amount').alias('total_spent'),
        avg('total_amount').alias('avg_order_value')
    ) \
    .withColumn('segment', 
        when(col('total_spent') >= 5000, 'High Value')
        .when(col('total_spent') >= 1000, 'Medium Value')
        .otherwise('Low Value')
    )
```

## 🎯 Expert Level

### Exercise 7: Performance Optimization
```python
# Caching and partitioning
orders_df.cache()
orders_df.repartition(10, 'customer_id')

# Broadcast joins for small tables
from pyspark.sql.functions import broadcast
small_table = spark.table('small_dimension')
large_table.join(broadcast(small_table), 'key')
```

### Exercise 8: Streaming Data Processing
```python
# Structured streaming
streaming_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'orders') \
    .load()

# Process streaming data
processed_stream = streaming_df.select(
    col('value').cast('string').alias('order_data')
).writeStream \
.outputMode('append') \
.format('console') \
.start()
```
"""
    
    with open('docs/LEARNING_EXERCISES.md', 'w') as f:
        f.write(exercises_content)
    
    print("✅ Created comprehensive learning exercises")

def add_delta_lake_support():
    """Add Delta Lake support for local development"""
    print("\n🏗️ ADDING DELTA LAKE SUPPORT")
    print("=============================")
    print()
    
    # Create Delta Lake configuration
    delta_config = """
# Delta Lake Configuration for Local Development
spark.sql.extensions: io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog: org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.databricks.delta.retentionDurationCheck.enabled: false
spark.databricks.delta.merge.repartitionBeforeWrite: true
"""
    
    with open('config/delta-local.yaml', 'w') as f:
        f.write(delta_config)
    
    # Create Delta Lake pipeline
    delta_pipeline_content = '''#!/usr/bin/env python3
"""
Delta Lake Pipeline for Local Development
Demonstrates Delta Lake features with the project data.
"""

import sys
import os
sys.path.insert(0, 'src')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max
from delta import configure_spark_with_delta_pip

def create_delta_spark_session():
    """Create SparkSession with Delta Lake support"""
    builder = SparkSession.builder \
        .appName("DeltaLakePipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def main():
    """Main Delta Lake pipeline"""
    print("🏗️ DELTA LAKE PIPELINE")
    print("======================")
    
    spark = create_delta_spark_session()
    
    try:
        # Read data
        orders_df = spark.read.option('header', 'true').option('inferSchema', 'true') \
            .csv('aws/data_fixed/02_snowflake_warehouse/snowflake_orders_100000.csv')
        
        # Write to Delta Lake
        print("📝 Writing to Delta Lake...")
        orders_df.write.format('delta').mode('overwrite') \
            .save('data/delta/orders')
        
        # Read from Delta Lake
        print("📖 Reading from Delta Lake...")
        delta_orders = spark.read.format('delta').load('data/delta/orders')
        
        # Demonstrate Delta Lake features
        print("🔍 Delta Lake Analytics:")
        print(f"   Records: {delta_orders.count():,}")
        
        # Time travel (if we had multiple versions)
        print("⏰ Delta Lake features available:")
        print("   ✅ ACID transactions")
        print("   ✅ Time travel")
        print("   ✅ Schema evolution")
        print("   ✅ Upserts and merges")
        
        print("✅ Delta Lake pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Delta Lake pipeline failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
'''
    
    with open('delta_pipeline.py', 'w') as f:
        f.write(delta_pipeline_content)
    
    print("✅ Added Delta Lake support")

def create_enhanced_tutorials():
    """Create step-by-step tutorials"""
    print("\n📖 CREATING ENHANCED TUTORIALS")
    print("==============================")
    print()
    
    tutorial_content = """# PySpark Data Engineering Tutorial

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
customer_ltv = orders_df.groupBy('customer_id') \
    .agg(sum('total_amount').alias('lifetime_value')) \
    .orderBy(desc('lifetime_value'))

# Product performance
product_performance = orders_df.groupBy('product_id') \
    .agg(
        count('*').alias('orders'),
        sum('quantity').alias('total_quantity'),
        sum('total_amount').alias('revenue')
    ) \
    .orderBy(desc('revenue'))
```

## 🏗️ ETL Pipeline Development

### Step 5: Building ETL Pipelines
```python
# Extract
raw_data = spark.read.csv('data/raw/*.csv', header=True, inferSchema=True)

# Transform
cleaned_data = raw_data.filter(col('amount') > 0) \
    .withColumn('processed_date', current_timestamp())

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
"""
    
    with open('docs/STEP_BY_STEP_TUTORIAL.md', 'w') as f:
        f.write(tutorial_content)
    
    print("✅ Created comprehensive tutorials")

def add_complex_data_relationships():
    """Add more complex data relationships"""
    print("\n🔗 ADDING COMPLEX DATA RELATIONSHIPS")
    print("===================================")
    print()
    
    # Create additional related data
    print("1️⃣ Creating Customer Segments...")
    segments_data = []
    for i in range(100):
        segments_data.append({
            'segment_id': f'SEG-{i:03d}',
            'segment_name': f'Segment {i}',
            'description': f'Customer segment {i}',
            'min_value': i * 100,
            'max_value': (i + 1) * 100,
            'created_at': '2024-01-01',
            'updated_at': '2024-01-01'
        })
    
    segments_df = pd.DataFrame(segments_data)
    segments_df.to_csv('aws/data_fixed/02_snowflake_warehouse/snowflake_customer_segments_100.csv', index=False)
    print(f"   ✅ Created: {len(segments_df)} customer segments")
    
    # Create product categories
    print("\n2️⃣ Creating Product Categories...")
    categories_data = []
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Beauty', 'Automotive', 'Toys']
    
    for i, category in enumerate(categories):
        categories_data.append({
            'category_id': f'CAT-{i:03d}',
            'category_name': category,
            'description': f'{category} products',
            'parent_category_id': None,
            'created_at': '2024-01-01',
            'updated_at': '2024-01-01'
        })
    
    categories_df = pd.DataFrame(categories_data)
    categories_df.to_csv('aws/data_fixed/02_snowflake_warehouse/snowflake_product_categories_8.csv', index=False)
    print(f"   ✅ Created: {len(categories_df)} product categories")
    
    # Create time-series data
    print("\n3️⃣ Creating Time-Series Data...")
    dates = pd.date_range('2023-01-01', '2024-12-31', freq='D')
    time_series_data = []
    
    for date in dates:
        time_series_data.append({
            'date': date.strftime('%Y-%m-%d'),
            'total_orders': random.randint(100, 1000),
            'total_revenue': random.uniform(50000, 200000),
            'active_customers': random.randint(1000, 5000),
            'new_customers': random.randint(50, 200)
        })
    
    time_series_df = pd.DataFrame(time_series_data)
    time_series_df.to_csv('aws/data_fixed/02_snowflake_warehouse/snowflake_daily_metrics_730.csv', index=False)
    print(f"   ✅ Created: {len(time_series_df)} daily metrics")
    
    print("\n✅ Complex data relationships added!")

def main():
    """Main improvement implementation"""
    print("🚀 IMPLEMENTING CRITICAL IMPROVEMENTS")
    print("====================================")
    print()
    
    try:
        # Fix data quality issues
        fix_data_quality_issues()
        
        # Add learning exercises
        add_learning_exercises()
        
        # Add Delta Lake support
        add_delta_lake_support()
        
        # Create enhanced tutorials
        create_enhanced_tutorials()
        
        # Add complex data relationships
        add_complex_data_relationships()
        
        print("\n✅ ALL IMPROVEMENTS IMPLEMENTED!")
        print("===============================")
        print("🎯 Data quality issues fixed")
        print("📚 Learning exercises created")
        print("🏗️ Delta Lake support added")
        print("📖 Enhanced tutorials created")
        print("🔗 Complex relationships added")
        print("\n🚀 Project is now significantly improved!")
        
    except Exception as e:
        print(f"❌ Implementation failed: {e}")

if __name__ == "__main__":
    main()
