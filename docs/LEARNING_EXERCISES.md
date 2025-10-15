# PySpark Learning Exercises

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
revenue_by_customer = orders_df.groupBy('customer_id')     .agg(sum('total_amount').alias('total_revenue'))     .orderBy(desc('total_revenue'))

# Monthly revenue trend
monthly_revenue = orders_df.withColumn('month', col('order_date').substr(1, 7))     .groupBy('month')     .agg(sum('total_amount').alias('monthly_revenue'))
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
customer_segments = orders_df.groupBy('customer_id')     .agg(
        count('*').alias('order_count'),
        sum('total_amount').alias('total_spent'),
        avg('total_amount').alias('avg_order_value')
    )     .withColumn('segment', 
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
streaming_df = spark.readStream     .format('kafka')     .option('kafka.bootstrap.servers', 'localhost:9092')     .option('subscribe', 'orders')     .load()

# Process streaming data
processed_stream = streaming_df.select(
    col('value').cast('string').alias('order_data')
).writeStream .outputMode('append') .format('console') .start()
```
