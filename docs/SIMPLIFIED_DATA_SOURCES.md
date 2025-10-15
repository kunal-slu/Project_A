# Simplified Data Sources - 5 Essential Sources

## 🎯 Overview

The data has been simplified to include **only essential files** for each of the 5 data sources, providing focused PySpark practice without overwhelming complexity.

## 📊 The 5 Simplified Data Sources

### 1️⃣ **HUBSPOT CRM** (2 files, 55K records)
**Location**: `aws/data_fixed/01_hubspot_crm/`

**Purpose**: Customer relationship management and sales pipeline

**Essential Files**:
- `hubspot_contacts_25000.csv` (25K records) - Customer contact information
- `hubspot_deals_30000.csv` (30K records) - Sales opportunities and pipeline

**Key Fields**:
- Contacts: `hubspot_contact_id`, `customer_id`, `email`, `lead_status`, `lifecycle_stage`
- Deals: `hubspot_deal_id`, `contact_id`, `amount`, `deal_stage`, `close_date`

### 2️⃣ **SNOWFLAKE WAREHOUSE** (3 files, 160K records)
**Location**: `aws/data_fixed/02_snowflake_warehouse/`

**Purpose**: Core business data warehouse

**Essential Files**:
- `snowflake_customers_50000.csv` (50K records) - Customer master data
- `snowflake_orders_100000.csv` (100K records) - Order transactions
- `snowflake_products_10000.csv` (10K records) - Product catalog

**Key Fields**:
- Customers: `customer_id`, `email`, `customer_segment`, `lifetime_value`
- Orders: `order_id`, `customer_id`, `product_id`, `total_amount`, `order_date`
- Products: `product_id`, `sku`, `category`, `price_usd`

### 3️⃣ **REDSHIFT ANALYTICS** (1 file, 50K records)
**Location**: `aws/data_fixed/03_redshift_analytics/`

**Purpose**: Customer behavior analytics

**Essential Files**:
- `redshift_customer_behavior_50000.csv` (50K records) - User behavior analytics

**Key Fields**:
- Behavior: `customer_id`, `event_name`, `event_timestamp`, `session_id`, `page_url`

### 4️⃣ **STREAM DATA** (1 file, 100K records)
**Location**: `aws/data_fixed/04_stream_data/`

**Purpose**: Real-time event streaming

**Essential Files**:
- `stream_kafka_events_100000.csv` (100K records) - Real-time event streaming

**Key Fields**:
- Events: `event_id`, `topic`, `timestamp`, `key`, `value` (JSON)

### 5️⃣ **FX RATES** (1 file, 20K records)
**Location**: `aws/data_fixed/05_fx_rates/`

**Purpose**: Multi-currency support

**Essential Files**:
- `fx_rates_historical_730_days.csv` (20K records) - 2 years of daily exchange rates

**Key Fields**:
- Rates: `as_of_date`, `base_currency`, `target_currency`, `exchange_rate`

## 🔗 Essential Data Relationships

### Primary Relationships
```
Snowflake Customers ↔ Snowflake Orders (via customer_id)
Snowflake Products ↔ Snowflake Orders (via product_id)
HubSpot Contacts ↔ HubSpot Deals (via contact_id)
HubSpot Contacts ↔ Snowflake Customers (via customer_id)
Redshift Behavior ↔ Snowflake Customers (via customer_id)
Stream Events ↔ Snowflake Customers (via customer_id in JSON)
Snowflake Orders ↔ FX Rates (via currency)
```

## 🎯 PySpark Practice Scenarios

### **Basic Data Operations**
```python
# Read essential data sources
customers = spark.read.csv("02_snowflake_warehouse/snowflake_customers_50000.csv", header=True, inferSchema=True)
orders = spark.read.csv("02_snowflake_warehouse/snowflake_orders_100000.csv", header=True, inferSchema=True)
products = spark.read.csv("02_snowflake_warehouse/snowflake_products_10000.csv", header=True, inferSchema=True)
hubspot_contacts = spark.read.csv("01_hubspot_crm/hubspot_contacts_25000.csv", header=True, inferSchema=True)
hubspot_deals = spark.read.csv("01_hubspot_crm/hubspot_deals_30000.csv", header=True, inferSchema=True)
behavior = spark.read.csv("03_redshift_analytics/redshift_customer_behavior_50000.csv", header=True, inferSchema=True)
kafka_events = spark.read.csv("04_stream_data/stream_kafka_events_100000.csv", header=True, inferSchema=True)
fx_rates = spark.read.csv("05_fx_rates/fx_rates_historical_730_days.csv", header=True, inferSchema=True)
```

### **Core Business Analytics**
```python
# Customer lifetime value analysis
customer_ltv = orders.groupBy("customer_id") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("lifetime_value"),
        avg("total_amount").alias("avg_order_value")
    )

# Product performance analysis
product_performance = orders.groupBy("product_id") \
    .agg(
        count("order_id").alias("orders_count"),
        sum("total_amount").alias("revenue"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .join(products, "product_id") \
    .select("product_id", "category", "orders_count", "revenue", "unique_customers")
```

### **CRM Analytics**
```python
# Sales pipeline analysis
pipeline_analysis = hubspot_deals.groupBy("deal_stage") \
    .agg(
        count("hubspot_deal_id").alias("deal_count"),
        sum("amount").alias("total_value"),
        avg("amount").alias("avg_deal_size")
    )

# Customer 360 view
customer_360 = customers.join(orders, "customer_id", "left") \
    .join(hubspot_contacts, "customer_id", "left") \
    .join(behavior, "customer_id", "left") \
    .select(
        customers.customer_id,
        customers.customer_segment,
        orders.total_amount,
        hubspot_contacts.lead_status,
        behavior.event_name
    )
```

### **Real-time Event Processing**
```python
# Parse JSON events from Kafka
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Define schema for JSON parsing
json_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Parse streaming events
parsed_events = kafka_events.withColumn("parsed_value", 
    from_json(col("value"), json_schema)) \
    .select(
        col("event_id"),
        col("timestamp"),
        col("parsed_value.customer_id").alias("customer_id"),
        col("parsed_value.event_type").alias("event_type"),
        col("parsed_value.amount").alias("amount")
    )
```

### **Multi-Currency Analysis**
```python
# Currency conversion for orders
orders_with_fx = orders.join(fx_rates, 
    (orders.currency == fx_rates.target_currency) & 
    (orders.order_date == fx_rates.as_of_date), "left") \
    .withColumn("amount_usd", 
        when(col("currency") == "USD", col("total_amount"))
        .otherwise(col("total_amount") * col("exchange_rate")))
```

## 📈 Essential Business Queries

### **Customer Analytics**
```sql
-- Top customers by revenue
SELECT 
    c.customer_id,
    c.customer_segment,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as lifetime_value,
    hc.lead_status
FROM snowflake_customers c
LEFT JOIN snowflake_orders o ON c.customer_id = o.customer_id
LEFT JOIN hubspot_contacts hc ON c.customer_id = hc.customer_id
GROUP BY c.customer_id, c.customer_segment, hc.lead_status
ORDER BY lifetime_value DESC
LIMIT 100;
```

### **Product Performance**
```sql
-- Product performance by category
SELECT 
    p.category,
    COUNT(o.order_id) as orders_count,
    SUM(o.total_amount) as revenue,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM snowflake_products p
JOIN snowflake_orders o ON p.product_id = o.product_id
GROUP BY p.category
ORDER BY revenue DESC;
```

### **Sales Pipeline Analysis**
```sql
-- HubSpot sales pipeline
SELECT 
    deal_stage,
    COUNT(hubspot_deal_id) as deal_count,
    SUM(amount) as total_value,
    AVG(amount) as avg_deal_size
FROM hubspot_deals
GROUP BY deal_stage
ORDER BY total_value DESC;
```

## 🚀 Key Learning Objectives

### **Data Engineering Skills**
1. **Multi-Source ETL**: Extract from 5 different data sources
2. **Data Integration**: Combine CRM, warehouse, analytics, and streaming data
3. **JSON Processing**: Parse complex streaming events
4. **Currency Conversion**: Handle multi-currency data
5. **Data Quality**: Validate and clean data across sources

### **Analytics Skills**
1. **Customer Analytics**: Lifetime value, segmentation, behavior
2. **Product Analytics**: Performance, category analysis
3. **Sales Analytics**: Pipeline analysis, deal tracking
4. **Real-time Analytics**: Event processing, streaming analytics
5. **Financial Analytics**: Multi-currency analysis, FX impact

### **PySpark Skills**
1. **DataFrames**: Read, transform, and write data
2. **Joins**: Complex multi-table joins
3. **Aggregations**: GroupBy, window functions
4. **JSON Processing**: Parse and extract from JSON
5. **Performance**: Optimization, partitioning, caching

## 📊 Simplified Data Summary

| Data Source | Files | Records | Purpose |
|-------------|-------|---------|---------|
| **HubSpot CRM** | 2 | 55,000 | Customer relationship management |
| **Snowflake Warehouse** | 3 | 160,000 | Core business data |
| **Redshift Analytics** | 1 | 50,000 | Customer behavior analytics |
| **Stream Data** | 1 | 100,000 | Real-time events |
| **FX Rates** | 1 | 20,000 | Multi-currency support |
| **TOTAL** | **8** | **385,000** | **Essential pipeline** |

## 🎯 Perfect for PySpark Practice

This simplified structure provides:
- **Focused Learning**: Only essential files, no overwhelming complexity
- **Real Business Scenarios**: Actual CRM, warehouse, analytics, and streaming data
- **Cross-Source Integration**: Practice combining data from different systems
- **Advanced Features**: JSON processing, multi-currency, real-time analytics
- **Performance Practice**: Large enough datasets for optimization techniques

The simplified approach ensures you can focus on mastering PySpark fundamentals while working with realistic, interconnected business data.
