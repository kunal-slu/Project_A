# Consumption Layer Documentation

## Overview

The Gold layer tables in this data platform are the **source of truth** for business analytics, reporting, and downstream consumption. This document explains how to query and use these tables.

## Gold Tables Available

### 1. `gold.fact_sales`
**Purpose**: Core sales fact table with revenue, quantity, and transaction details.

**Schema**:
- `order_id` (string): Unique order identifier
- `customer_id` (string): Customer identifier
- `product_id` (string): Product identifier
- `order_date` (date): Order date
- `order_year`, `order_month`, `order_quarter` (int): Time dimensions
- `quantity` (int): Order quantity
- `total_amount` (decimal): Total order amount
- `order_size_category` (string): Small/Medium/Large/Extra Large
- `payment_method` (string): Payment type
- `shipping_country`, `shipping_state` (string): Geography dimensions

**Query Example (Athena)**:
```sql
SELECT 
    order_year,
    order_quarter,
    order_size_category,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue
FROM gold.fact_sales
WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY 1, 2, 3
ORDER BY total_revenue DESC;
```

**Query Example (Spark SQL)**:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SalesAnalytics").getOrCreate()

revenue_by_category = spark.sql("""
    SELECT 
        order_size_category,
        order_year,
        SUM(total_amount) as total_revenue,
        COUNT(*) as order_count,
        AVG(total_amount) as avg_order_value
    FROM gold.fact_sales
    WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY order_size_category, order_year
    ORDER BY total_revenue DESC
""")
revenue_by_category.show()
```

### 2. `gold.dim_customer`
**Purpose**: Customer dimension table with SCD2 support for historical tracking.

**Schema**:
- `customer_id` (string): Business key
- `first_name`, `last_name`, `email` (string): Customer attributes
- `account_id` (string): Linked account
- `created_date`, `last_modified_date` (timestamp): SCD2 timestamps
- `valid_from`, `valid_to` (timestamp): SCD2 validity range

**Query Example (Current Customers)**:
```sql
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    account_id
FROM gold.dim_customer
WHERE valid_to IS NULL  -- Current record
ORDER BY last_modified_date DESC;
```

### 3. `gold.marketing_attribution`
**Purpose**: Marketing attribution and funnel analysis.

**Schema**:
- `customer_id` (string)
- `campaign_id` (string)
- `channel` (string): Marketing channel
- `conversion_date` (date)
- `attribution_value` (decimal)
- `funnel_stage` (string): Awareness/Consideration/Purchase

## Access Methods

### 1. **Amazon Athena** (BI/Analytics)
**Recommended for**: Ad-hoc queries, Tableau/Power BI connections

**Setup**:
1. Gold tables are registered in AWS Glue Catalog
2. Use Athena console or JDBC/ODBC connections
3. Query using standard SQL

**Connection String**:
```
jdbc:awsathena://athena.us-east-1.amazonaws.com:443/
```

### 2. **Spark SQL** (Data Engineering)
**Recommended for**: Programmatic access, data transformations

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ConsumeGoldTables") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read Gold table
sales_fact = spark.read.format("delta").load("s3://company-data-lake/gold/fact_sales")
```

### 3. **Snowflake External Tables** (Warehouse Integration)
**Recommended for**: Integration with existing Snowflake analytics

Gold tables can be published to Snowflake via the `publish_gold_to_warehouse.py` job, which creates external tables pointing to S3 Delta tables.

## Data Freshness

- **Update Frequency**: Daily (after Silver → Gold transformation completes)
- **SLA**: Gold tables updated within 6 hours of source data landing in Bronze
- **Data Retention**: 7 years (compliance requirement)

## Business Metrics Available

### Revenue Metrics
- **Total Revenue**: `SUM(total_amount)` from `fact_sales`
- **Revenue by Time Period**: Group by `order_year`, `order_month`, `order_quarter`
- **Revenue by Product**: Join `fact_sales` with `dim_product`
- **Revenue by Customer Segment**: Join with `dim_customer`

### Conversion Metrics
- **Marketing Attribution**: Available in `marketing_attribution` table
- **Funnel Conversion Rates**: Calculate from `funnel_stage` groupings

### Customer Metrics
- **Customer Lifetime Value**: Aggregate `total_amount` by `customer_id`
- **Active Customers**: Count from `dim_customer` where `valid_to IS NULL`
- **Customer Segmentation**: Use `dim_customer.customer_segment`

## Access Control

### Roles and Permissions

**Data Analyst Role**:
- ✅ Read access to all `gold.*` tables
- ❌ No access to `bronze.*` or raw data
- ❌ No write access

**Data Scientist Role**:
- ✅ Read access to `gold.*` and `silver.*` tables (with PII masking)
- ❌ No access to `bronze.*`
- ❌ No write access

**Data Engineer Role**:
- ✅ Full access to all layers (bronze/silver/gold)
- ✅ Write access to gold layer

Access is enforced via **AWS Lake Formation** and **IAM policies**. See `docs/runbooks/DATA_ACCESS_GOVERNANCE.md` for details.

## Performance Optimization

### Partitioning Strategy
- **Date Partitioning**: All fact tables partitioned by `order_date` / `event_date`
- **Z-ORDER**: Applied to high-cardinality join keys (`customer_id`, `product_id`)

### Query Optimization Tips
1. **Always filter by date range** when querying fact tables
2. **Use column pruning** - only select columns you need
3. **Leverage Glue partitions** for faster scans
4. **Cache small dimension tables** before joining with facts

### Example Optimized Query:
```sql
-- ✅ GOOD: Filtered by date, specific columns
SELECT customer_id, SUM(total_amount) as revenue
FROM gold.fact_sales
WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY customer_id;

-- ❌ BAD: Full table scan, all columns
SELECT * FROM gold.fact_sales;
```

## Dashboard Integration

### Power BI
1. Use Athena connector
2. Point to Glue database `gold`
3. Create relationships between fact and dimension tables

### Tableau
1. Use Athena connector
2. Data source: `gold` Glue database
3. Leverage calculated fields for business metrics

## Troubleshooting

### Issue: "Table not found"
- **Solution**: Ensure Glue tables are registered. Run `register_glue_tables.py` if needed.

### Issue: "Slow queries"
- **Solution**: Check if partitions are optimized. Run `delta_optimize_vacuum.py` maintenance job.

### Issue: "Stale data"
- **Solution**: Check DAG execution status. Gold tables update daily after Silver → Gold transformation.

## Support

For questions or issues accessing Gold tables:
- **Data Engineering Team**: data-eng@company.com
- **On-call**: #data-pipeline-alerts Slack channel
- **Documentation**: See `docs/runbooks/` for operational guides

---

**Last Updated**: 2024-01-15  
**Maintained By**: Data Engineering Team

