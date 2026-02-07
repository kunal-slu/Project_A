-- Sample Athena Queries for AWS Production Pipeline
-- These queries demonstrate how to use the registered Delta tables in Athena

-- ==============================================
-- BASIC DATA EXPLORATION QUERIES
-- ==============================================

-- 1. Count records in each Silver table
SELECT 'fx_rates' as table_name, COUNT(*) as record_count FROM silver.fx_rates
UNION ALL
SELECT 'orders' as table_name, COUNT(*) as record_count FROM silver.orders
UNION ALL
SELECT 'salesforce_accounts' as table_name, COUNT(*) as record_count FROM silver.salesforce_accounts
UNION ALL
SELECT 'salesforce_leads' as table_name, COUNT(*) as record_count FROM silver.salesforce_leads;

-- 2. Count records in each Gold table
SELECT 'dim_customers' as table_name, COUNT(*) as record_count FROM gold.dim_customers
UNION ALL
SELECT 'dim_fx' as table_name, COUNT(*) as record_count FROM gold.dim_fx
UNION ALL
SELECT 'fact_orders' as table_name, COUNT(*) as record_count FROM gold.fact_orders;

-- 3. Check data freshness (latest records)
SELECT 
    'fx_rates' as table_name,
    MAX(as_of_date) as latest_date
FROM silver.fx_rates
UNION ALL
SELECT 
    'orders' as table_name,
    MAX(order_date) as latest_date
FROM silver.orders
UNION ALL
SELECT 
    'salesforce_accounts' as table_name,
    MAX(created_at) as latest_date
FROM silver.salesforce_accounts;

-- ==============================================
-- BUSINESS INTELLIGENCE QUERIES
-- ==============================================

-- 4. Orders by Status (Silver layer)
SELECT 
    status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM silver.orders
GROUP BY status
ORDER BY order_count DESC;

-- 5. Revenue by Currency (Silver layer)
SELECT 
    currency,
    COUNT(*) as order_count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM silver.orders
GROUP BY currency
ORDER BY total_amount DESC;

-- 6. Customer Analysis (Silver layer)
SELECT 
    type,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM silver.salesforce_accounts
GROUP BY type
ORDER BY customer_count DESC;

-- 7. Lead Status Analysis (Silver layer)
SELECT 
    status,
    COUNT(*) as lead_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM silver.salesforce_leads
GROUP BY status
ORDER BY lead_count DESC;

-- ==============================================
-- GOLD LAYER ANALYTICS QUERIES
-- ==============================================

-- 8. Revenue Analysis with USD Normalization (Gold layer)
SELECT 
    f.order_id,
    c.name AS customer_name,
    f.order_date,
    f.currency,
    f.amount_native,
    ROUND(f.amount_usd, 2) AS amount_usd,
    f.status
FROM gold.fact_orders f
JOIN gold.dim_customers c ON f.customer_id = c.id
ORDER BY f.order_date DESC
LIMIT 50;

-- 9. Total Revenue by Customer (Gold layer)
SELECT 
    c.name AS customer_name,
    c.type AS customer_type,
    c.region,
    COUNT(f.order_id) as total_orders,
    ROUND(SUM(f.amount_usd), 2) as total_revenue_usd,
    ROUND(AVG(f.amount_usd), 2) as avg_order_value_usd
FROM gold.fact_orders f
JOIN gold.dim_customers c ON f.customer_id = c.id
WHERE f.amount_usd IS NOT NULL
GROUP BY c.name, c.type, c.region
ORDER BY total_revenue_usd DESC;

-- 10. Revenue by Region (Gold layer)
SELECT 
    c.region,
    COUNT(f.order_id) as total_orders,
    ROUND(SUM(f.amount_usd), 2) as total_revenue_usd,
    ROUND(AVG(f.amount_usd), 2) as avg_order_value_usd
FROM gold.fact_orders f
JOIN gold.dim_customers c ON f.customer_id = c.id
WHERE f.amount_usd IS NOT NULL
GROUP BY c.region
ORDER BY total_revenue_usd DESC;

-- 11. Currency Exchange Analysis (Gold layer)
SELECT 
    fx.ccy,
    COUNT(DISTINCT fx.as_of_date) as days_available,
    ROUND(AVG(fx.rate_to_usd), 4) as avg_rate_to_usd,
    ROUND(MIN(fx.rate_to_usd), 4) as min_rate_to_usd,
    ROUND(MAX(fx.rate_to_usd), 4) as max_rate_to_usd
FROM gold.dim_fx fx
GROUP BY fx.ccy
ORDER BY avg_rate_to_usd DESC;

-- 12. Monthly Revenue Trend (Gold layer)
SELECT 
    DATE_TRUNC('month', f.order_date) as month,
    COUNT(f.order_id) as total_orders,
    ROUND(SUM(f.amount_usd), 2) as total_revenue_usd
FROM gold.fact_orders f
WHERE f.amount_usd IS NOT NULL
GROUP BY DATE_TRUNC('month', f.order_date)
ORDER BY month DESC;

-- ==============================================
-- DATA QUALITY VALIDATION QUERIES
-- ==============================================

-- 13. Check for NULL values in critical fields
SELECT 
    'fx_rates' as table_name,
    'ccy' as column_name,
    COUNT(*) as total_records,
    COUNT(ccy) as non_null_records,
    COUNT(*) - COUNT(ccy) as null_records
FROM silver.fx_rates
UNION ALL
SELECT 
    'orders' as table_name,
    'order_id' as column_name,
    COUNT(*) as total_records,
    COUNT(order_id) as non_null_records,
    COUNT(*) - COUNT(order_id) as null_records
FROM silver.orders
UNION ALL
SELECT 
    'orders' as table_name,
    'amount' as column_name,
    COUNT(*) as total_records,
    COUNT(amount) as non_null_records,
    COUNT(*) - COUNT(amount) as null_records
FROM silver.orders;

-- 14. Check for duplicate order IDs
SELECT 
    order_id,
    COUNT(*) as duplicate_count
FROM silver.orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 15. Check for invalid order amounts (negative values)
SELECT 
    COUNT(*) as total_orders,
    COUNT(CASE WHEN amount < 0 THEN 1 END) as negative_amount_orders,
    COUNT(CASE WHEN amount = 0 THEN 1 END) as zero_amount_orders
FROM silver.orders;

-- 16. Check for invalid order statuses
SELECT 
    status,
    COUNT(*) as count
FROM silver.orders
WHERE status NOT IN ('PLACED', 'PAID', 'SHIPPED', 'CANCELLED')
GROUP BY status;

-- ==============================================
-- PERFORMANCE MONITORING QUERIES
-- ==============================================

-- 17. Table sizes and record counts
SELECT 
    'silver.fx_rates' as table_name,
    COUNT(*) as record_count,
    MIN(as_of_date) as earliest_date,
    MAX(as_of_date) as latest_date
FROM silver.fx_rates
UNION ALL
SELECT 
    'silver.orders' as table_name,
    COUNT(*) as record_count,
    MIN(order_date) as earliest_date,
    MAX(order_date) as latest_date
FROM silver.orders
UNION ALL
SELECT 
    'gold.fact_orders' as table_name,
    COUNT(*) as record_count,
    MIN(order_date) as earliest_date,
    MAX(order_date) as latest_date
FROM gold.fact_orders;

-- 18. Data freshness check (last 7 days)
SELECT 
    'fx_rates' as table_name,
    COUNT(*) as records_last_7_days
FROM silver.fx_rates
WHERE as_of_date >= CURRENT_DATE - INTERVAL '7' DAY
UNION ALL
SELECT 
    'orders' as table_name,
    COUNT(*) as records_last_7_days
FROM silver.orders
WHERE order_date >= CURRENT_DATE - INTERVAL '7' DAY;

-- ==============================================
-- BUSINESS KPI QUERIES
-- ==============================================

-- 19. Top 10 Customers by Revenue
SELECT 
    c.name AS customer_name,
    c.type AS customer_type,
    ROUND(SUM(f.amount_usd), 2) as total_revenue_usd,
    COUNT(f.order_id) as total_orders
FROM gold.fact_orders f
JOIN gold.dim_customers c ON f.customer_id = c.id
WHERE f.amount_usd IS NOT NULL
GROUP BY c.name, c.type
ORDER BY total_revenue_usd DESC
LIMIT 10;

-- 20. Order Status Distribution
SELECT 
    f.status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(SUM(f.amount_usd), 2) as total_revenue_usd
FROM gold.fact_orders f
WHERE f.amount_usd IS NOT NULL
GROUP BY f.status
ORDER BY order_count DESC;

-- 21. Currency Usage Analysis
SELECT 
    f.currency,
    COUNT(*) as order_count,
    ROUND(SUM(f.amount_native), 2) as total_native_amount,
    ROUND(SUM(f.amount_usd), 2) as total_usd_amount
FROM gold.fact_orders f
GROUP BY f.currency
ORDER BY order_count DESC;
