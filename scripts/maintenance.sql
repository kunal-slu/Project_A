-- Delta Lake Maintenance Script
-- Run this script daily for table optimization and cleanup

-- Optimize bronze layer tables
OPTIMIZE delta.`/data/bronze/contacts` ZORDER BY (created_at, contact_id);
OPTIMIZE delta.`/data/bronze/orders` ZORDER BY (order_date, customer_id);
OPTIMIZE delta.`/data/bronze/products` ZORDER BY (category, product_id);

-- Optimize silver layer tables
OPTIMIZE delta.`/data/silver/contacts` ZORDER BY (updated_at, contact_id);
OPTIMIZE delta.`/data/silver/orders` ZORDER BY (order_date, customer_id);
OPTIMIZE delta.`/data/silver/products` ZORDER BY (category, product_id);

-- Optimize gold layer tables
OPTIMIZE delta.`/data/gold/customer_analytics` ZORDER BY (event_date, customer_id);
OPTIMIZE delta.`/data/gold/monthly_revenue` ZORDER BY (month, region);

-- Vacuum old files (retain 7 days for bronze, 30 days for silver/gold)
VACUUM delta.`/data/bronze/contacts` RETAIN 168 HOURS;
VACUUM delta.`/data/bronze/orders` RETAIN 168 HOURS;
VACUUM delta.`/data/bronze/products` RETAIN 168 HOURS;

VACUUM delta.`/data/silver/contacts` RETAIN 720 HOURS;
VACUUM delta.`/data/silver/orders` RETAIN 720 HOURS;
VACUUM delta.`/data/silver/products` RETAIN 720 HOURS;

VACUUM delta.`/data/gold/customer_analytics` RETAIN 720 HOURS;
VACUUM delta.`/data/gold/monthly_revenue` RETAIN 720 HOURS;

-- Analyze table statistics
ANALYZE TABLE delta.`/data/bronze/contacts` COMPUTE STATISTICS;
ANALYZE TABLE delta.`/data/bronze/orders` COMPUTE STATISTICS;
ANALYZE TABLE delta.`/data/bronze/products` COMPUTE STATISTICS;

ANALYZE TABLE delta.`/data/silver/contacts` COMPUTE STATISTICS;
ANALYZE TABLE delta.`/data/silver/orders` COMPUTE STATISTICS;
ANALYZE TABLE delta.`/data/silver/products` COMPUTE STATISTICS;

ANALYZE TABLE delta.`/data/gold/customer_analytics` COMPUTE STATISTICS;
ANALYZE TABLE delta.`/data/gold/monthly_revenue` COMPUTE STATISTICS;
