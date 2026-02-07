# Snowflake Data Schema Documentation

## Overview
This document defines the schema contracts for Snowflake data warehouse integration in our enterprise ETL pipeline.

## Bronze Layer Schemas

### Snowflake Orders (`snowflake_orders_bronze`)

**Source**: Snowflake Data Warehouse  
**Target**: `s3://company-data-lake-ACCOUNT_ID/bronze/snowflake/orders/`  
**Format**: Delta Lake (Parquet)

#### Required Fields
| Field Name | Data Type | Constraints | Description |
|------------|-----------|-------------|-------------|
| `order_id` | STRING | NOT NULL, PRIMARY KEY | Unique order identifier |
| `customer_id` | STRING | NOT NULL, FOREIGN KEY | Reference to customer |
| `order_date` | TIMESTAMP | NOT NULL | Order placement date |
| `currency` | STRING | NOT NULL | Order currency (USD, EUR, GBP) |
| `total_amount` | DOUBLE | NOT NULL, > 0 | Total order amount |
| `status` | STRING | NOT NULL | Order status |
| `product_count` | INTEGER | NOT NULL, >= 0 | Number of products in order |
| `shipping_address` | STRING | NULLABLE | Shipping address |
| `billing_address` | STRING | NULLABLE | Billing address |
| `payment_method` | STRING | NULLABLE | Payment method used |
| `created_at` | TIMESTAMP | NOT NULL | Record creation timestamp |
| `updated_at` | TIMESTAMP | NOT NULL | Record update timestamp |

#### Metadata Fields
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `_source_system` | STRING | Source system identifier |
| `_ingestion_ts` | TIMESTAMP | Data ingestion timestamp |
| `_job_id` | STRING | ETL job identifier |

#### Data Quality Rules
1. **Primary Key Constraint**: `order_id` must be unique and non-null
2. **Foreign Key Constraint**: `customer_id` must reference valid customer
3. **Business Rule**: `total_amount` must be > 0
4. **Business Rule**: `product_count` must be >= 0
5. **Currency Validation**: `currency` must be valid ISO currency code
6. **Freshness Check**: `updated_at` must be within last 24 hours

### Snowflake Customers (`snowflake_customers_bronze`)

**Source**: Snowflake Data Warehouse  
**Target**: `s3://company-data-lake-ACCOUNT_ID/bronze/snowflake/customers/`  
**Format**: Delta Lake (Parquet)

#### Required Fields
| Field Name | Data Type | Constraints | Description |
|------------|-----------|-------------|-------------|
| `customer_id` | STRING | NOT NULL, PRIMARY KEY | Unique customer identifier |
| `email` | STRING | NOT NULL | Customer email address |
| `first_name` | STRING | NULLABLE | Customer first name |
| `last_name` | STRING | NULLABLE | Customer last name |
| `registration_date` | TIMESTAMP | NOT NULL | Customer registration date |
| `country` | STRING | NULLABLE | Customer country |
| `city` | STRING | NULLABLE | Customer city |
| `customer_tier` | STRING | NULLABLE | Customer tier (Bronze/Silver/Gold) |
| `total_orders` | INTEGER | NOT NULL, >= 0 | Total number of orders |
| `total_spent` | DOUBLE | NOT NULL, >= 0 | Total amount spent |
| `last_order_date` | TIMESTAMP | NULLABLE | Date of last order |
| `is_active` | BOOLEAN | NOT NULL | Whether customer is active |
| `created_at` | TIMESTAMP | NOT NULL | Record creation timestamp |
| `updated_at` | TIMESTAMP | NOT NULL | Record update timestamp |

#### Metadata Fields
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `_source_system` | STRING | Source system identifier |
| `_ingestion_ts` | TIMESTAMP | Data ingestion timestamp |
| `_job_id` | STRING | ETL job identifier |

#### Data Quality Rules
1. **Primary Key Constraint**: `customer_id` must be unique and non-null
2. **Email Validation**: `email` must be valid email format
3. **Business Rule**: `total_orders` must be >= 0
4. **Business Rule**: `total_spent` must be >= 0
5. **Freshness Check**: `updated_at` must be within last 24 hours

### Snowflake Products (`snowflake_products_bronze`)

**Source**: Snowflake Data Warehouse  
**Target**: `s3://company-data-lake-ACCOUNT_ID/bronze/snowflake/products/`  
**Format**: Delta Lake (Parquet)

#### Required Fields
| Field Name | Data Type | Constraints | Description |
|------------|-----------|-------------|-------------|
| `product_id` | STRING | NOT NULL, PRIMARY KEY | Unique product identifier |
| `product_name` | STRING | NOT NULL | Product name |
| `category` | STRING | NOT NULL | Product category |
| `subcategory` | STRING | NULLABLE | Product subcategory |
| `price` | DOUBLE | NOT NULL, > 0 | Product price |
| `cost` | DOUBLE | NOT NULL, >= 0 | Product cost |
| `sku` | STRING | NOT NULL | Stock keeping unit |
| `brand` | STRING | NULLABLE | Product brand |
| `description` | STRING | NULLABLE | Product description |
| `is_active` | BOOLEAN | NOT NULL | Whether product is active |
| `created_at` | TIMESTAMP | NOT NULL | Record creation timestamp |
| `updated_at` | TIMESTAMP | NOT NULL | Record update timestamp |

#### Metadata Fields
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `_source_system` | STRING | Source system identifier |
| `_ingestion_ts` | TIMESTAMP | Data ingestion timestamp |
| `_job_id` | STRING | ETL job identifier |

#### Data Quality Rules
1. **Primary Key Constraint**: `product_id` must be unique and non-null
2. **Business Rule**: `price` must be > 0
3. **Business Rule**: `cost` must be >= 0
4. **Business Rule**: `price` must be >= `cost` (margin check)
5. **Freshness Check**: `updated_at` must be within last 24 hours

## Data Quality Monitoring

### Bronze Layer DQ Checks
- **Freshness**: Data must be ingested within SLA (hourly for orders, daily for customers/products)
- **Volume**: Record count must be within expected range (±5%)
- **Completeness**: Required fields must be non-null
- **Referential Integrity**: Foreign keys must reference valid records
- **Business Rules**: Amounts must be positive, counts must be non-negative

### Compliance Notes

#### PII Handling
- **Email addresses**: Masked in non-production environments
- **Names**: Available for business use, logged for audit
- **Addresses**: Available for shipping/billing operations

#### Data Retention
- **Bronze**: 3 years (business requirement)
- **Silver**: 2 years (analytics requirement)
- **Gold**: 1 year (reporting requirement)

#### Access Control
- **Bronze**: Data Engineers only
- **Silver**: Data Engineers + Data Analysts
- **Gold**: All authorized users

---

*This document is maintained by the Data Engineering team and reviewed quarterly for accuracy and compliance.*
