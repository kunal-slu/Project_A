# Data Model - Project_A

## Overview

This document defines the logical data model for Project_A, a production-grade data engineering platform demonstrating e-commerce/fintech analytics.

## Data Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                            │
│  (Raw, unprocessed data from source systems)                    │
├─────────────────────────────────────────────────────────────────┤
│  • snowflake_orders        • crm_accounts                       │
│  • snowflake_customers     • crm_contacts                       │
│  • snowflake_products      • crm_opportunities                  │
│  • redshift_behavior       • fx_rates                           │
│  • kafka_events            • financial_metrics                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                            │
│  (Cleaned, validated, normalized data)                          │
├─────────────────────────────────────────────────────────────────┤
│  • customers_silver        • fx_rates_silver                     │
│  • orders_silver           • customer_behavior_silver            │
│  • products_silver         • order_events_silver                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER                             │
│  (Star schema: Facts + Dimensions for analytics)                │
├─────────────────────────────────────────────────────────────────┤
│  DIMENSIONS:                    FACTS:                         │
│  • dim_customer                 • fact_orders                   │
│  • dim_product                  • fact_opportunities (future)   │
│  • dim_account                   • fact_events (future)          │
│  • dim_contact                   • fact_behavior (future)        │
│  • dim_date                                                      │
│                                                                  │
│  AGGREGATES:                                                    │
│  • customer_360 (360° customer view)                            │
│  • product_performance (product analytics)                       │
└─────────────────────────────────────────────────────────────────┘
```

## Fact Tables

### fact_orders

**Purpose**: Central fact table for order transactions.

**Primary Key**: `order_id` (surrogate key)

**Foreign Keys**:
- `customer_id` → `dim_customer.customer_id`
- `product_id` → `dim_product.product_id`
- `order_date` → `dim_date.date_key`

**Grain**: One row per order line item

**Key Measures**:
- `sales_amount` (decimal) - Order amount in USD
- `quantity` (integer) - Quantity ordered
- `discount_amount` (decimal) - Discount applied
- `tax_amount` (decimal) - Tax amount
- `shipping_cost` (decimal) - Shipping cost

**Partitioning**: `order_date` (daily partitions)

**Z-Order**: `(order_date, customer_id)`

**Source**: `snowflake_orders` (Bronze) → `orders_silver` (Silver) → `fact_orders` (Gold)

---

### fact_opportunities (Future)

**Purpose**: Sales pipeline and opportunity tracking.

**Primary Key**: `opportunity_id` (surrogate key)

**Foreign Keys**:
- `account_id` → `dim_account.account_id`
- `contact_id` → `dim_contact.contact_id`
- `opportunity_date` → `dim_date.date_key`

**Grain**: One row per opportunity

**Key Measures**:
- `amount` (decimal) - Opportunity value
- `probability` (decimal 0-100) - Win probability
- `stage_duration_days` (integer) - Days in current stage

**Partitioning**: `opportunity_date`

---

## Dimension Tables

### dim_customer

**Purpose**: Customer master data with SCD2 support.

**Primary Key**: `customer_id` (natural key from source)

**Type**: Type 2 Slowly Changing Dimension (SCD2)

**Key Attributes**:
- `customer_id` (string) - Natural key
- `customer_name` (string)
- `email` (string)
- `phone` (string)
- `segment` (string) - Customer segment (Enterprise, SMB, Consumer)
- `country` (string)
- `industry` (string)
- `customer_since` (date) - First order date
- `is_active` (boolean)
- `effective_date` (date) - SCD2 effective start
- `expiry_date` (date) - SCD2 effective end (NULL = current)
- `is_current` (boolean) - Current version flag

**Source**: `snowflake_customers` + `crm_accounts` + `crm_contacts` (Bronze) → `customers_silver` (Silver) → `dim_customer` (Gold)

**Partitioning**: `country` (for broadcast optimization)

---

### dim_product

**Purpose**: Product master data.

**Primary Key**: `product_id` (natural key from source)

**Type**: Type 1 SCD (overwrite on change)

**Key Attributes**:
- `product_id` (string) - Natural key
- `product_name` (string)
- `category` (string) - Product category
- `subcategory` (string)
- `price_usd` (decimal) - Current price in USD
- `cost_usd` (decimal) - Product cost
- `currency` (string) - Default currency
- `is_active` (boolean)
- `launch_date` (date)

**Source**: `snowflake_products` (Bronze) → `products_silver` (Silver) → `dim_product` (Gold)

**Broadcast**: Yes (small table, < 10k rows)

---

### dim_account

**Purpose**: CRM account/company master data.

**Primary Key**: `account_id` (surrogate key from CRM `Id`)

**Key Attributes**:
- `account_id` (string) - Natural key (from CRM Id)
- `account_name` (string)
- `industry` (string)
- `annual_revenue` (decimal)
- `employee_count` (integer)
- `country` (string)
- `account_type` (string) - Customer, Partner, Prospect
- `created_date` (date)
- `last_modified_date` (date)

**Source**: `crm_accounts` (Bronze) → `accounts_silver` (Silver) → `dim_account` (Gold)

---

### dim_contact

**Purpose**: CRM contact/person master data.

**Primary Key**: `contact_id` (surrogate key from CRM `Id`)

**Foreign Keys**:
- `account_id` → `dim_account.account_id`

**Key Attributes**:
- `contact_id` (string) - Natural key (from CRM Id)
- `contact_name` (string)
- `email` (string)
- `phone` (string)
- `title` (string) - Job title
- `account_id` (string) - FK to dim_account
- `created_date` (date)
- `last_modified_date` (date)

**Source**: `crm_contacts` (Bronze) → `contacts_silver` (Silver) → `dim_contact` (Gold)

---

### dim_date

**Purpose**: Date dimension for time-based analytics.

**Primary Key**: `date_key` (integer, YYYYMMDD format)

**Key Attributes**:
- `date_key` (integer) - YYYYMMDD
- `date` (date) - Actual date
- `year` (integer)
- `quarter` (integer) - 1-4
- `month` (integer) - 1-12
- `month_name` (string)
- `week` (integer) - Week of year
- `day_of_week` (integer) - 1=Monday, 7=Sunday
- `day_name` (string)
- `is_weekend` (boolean)
- `is_holiday` (boolean)
- `fiscal_year` (integer)
- `fiscal_quarter` (integer)

**Source**: Generated (not from source data)

**Size**: ~10 years = ~3,650 rows (small, broadcastable)

---

## Supporting Tables

### customer_behavior_silver

**Purpose**: Aggregated customer behavioral metrics from Redshift.

**Primary Key**: `(customer_id, event_date)` (composite)

**Foreign Keys**:
- `customer_id` → `dim_customer.customer_id`

**Key Metrics**:
- `login_count` (integer) - Daily logins
- `page_views` (integer) - Daily page views
- `purchases` (integer) - Daily purchases
- `revenue` (decimal) - Daily revenue
- `session_duration_minutes` (decimal) - Average session duration

**Partitioning**: `event_date` (daily)

**Source**: `redshift_customer_behavior` (Bronze) → `customer_behavior_silver` (Silver)

---

### fx_rates_silver

**Purpose**: Foreign exchange rates for currency conversion.

**Primary Key**: `(base_currency, target_currency, rate_date)` (composite)

**Key Attributes**:
- `base_currency` (string) - Base currency (e.g., USD)
- `target_currency` (string) - Target currency (e.g., EUR)
- `rate_date` (date) - Rate date
- `exchange_rate` (decimal) - Mid rate
- `bid_rate` (decimal) - Bid rate
- `ask_rate` (decimal) - Ask rate
- `source` (string) - Data source

**Partitioning**: `rate_date` (daily)

**Source**: `fx_rates_historical.json` (Bronze) → `fx_rates_silver` (Silver)

---

### order_events_silver

**Purpose**: Processed Kafka streaming events related to orders.

**Primary Key**: `event_id` (from Kafka)

**Foreign Keys**:
- `order_id` → `fact_orders.order_id` (optional, may be null for pre-order events)
- `customer_id` → `dim_customer.customer_id`

**Key Attributes**:
- `event_id` (string) - Unique event ID
- `event_type` (string) - OrderPlaced, OrderShipped, OrderRefunded, etc.
- `event_timestamp` (timestamp) - Event time
- `order_id` (string) - Related order (nullable)
- `customer_id` (string) - Related customer
- `amount` (decimal) - Event amount
- `channel` (string) - Event channel (web, mobile, api)

**Partitioning**: `event_date` (derived from `event_timestamp`)

**Source**: `kafka_events` (Bronze) → `order_events_silver` (Silver)

---

### financial_metrics_24_months

**Purpose**: Monthly aggregated financial KPIs.

**Primary Key**: `month` (YYYY-MM format)

**Key Metrics**:
- `total_revenue_usd` (decimal)
- `total_orders` (integer)
- `active_customers` (integer)
- `churn_rate` (decimal 0-100)
- `marketing_spend_usd` (decimal)
- `net_margin_pct` (decimal)

**Source**: Generated/aggregated from fact_orders and other sources

---

## Data Quality Requirements

### Primary Key Constraints

All fact and dimension tables must have:
- Non-null primary keys
- Unique primary keys (enforced at write time)

### Foreign Key Integrity

- `fact_orders.customer_id` must exist in `dim_customer.customer_id`
- `fact_orders.product_id` must exist in `dim_product.product_id`
- `fact_orders.order_date` must exist in `dim_date.date_key`
- `customer_behavior_silver.customer_id` must exist in `dim_customer.customer_id`
- `order_events_silver.customer_id` must exist in `dim_customer.customer_id` (when not null)

**Orphan Tolerance**: < 0.1% (intentional test cases only)

### Data Quality Rules

1. **Null Handling**:
   - Primary keys: Never null
   - Foreign keys: Nullable only when relationship is optional
   - Measures: Nullable with business justification

2. **Value Ranges**:
   - Amounts: >= 0 (negative only for refunds, explicitly flagged)
   - Probabilities: 0-100
   - Dates: Within reasonable business range (e.g., 2020-2030)

3. **Referential Integrity**:
   - All FK relationships validated before Gold writes
   - Orphan records quarantined to error lane

---

## Join Patterns

### Common Joins

1. **Order Analytics**:
   ```sql
   fact_orders f
   JOIN dim_customer c ON f.customer_id = c.customer_id
   JOIN dim_product p ON f.product_id = p.product_id
   JOIN dim_date d ON f.order_date = d.date_key
   ```

2. **Customer 360 View**:
   ```sql
   dim_customer c
   LEFT JOIN customer_behavior_silver b ON c.customer_id = b.customer_id
   LEFT JOIN fact_orders o ON c.customer_id = o.customer_id
   ```

3. **Streaming Join** (Kafka events):
   ```sql
   order_events_silver e
   JOIN dim_customer c ON e.customer_id = c.customer_id
   WHERE e.event_timestamp >= watermark
   ```

---

## Partitioning Strategy

- **Bronze**: Raw snapshots, partitioned by source system
- **Silver**: Partitioned by date (where applicable)
  - `orders_silver`: `order_date`
  - `customer_behavior_silver`: `event_date`
  - `fx_rates_silver`: `rate_date`
  - `order_events_silver`: `event_date`
- **Gold**:
  - `fact_orders`: `order_date` (daily)
  - Dimensions: By high-cardinality attribute (e.g., `dim_customer` by `country`)

---

## Z-Order Optimization

- `fact_orders`: `(order_date, customer_id)` - Optimizes date range + customer filters
- `customer_behavior_silver`: `(event_date, customer_id)` - Optimizes time-series queries

---

## Incremental Load Support

All fact and time-series tables support incremental loads via:
- `order_date >= last_watermark` (fact_orders)
- `event_timestamp >= last_watermark` (order_events_silver)
- `rate_date >= last_watermark` (fx_rates_silver)

Watermarks stored in metadata table or checkpoint location.

---

## Schema Evolution

- **Bronze**: Schema-on-read, accept all fields
- **Silver**: Schema contracts enforced, new columns added via ALTER
- **Gold**: Schema versioning, backward-compatible changes only

---

## Data Volume Targets

- **Fact Tables**: 100k - 1M rows (realistic for demo)
- **Dimensions**: 10k - 50k rows (broadcastable)
- **Time Series**: 50k - 500k rows (partitioned by date)

---

## Next Steps

1. ✅ Data model defined
2. ⏳ Validate against current data profiles
3. ⏳ Generate/enhance data to match model
4. ⏳ Create schema contracts
5. ⏳ Implement validation logic
6. ⏳ Update ETL jobs to enforce model

