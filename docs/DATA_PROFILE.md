# Data Profile Report

Generated: 2025-11-19 09:08:37

## CRM

### accounts.csv

- **File**: `aws/data/samples/crm/accounts.csv`
- **Format**: CSV
- **Rows**: 20,000
- **Columns**: 29

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| Id | object | 0.0% | 20000 | PK candidate;  |
| Name | object | 0.0% | 16233 |  |
| Phone | object | 0.0% | 20000 | PK candidate;  |
| Website | object | 0.0% | 13221 |  |
| Industry | object | 0.0% | 10 |  |
| AnnualRevenue | int64 | 0.0% | 19999 |  |
| NumberOfEmployees | int64 | 0.0% | 16479 |  |
| BillingStreet | object | 0.0% | 20000 | PK candidate;  |
| BillingCity | object | 0.0% | 13260 |  |
| BillingState | object | 0.0% | 50 |  |
| BillingPostalCode | int64 | 0.0% | 18138 |  |
| BillingCountry | object | 0.0% | 243 |  |
| Rating | object | 0.0% | 3 |  |
| Type | object | 0.0% | 4 |  |
| AccountSource | object | 0.0% | 6 |  |
| AccountNumber | object | 0.0% | 20000 | PK candidate;  |
| Site | object | 60.0% | 4749 | High nulls;  |
| Description | object | 29.3% | 14143 |  |
| Ownership | object | 0.0% | 4 |  |
| ParentId | object | 70.4% | 100 | High nulls;  |

#### Quality Notes

- ⚠️ 3 columns with >50% nulls
- ℹ️ 4 potential primary keys

---

### contacts.csv

- **File**: `aws/data/samples/crm/contacts.csv`
- **Format**: CSV
- **Rows**: 60,000
- **Columns**: 24

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| Id | object | 0.0% | 60000 | PK candidate;  |
| LastName | object | 0.0% | 1000 |  |
| AccountId | object | 0.0% | 15843 |  |
| FirstName | object | 0.0% | 690 |  |
| Email | object | 0.0% | 54155 |  |
| Phone | object | 0.0% | 60000 | PK candidate;  |
| MobilePhone | object | 0.0% | 60000 | PK candidate;  |
| Title | object | 0.0% | 639 |  |
| Department | object | 0.0% | 8 |  |
| LeadSource | object | 0.0% | 5 |  |
| MailingStreet | object | 0.0% | 60000 | PK candidate;  |
| MailingCity | object | 0.0% | 28059 |  |
| MailingState | object | 0.0% | 50 |  |
| MailingPostalCode | int64 | 0.0% | 45086 |  |
| MailingCountry | object | 0.0% | 243 |  |
| DoNotCall | bool | 0.0% | 2 |  |
| HasOptedOutOfEmail | bool | 0.0% | 2 |  |
| Description | object | 50.1% | 29959 | High nulls;  |
| CreatedDate | object | 0.0% | 1096 |  |
| LastModifiedDate | object | 0.0% | 366 |  |

#### Quality Notes

- ⚠️ 1 columns with >50% nulls
- ℹ️ 4 potential primary keys

---

### opportunities.csv

- **File**: `aws/data/samples/crm/opportunities.csv`
- **Format**: CSV
- **Rows**: 100,000
- **Columns**: 22

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| Id | object | 0.0% | 100000 | PK candidate;  |
| Name | object | 0.0% | 95344 |  |
| AccountId | object | 0.0% | 16197 |  |
| StageName | object | 0.0% | 10 |  |
| CloseDate | object | 0.0% | 396 |  |
| Amount | int64 | 0.0% | 99009 |  |
| Probability | int64 | 0.0% | 101 |  |
| LeadSource | object | 0.0% | 5 |  |
| Type | object | 0.0% | 5 |  |
| NextStep | object | 30.0% | 70012 |  |
| Description | object | 40.1% | 59911 |  |
| ForecastCategory | object | 0.0% | 5 |  |
| IsClosed | bool | 0.0% | 2 |  |
| IsWon | bool | 0.0% | 2 |  |
| CreatedDate | object | 0.0% | 731 |  |
| LastModifiedDate | object | 0.0% | 1 | Constant;  |
| OwnerId | object | 0.0% | 100 |  |
| DealSize | object | 0.0% | 4 |  |
| SalesCycle | int64 | 0.0% | 336 |  |
| ProductInterest | object | 20.1% | 5 |  |

---

### accounts.csv

- **File**: `data/samples/crm/accounts.csv`
- **Format**: CSV
- **Rows**: 20,000
- **Columns**: 29

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| Id | object | 0.0% | 20000 | PK candidate;  |
| Name | object | 0.0% | 16233 |  |
| Phone | object | 0.0% | 20000 | PK candidate;  |
| Website | object | 0.0% | 13221 |  |
| Industry | object | 0.0% | 10 |  |
| AnnualRevenue | int64 | 0.0% | 19999 |  |
| NumberOfEmployees | int64 | 0.0% | 16479 |  |
| BillingStreet | object | 0.0% | 20000 | PK candidate;  |
| BillingCity | object | 0.0% | 13260 |  |
| BillingState | object | 0.0% | 50 |  |
| BillingPostalCode | int64 | 0.0% | 18138 |  |
| BillingCountry | object | 0.0% | 243 |  |
| Rating | object | 0.0% | 3 |  |
| Type | object | 0.0% | 4 |  |
| AccountSource | object | 0.0% | 6 |  |
| AccountNumber | object | 0.0% | 20000 | PK candidate;  |
| Site | object | 60.0% | 4749 | High nulls;  |
| Description | object | 29.3% | 14143 |  |
| Ownership | object | 0.0% | 4 |  |
| ParentId | object | 70.4% | 100 | High nulls;  |

#### Quality Notes

- ⚠️ 3 columns with >50% nulls
- ℹ️ 4 potential primary keys

---

### contacts.csv

- **File**: `data/samples/crm/contacts.csv`
- **Format**: CSV
- **Rows**: 60,000
- **Columns**: 24

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| Id | object | 0.0% | 60000 | PK candidate;  |
| LastName | object | 0.0% | 1000 |  |
| AccountId | object | 0.0% | 15843 |  |
| FirstName | object | 0.0% | 690 |  |
| Email | object | 0.0% | 54155 |  |
| Phone | object | 0.0% | 60000 | PK candidate;  |
| MobilePhone | object | 0.0% | 60000 | PK candidate;  |
| Title | object | 0.0% | 639 |  |
| Department | object | 0.0% | 8 |  |
| LeadSource | object | 0.0% | 5 |  |
| MailingStreet | object | 0.0% | 60000 | PK candidate;  |
| MailingCity | object | 0.0% | 28059 |  |
| MailingState | object | 0.0% | 50 |  |
| MailingPostalCode | int64 | 0.0% | 45086 |  |
| MailingCountry | object | 0.0% | 243 |  |
| DoNotCall | bool | 0.0% | 2 |  |
| HasOptedOutOfEmail | bool | 0.0% | 2 |  |
| Description | object | 50.1% | 29959 | High nulls;  |
| CreatedDate | object | 0.0% | 1096 |  |
| LastModifiedDate | object | 0.0% | 366 |  |

#### Quality Notes

- ⚠️ 1 columns with >50% nulls
- ℹ️ 4 potential primary keys

---

### opportunities.csv

- **File**: `data/samples/crm/opportunities.csv`
- **Format**: CSV
- **Rows**: 100,000
- **Columns**: 22

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| Id | object | 0.0% | 100000 | PK candidate;  |
| Name | object | 0.0% | 95344 |  |
| AccountId | object | 0.0% | 16197 |  |
| StageName | object | 0.0% | 10 |  |
| CloseDate | object | 0.0% | 396 |  |
| Amount | int64 | 0.0% | 99009 |  |
| Probability | int64 | 0.0% | 101 |  |
| LeadSource | object | 0.0% | 5 |  |
| Type | object | 0.0% | 5 |  |
| NextStep | object | 30.0% | 70012 |  |
| Description | object | 40.1% | 59911 |  |
| ForecastCategory | object | 0.0% | 5 |  |
| IsClosed | bool | 0.0% | 2 |  |
| IsWon | bool | 0.0% | 2 |  |
| CreatedDate | object | 0.0% | 731 |  |
| LastModifiedDate | object | 0.0% | 1 | Constant;  |
| OwnerId | object | 0.0% | 100 |  |
| DealSize | object | 0.0% | 4 |  |
| SalesCycle | int64 | 0.0% | 336 |  |
| ProductInterest | object | 20.1% | 5 |  |

---

## Kafka

### stream_kafka_events_100000.csv

- **File**: `aws/data/samples/kafka/stream_kafka_events_100000.csv`
- **Format**: CSV
- **Rows**: 100,000
- **Columns**: 8

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| event_id | object | 0.0% | 100000 | PK candidate;  |
| topic | object | 0.0% | 5 |  |
| partition | int64 | 0.0% | 8 |  |
| offset | int64 | 0.0% | 95129 |  |
| timestamp | object | 0.0% | 100000 | PK candidate;  |
| key | object | 0.0% | 94577 |  |
| value | object | 0.0% | 100000 | PK candidate;  |
| headers | object | 0.0% | 1 | Constant;  |

#### Quality Notes

- ℹ️ 3 potential primary keys

---

### stream_kafka_events_100000.csv

- **File**: `data/samples/kafka/stream_kafka_events_100000.csv`
- **Format**: CSV
- **Rows**: 100,000
- **Columns**: 8

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| event_id | object | 0.0% | 100000 | PK candidate;  |
| topic | object | 0.0% | 5 |  |
| partition | int64 | 0.0% | 8 |  |
| offset | int64 | 0.0% | 95129 |  |
| timestamp | object | 0.0% | 100000 | PK candidate;  |
| key | object | 0.0% | 94577 |  |
| value | object | 0.0% | 100000 | PK candidate;  |
| headers | object | 0.0% | 1 | Constant;  |

#### Quality Notes

- ℹ️ 3 potential primary keys

---

## Redshift

### redshift_customer_behavior_50000.csv

- **File**: `aws/data/samples/redshift/redshift_customer_behavior_50000.csv`
- **Format**: CSV
- **Rows**: 50,000
- **Columns**: 24

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| behavior_id | object | 0.0% | 50000 | PK candidate;  |
| customer_id | object | 0.0% | 31643 |  |
| event_name | object | 0.0% | 8 |  |
| event_timestamp | object | 0.0% | 375 |  |
| session_id | object | 0.0% | 48602 |  |
| page_url | object | 0.0% | 27066 |  |
| referrer | object | 50.2% | 15675 | High nulls;  |
| device_type | object | 0.0% | 3 |  |
| browser | object | 0.0% | 5 |  |
| os | object | 0.0% | 5 |  |
| country | object | 0.6% | 194 |  |
| city | object | 0.0% | 25303 |  |
| user_agent | object | 0.0% | 39957 |  |
| ip_address | object | 0.0% | 50000 | PK candidate;  |
| duration_seconds | int64 | 0.0% | 3600 |  |
| conversion_value | float64 | 0.0% | 13936 |  |
| utm_source | object | 0.0% | 5 |  |
| utm_medium | object | 0.0% | 5 |  |
| utm_campaign | object | 0.0% | 4 |  |
| behavior_segment | object | 0.0% | 4 |  |

#### Quality Notes

- ⚠️ 1 columns with >50% nulls
- ℹ️ 3 potential primary keys

---

### redshift_customer_behavior_50000.csv

- **File**: `data/samples/redshift/redshift_customer_behavior_50000.csv`
- **Format**: CSV
- **Rows**: 50,000
- **Columns**: 24

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| behavior_id | object | 0.0% | 50000 | PK candidate;  |
| customer_id | object | 0.0% | 31643 |  |
| event_name | object | 0.0% | 8 |  |
| event_timestamp | object | 0.0% | 375 |  |
| session_id | object | 0.0% | 48602 |  |
| page_url | object | 0.0% | 27066 |  |
| referrer | object | 50.2% | 15675 | High nulls;  |
| device_type | object | 0.0% | 3 |  |
| browser | object | 0.0% | 5 |  |
| os | object | 0.0% | 5 |  |
| country | object | 0.6% | 194 |  |
| city | object | 0.0% | 25303 |  |
| user_agent | object | 0.0% | 39957 |  |
| ip_address | object | 0.0% | 50000 | PK candidate;  |
| duration_seconds | int64 | 0.0% | 3600 |  |
| conversion_value | float64 | 0.0% | 13936 |  |
| utm_source | object | 0.0% | 5 |  |
| utm_medium | object | 0.0% | 5 |  |
| utm_campaign | object | 0.0% | 4 |  |
| behavior_segment | object | 0.0% | 4 |  |

#### Quality Notes

- ⚠️ 1 columns with >50% nulls
- ℹ️ 3 potential primary keys

---

## Snowflake

### snowflake_customers_50000.csv

- **File**: `aws/data/samples/snowflake/snowflake_customers_50000.csv`
- **Format**: CSV
- **Rows**: 50,000
- **Columns**: 23

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| customer_id | object | 0.0% | 50000 | PK candidate;  |
| first_name | object | 0.0% | 690 |  |
| last_name | object | 0.0% | 1000 |  |
| email | object | 0.0% | 45791 |  |
| phone | object | 0.0% | 50000 | PK candidate;  |
| address | object | 0.0% | 50000 | PK candidate;  |
| city | object | 0.0% | 25225 |  |
| state | object | 0.0% | 50 |  |
| country | object | 0.0% | 243 |  |
| zip_code | int64 | 0.0% | 39306 |  |
| registration_date | object | 0.0% | 1827 |  |
| gender | object | 0.0% | 3 |  |
| age | int64 | 0.0% | 63 |  |
| customer_segment | object | 0.0% | 4 |  |
| lifetime_value | float64 | 0.0% | 49748 |  |
| last_purchase_date | object | 0.0% | 731 |  |
| total_orders | int64 | 0.0% | 101 |  |
| preferred_channel | object | 0.0% | 4 |  |
| created_at | object | 0.0% | 50000 | PK candidate;  |
| updated_at | object | 0.0% | 50000 | PK candidate;  |

#### Quality Notes

- ℹ️ 5 potential primary keys

---

### snowflake_orders_100000.csv

- **File**: `aws/data/samples/snowflake/snowflake_orders_100000.csv`
- **Format**: CSV
- **Rows**: 43,161
- **Columns**: 30

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| order_id | object | 0.0% | 43161 | PK candidate;  |
| customer_id | object | 0.0% | 43161 | PK candidate;  |
| product_id | object | 0.0% | 9875 |  |
| order_date | object | 0.0% | 43144 |  |
| order_timestamp | object | 0.0% | 43144 |  |
| quantity | int64 | 0.0% | 10 |  |
| unit_price | float64 | 0.0% | 28608 |  |
| total_amount | float64 | 0.0% | 38805 |  |
| currency | object | 0.0% | 8 |  |
| payment_method | object | 0.0% | 5 |  |
| shipping_method | object | 0.0% | 4 |  |
| status | object | 0.0% | 6 |  |
| shipping_address | object | 0.0% | 43161 | PK candidate;  |
| billing_address | object | 0.0% | 43160 |  |
| discount_percent | float64 | 0.0% | 3001 |  |
| tax_amount | float64 | 0.0% | 9872 |  |
| shipping_cost | float64 | 0.0% | 42504 |  |
| promo_code | object | 50.3% | 8135 | High nulls;  |
| sales_rep_id | object | 0.0% | 50 |  |
| channel | object | 0.0% | 5 |  |

#### Quality Notes

- ⚠️ 1 columns with >50% nulls
- ℹ️ 4 potential primary keys

---

### snowflake_products_10000.csv

- **File**: `aws/data/samples/snowflake/snowflake_products_10000.csv`
- **Format**: CSV
- **Rows**: 10,000
- **Columns**: 23

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| product_id | object | 0.0% | 10000 | PK candidate;  |
| sku | object | 0.0% | 9947 |  |
| product_name | object | 0.0% | 9948 |  |
| category | object | 0.0% | 6 |  |
| subcategory | object | 0.0% | 23 |  |
| brand | object | 0.0% | 8670 |  |
| price_usd | float64 | 0.0% | 9535 |  |
| cost_usd | float64 | 0.0% | 9112 |  |
| weight_kg | float64 | 0.0% | 4319 |  |
| dimensions | object | 0.0% | 9902 |  |
| color | object | 0.0% | 8 |  |
| size | object | 0.0% | 7 |  |
| active | bool | 0.0% | 2 |  |
| stock_quantity | int64 | 0.0% | 1001 |  |
| reorder_level | int64 | 0.0% | 91 |  |
| supplier_id | object | 0.0% | 100 |  |
| created_at | object | 0.0% | 10000 | PK candidate;  |
| updated_at | object | 0.0% | 10000 | PK candidate;  |
| source_system | object | 0.0% | 1 | Constant;  |
| ingestion_timestamp | object | 0.0% | 1 | Constant;  |

#### Quality Notes

- ℹ️ 3 potential primary keys

---

### snowflake_customers_50000.csv

- **File**: `data/samples/snowflake/snowflake_customers_50000.csv`
- **Format**: CSV
- **Rows**: 50,000
- **Columns**: 23

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| customer_id | object | 0.0% | 50000 | PK candidate;  |
| first_name | object | 0.0% | 690 |  |
| last_name | object | 0.0% | 1000 |  |
| email | object | 0.0% | 45791 |  |
| phone | object | 0.0% | 50000 | PK candidate;  |
| address | object | 0.0% | 50000 | PK candidate;  |
| city | object | 0.0% | 25225 |  |
| state | object | 0.0% | 50 |  |
| country | object | 0.0% | 243 |  |
| zip_code | int64 | 0.0% | 39306 |  |
| registration_date | object | 0.0% | 1827 |  |
| gender | object | 0.0% | 3 |  |
| age | int64 | 0.0% | 63 |  |
| customer_segment | object | 0.0% | 4 |  |
| lifetime_value | float64 | 0.0% | 49748 |  |
| last_purchase_date | object | 0.0% | 731 |  |
| total_orders | int64 | 0.0% | 101 |  |
| preferred_channel | object | 0.0% | 4 |  |
| created_at | object | 0.0% | 50000 | PK candidate;  |
| updated_at | object | 0.0% | 50000 | PK candidate;  |

#### Quality Notes

- ℹ️ 5 potential primary keys

---

### snowflake_orders_100000.csv

- **File**: `data/samples/snowflake/snowflake_orders_100000.csv`
- **Format**: CSV
- **Rows**: 43,161
- **Columns**: 30

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| order_id | object | 0.0% | 43161 | PK candidate;  |
| customer_id | object | 0.0% | 43161 | PK candidate;  |
| product_id | object | 0.0% | 9875 |  |
| order_date | object | 0.0% | 43144 |  |
| order_timestamp | object | 0.0% | 43144 |  |
| quantity | int64 | 0.0% | 10 |  |
| unit_price | float64 | 0.0% | 28608 |  |
| total_amount | float64 | 0.0% | 38805 |  |
| currency | object | 0.0% | 8 |  |
| payment_method | object | 0.0% | 5 |  |
| shipping_method | object | 0.0% | 4 |  |
| status | object | 0.0% | 6 |  |
| shipping_address | object | 0.0% | 43161 | PK candidate;  |
| billing_address | object | 0.0% | 43160 |  |
| discount_percent | float64 | 0.0% | 3001 |  |
| tax_amount | float64 | 0.0% | 9872 |  |
| shipping_cost | float64 | 0.0% | 42504 |  |
| promo_code | object | 50.3% | 8135 | High nulls;  |
| sales_rep_id | object | 0.0% | 50 |  |
| channel | object | 0.0% | 5 |  |

#### Quality Notes

- ⚠️ 1 columns with >50% nulls
- ℹ️ 4 potential primary keys

---

### snowflake_products_10000.csv

- **File**: `data/samples/snowflake/snowflake_products_10000.csv`
- **Format**: CSV
- **Rows**: 10,000
- **Columns**: 23

#### Columns

| Column | Type | Null % | Distinct | Notes |
|--------|------|--------|----------|-------|
| product_id | object | 0.0% | 10000 | PK candidate;  |
| sku | object | 0.0% | 9947 |  |
| product_name | object | 0.0% | 9948 |  |
| category | object | 0.0% | 6 |  |
| subcategory | object | 0.0% | 23 |  |
| brand | object | 0.0% | 8670 |  |
| price_usd | float64 | 0.0% | 9535 |  |
| cost_usd | float64 | 0.0% | 9112 |  |
| weight_kg | float64 | 0.0% | 4319 |  |
| dimensions | object | 0.0% | 9902 |  |
| color | object | 0.0% | 8 |  |
| size | object | 0.0% | 7 |  |
| active | bool | 0.0% | 2 |  |
| stock_quantity | int64 | 0.0% | 1001 |  |
| reorder_level | int64 | 0.0% | 91 |  |
| supplier_id | object | 0.0% | 100 |  |
| created_at | object | 0.0% | 10000 | PK candidate;  |
| updated_at | object | 0.0% | 10000 | PK candidate;  |
| source_system | object | 0.0% | 1 | Constant;  |
| ingestion_timestamp | object | 0.0% | 1 | Constant;  |

#### Quality Notes

- ℹ️ 3 potential primary keys

---
