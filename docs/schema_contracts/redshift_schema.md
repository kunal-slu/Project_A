# Redshift Data Schema Documentation

## Overview
This document defines the schema contracts for Amazon Redshift data warehouse integration in our enterprise ETL pipeline.

## Bronze Layer Schemas

### Redshift Customer Behavior (`redshift_customer_behavior_bronze`)

**Source**: Amazon Redshift Data Warehouse  
**Target**: `s3://company-data-lake-ACCOUNT_ID/bronze/redshift/customer_behavior/`  
**Format**: Delta Lake (Parquet)

#### Required Fields
| Field Name | Data Type | Constraints | Description |
|------------|-----------|-------------|-------------|
| `behavior_id` | STRING | NOT NULL, PRIMARY KEY | Unique behavior event identifier |
| `customer_id` | STRING | NOT NULL, FOREIGN KEY | Reference to customer |
| `event_type` | STRING | NOT NULL | Type of behavior event |
| `event_timestamp` | TIMESTAMP | NOT NULL | When the event occurred |
| `session_id` | STRING | NULLABLE | User session identifier |
| `page_url` | STRING | NULLABLE | URL where event occurred |
| `referrer_url` | STRING | NULLABLE | Referring URL |
| `user_agent` | STRING | NULLABLE | Browser user agent |
| `ip_address` | STRING | NULLABLE | User IP address |
| `device_type` | STRING | NULLABLE | Device type (mobile/desktop/tablet) |
| `browser` | STRING | NULLABLE | Browser name |
| `operating_system` | STRING | NULLABLE | Operating system |
| `country` | STRING | NULLABLE | User country |
| `city` | STRING | NULLABLE | User city |
| `event_properties` | STRING | NULLABLE | JSON string of event properties |
| `created_at` | TIMESTAMP | NOT NULL | Record creation timestamp |

#### Metadata Fields
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `_source_system` | STRING | Source system identifier |
| `_ingestion_ts` | TIMESTAMP | Data ingestion timestamp |
| `_job_id` | STRING | ETL job identifier |

#### Data Quality Rules
1. **Primary Key Constraint**: `behavior_id` must be unique and non-null
2. **Foreign Key Constraint**: `customer_id` must reference valid customer
3. **Business Rule**: `event_timestamp` must be <= current timestamp
4. **Business Rule**: `event_timestamp` must be >= `created_at`
5. **Freshness Check**: `created_at` must be within last 2 hours

### Event Types
- `page_view`: User viewed a page
- `click`: User clicked on an element
- `purchase`: User completed a purchase
- `signup`: User signed up
- `login`: User logged in
- `search`: User performed a search
- `add_to_cart`: User added item to cart
- `remove_from_cart`: User removed item from cart

## Silver Layer Schemas

### Customer Behavior Analytics (`customer_behavior_analytics`)

**Source**: `redshift_customer_behavior_bronze`  
**Target**: `s3://company-data-lake-ACCOUNT_ID/silver/redshift/customer_behavior_analytics/`

#### Enrichments
- `session_duration`: Calculated session length
- `page_views_per_session`: Count of page views per session
- `conversion_funnel_stage`: User's position in conversion funnel
- `engagement_score`: Calculated engagement metric
- `geographic_region`: Derived from country/city
- `device_category`: Simplified device type

## Gold Layer Schemas

### Customer Journey Analytics (`customer_journey_analytics`)

**Source**: `customer_behavior_analytics` + CRM data  
**Target**: `s3://company-data-lake-ACCOUNT_ID/gold/analytics/customer_journey/`

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `customer_id` | STRING | Customer identifier |
| `journey_stage` | STRING | Current journey stage |
| `total_sessions` | INTEGER | Total number of sessions |
| `total_page_views` | INTEGER | Total page views |
| `avg_session_duration` | DOUBLE | Average session duration |
| `conversion_rate` | DOUBLE | Conversion rate |
| `last_activity_date` | TIMESTAMP | Last activity date |
| `engagement_score` | DOUBLE | Calculated engagement score |

### Behavioral Segmentation (`behavioral_segmentation`)

**Source**: `customer_behavior_analytics`  
**Target**: `s3://company-data-lake-ACCOUNT_ID/gold/analytics/behavioral_segmentation/`

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `customer_id` | STRING | Customer identifier |
| `behavioral_segment` | STRING | Behavioral segment |
| `recency_score` | INTEGER | Recency score (1-5) |
| `frequency_score` | INTEGER | Frequency score (1-5) |
| `engagement_score` | INTEGER | Engagement score (1-5) |
| `predicted_lifetime_value` | DOUBLE | Predicted LTV |
| `churn_probability` | DOUBLE | Churn probability |

## Data Quality Monitoring

### Bronze Layer DQ Checks
- **Freshness**: Data must be ingested within SLA (hourly for behavior data)
- **Volume**: Record count must be within expected range (±15% for streaming data)
- **Completeness**: Required fields must be non-null
- **Referential Integrity**: `customer_id` must reference valid customer
- **Temporal Consistency**: `event_timestamp` must be reasonable

### Silver Layer DQ Checks
- **Business Rules**: Calculated fields must be within expected ranges
- **Data Consistency**: Cross-session consistency checks
- **Anomaly Detection**: Identify unusual behavior patterns

### Gold Layer DQ Checks
- **Aggregation Accuracy**: Verify calculated metrics
- **Trend Analysis**: Detect unusual patterns in behavior
- **Model Validation**: Validate ML model outputs

## Compliance Notes

### PII Handling
- **IP addresses**: Anonymized in non-production environments
- **User agents**: Available for analytics, logged for audit
- **Email addresses**: Not stored in behavior data (referenced via customer_id)

### Data Retention
- **Bronze**: 1 year (privacy requirement)
- **Silver**: 6 months (analytics requirement)
- **Gold**: 3 months (reporting requirement)

### Access Control
- **Bronze**: Data Engineers only
- **Silver**: Data Engineers + Data Scientists
- **Gold**: Data Scientists + Business Analysts

### Privacy Compliance
- **GDPR**: Right to deletion implemented
- **CCPA**: Data portability supported
- **Data Minimization**: Only necessary fields collected

---

*This document is maintained by the Data Engineering team and reviewed quarterly for accuracy and compliance.*
