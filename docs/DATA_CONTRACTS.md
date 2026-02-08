# Data Contracts & Ownership

## Overview

Data contracts define the **explicit agreements** between data producers and consumers. They specify schema expectations, quality requirements, SLAs, and ownership.

**Purpose**: Prevent breaking changes from propagating silently through the data platform.

## Contract Artifacts (Executable + Human-Readable)

- Machine-readable contract file: `config/contracts/silver_contracts.yaml`
- Runtime fail-fast validator: `src/project_a/contracts/runtime_contracts.py`
- dbt model/source tests: `dbt/models/schema.yml`, `dbt/models/sources.yml`

Pipeline behavior: contract violations fail the pipeline run.

## Contract Philosophy

> "Treat data like an API - versioned, tested, and backward compatible"

### Core Principles

1. **Explicit is Better Than Implicit**: Document all expectations
2. **Fail Fast**: Violating a contract stops the pipeline immediately
3. **Backward Compatibility**: Schema changes must not break existing consumers
4. **Clear Ownership**: Every dataset has an owner responsible for quality and availability

## Silver Layer Contracts

### customers_silver

**Owner**: Data Engineering Team  
**Contact**: data-eng@company.com  
**SLA**: Daily refresh by 6 AM UTC  
**Retention**: 7 years (compliance requirement)

**Schema Contract**:
```yaml
columns:
  customer_id: {type: string, nullable: false, unique: true}
  email: {type: string, nullable: false, pii: true}
  first_name: {type: string, nullable: true, pii: true}
  last_name: {type: string, nullable: true, pii: true}
  country: {type: string, nullable: true}
  registration_date: {type: date, nullable: false}
```

**Quality Contract**:
- Uniqueness: `customer_id` must be unique (100%)
- Completeness: `customer_id`, `email`, `registration_date` must be non-null (100%)
- Referential Integrity: All customers referenced in orders must exist
- Freshness: Data no older than 24 hours

**Breaking Change Policy**:
- Removing columns: 2-week notice + deprecation period
- Changing column types: Requires major version bump
- Adding columns: Safe, no notice required

**Consumers**:
- dbt: dim_customer, fct_orders
- ML Models: customer_churn_prediction
- BI Dashboards: Customer 360 Report

---

### orders_silver

**Owner**: Data Engineering Team  
**Contact**: data-eng@company.com  
**SLA**: Hourly refresh (15 minutes past the hour)  
**Retention**: 7 years (compliance requirement)

**Schema Contract**:
```yaml
columns:
  order_id: {type: string, nullable: false, unique: true}
  customer_id: {type: string, nullable: false, fk: customers_silver}
  product_id: {type: string, nullable: true, fk: products_silver}
  order_date: {type: date, nullable: false}
  total_amount: {type: decimal(10,2), nullable: false, min: 0}
```

**Quality Contract**:
- Uniqueness: `order_id` must be unique (100%)
- Completeness: Required fields non-null (100%)
- Referential Integrity: All `customer_id` must exist in customers_silver
- Value Ranges: `total_amount` >= 0
- Freshness: Data no older than 2 hours

**Late Data Handling**:
- Orders can arrive up to 48 hours late
- Rolling 3-day window reprocessed to capture updates
- Merge strategy ensures idempotency

**Breaking Change Policy**:
- Same as customers_silver

**Consumers**:
- dbt: fct_orders, dim_product
- BI Dashboards: Sales Analytics
- Finance: Revenue Reporting

---

### customer_behavior_silver

**Owner**: Data Engineering Team  
**Contact**: data-eng@company.com  
**SLA**: Real-time (< 5 min lag from source)  
**Retention**: 90 days (hot), 2 years (cold)

**Schema Contract**:
```yaml
columns:
  event_id: {type: string, nullable: false, unique: true}
  customer_id: {type: string, nullable: false, fk: customers_silver}
  event_type: {type: string, nullable: false, enum: [page_view, product_view, add_to_cart, purchase, email_open, email_click]}
  event_ts: {type: timestamp, nullable: false}
  session_id: {type: string, nullable: true}
  time_spent_seconds: {type: integer, nullable: true, min: 0}
```

**Quality Contract**:
- Uniqueness: `event_id` must be unique (100%)
- Completeness: `event_id`, `customer_id`, `event_type`, `event_ts` non-null (100%)
- Value Constraints: `event_type` must be in allowed list
- Freshness: Data no older than 5 minutes (real-time SLA)
- Volume: Expected 10K-50K events/hour (alert if outside range)

**Breaking Change Policy**:
- Adding event types: Safe (backward compatible)
- Removing event types: 1-month deprecation notice
- Schema changes: 2-week notice

**Consumers**:
- ML Models: Recommendation Engine
- dbt: Customer Engagement Metrics
- Real-time Dashboards: Active Users

## Gold Layer Contracts

### dim_customer

**Owner**: Analytics Engineering Team  
**Contact**: analytics@company.com  
**SLA**: Daily refresh by 7 AM UTC (1 hour after Silver)  
**Retention**: Indefinite (slowly changing dimension)

**Schema Contract**:
```yaml
primary_key: customer_id
scd_type: 1
columns:
  customer_id: {type: string, nullable: false, unique: true}
  first_name: {type: string, nullable: true, pii: true}
  last_name: {type: string, nullable: true, pii: true}
  email: {type: string, nullable: false, pii: true}
  country: {type: string, nullable: true}
  registration_date: {type: date, nullable: false}
  lifetime_orders: {type: integer, nullable: false, min: 0}
  lifetime_value: {type: decimal(10,2), nullable: false, min: 0}
  customer_segment: {type: string, nullable: false, enum: [VIP, High Value, Regular, New]}
  dbt_updated_at: {type: timestamp, nullable: false}
```

**Quality Contract**:
- Uniqueness: `customer_id` PK constraint (100%)
- Completeness: All required fields non-null (100%)
- Validity: `customer_segment` in allowed values (100%)
- Referential Integrity: No orphaned records from source

**Business Logic**:
- Customer segmentation based on lifetime_value
- Aggregations from orders and behavior data
- Type 1 SCD: Overwrite on every refresh

**Breaking Change Policy**:
- Schema changes: 2-week notice to BI team
- Segment definition changes: 1-week notice + documentation update
- Adding columns: Safe

**Consumers**:
- BI Tool: Tableau (Customer Dashboard)
- Marketing: Customer Segmentation Tool
- ML: Customer LTV Prediction

---

### fct_orders

**Owner**: Analytics Engineering Team  
**Contact**: analytics@company.com  
**SLA**: Hourly refresh (30 minutes past the hour)  
**Retention**: 7 years (compliance)

**Schema Contract**:
```yaml
primary_key: order_id
grain: One row per order
incremental: true
columns:
  order_id: {type: string, nullable: false, unique: true}
  customer_id: {type: string, nullable: false, fk: dim_customer}
  product_id: {type: string, nullable: true, fk: dim_product}
  order_date: {type: date, nullable: false}
  total_amount: {type: decimal(10,2), nullable: false, min: 0}
  customer_segment: {type: string, nullable: true}
  product_category: {type: string, nullable: true}
  estimated_margin: {type: decimal(10,2), nullable: true}
  updated_at: {type: timestamp, nullable: false}
```

**Quality Contract**:
- Uniqueness: `order_id` PK constraint (100%)
- Completeness: Required fields non-null (100%)
- Referential Integrity: All FKs valid (100%)
- Late Data: 3-day rolling window handles corrections

**Incremental Strategy**:
- Merge on `order_id`
- Reprocess last 3 days on every run
- Idempotent: Safe to re-run

**Breaking Change Policy**:
- Same as dim_customer

**Consumers**:
- BI Tool: Revenue Analytics Dashboard
- Finance: Monthly Revenue Reports
- ML: Demand Forecasting

## Testing Strategy

### Contract Enforcement

Contracts are enforced through **dbt tests** defined in `schema.yml`:

```yaml
# Example: Uniqueness test
- name: order_id
  tests:
    - unique
    - not_null

# Example: Referential integrity test
- name: customer_id
  tests:
    - relationships:
        to: ref('dim_customer')
        field: customer_id

# Example: Value constraint test
- name: customer_segment
  tests:
    - accepted_values:
        values: ['VIP', 'High Value', 'Regular', 'New']
```

### Test Execution

Tests run:
1. **On every dbt run** (CI/CD integration)
2. **Before promoting to production**
3. **On scheduled data quality checks**

**Failure Behavior**: Pipeline stops if any contract test fails

## Change Management

### Backward-Compatible Changes (Safe)

✅ **Can be deployed immediately:**
- Adding new columns
- Adding new enum values
- Relaxing constraints (making nullable)
- Adding new tables

### Breaking Changes (Require Notice)

⚠️ **Require advance notice:**
- Removing columns (2-week notice)
- Changing column types (2-week notice)
- Removing enum values (1-month deprecation)
- Changing PK/FK relationships (2-week notice)

### Change Process

1. **Propose Change**: Create RFC document
2. **Impact Analysis**: Identify affected consumers
3. **Notice Period**: Notify consumers (email + Slack)
4. **Deprecation**: Mark old columns as deprecated
5. **Migration**: Provide transition period
6. **Removal**: Delete after notice period

## Ownership & SLAs

### Team Responsibilities

**Data Engineering Team** (Silver Layer):
- Ensures data arrives on schedule
- Maintains data quality standards
- Responds to pipeline failures within 1 hour
- Handles schema evolution

**Analytics Engineering Team** (Gold Layer):
- Maintains dbt models
- Ensures business logic correctness
- Responds to model failures within 2 hours
- Collaborates on contract changes

### SLA Definitions

| Layer | Refresh Frequency | Max Latency | Availability |
|-------|------------------|-------------|--------------|
| Silver | Hourly | 15 minutes | 99.5% |
| Gold | Hourly | 30 minutes | 99.5% |

**Incident Response**:
- P0 (Data Loss): 30 min response, 2 hour resolution
- P1 (SLA Miss): 1 hour response, 4 hour resolution
- P2 (Quality Issue): 4 hour response, 24 hour resolution

## Monitoring & Alerts

### Contract Violations

Alerts trigger on:
- ❌ Schema mismatch detected
- ❌ Uniqueness constraint violated
- ❌ Referential integrity broken
- ❌ SLA missed (late data)
- ❌ Volume anomaly (too many/few records)

### Alert Channels

- **Critical**: PagerDuty
- **Warning**: Slack #data-alerts
- **Info**: Datadog Dashboard

## Documentation

Contracts are documented in:
1. **This file**: High-level agreements
2. **dbt schema.yml**: Enforcement via tests
3. **Data Catalog**: Searchable metadata
4. **Confluence**: Team runbooks

## Future Enhancements

- [ ] Automated contract testing in CI/CD
- [ ] Contract versioning (v1, v2, etc.)
- [ ] Consumer registration system
- [ ] Automated impact analysis
- [ ] Contract performance SLAs

---

**Last Updated**: 2026-02-06  
**Next Review**: 2026-03-06  
**Document Owner**: Data Engineering Lead
