# Platform Overview

This document provides a comprehensive overview of our data platform, its architecture, and how it operates in production.

## Executive Summary

Our data platform is an enterprise-grade, production-ready system that ingests data from 5+ upstream sources, transforms it through Bronze-Silver-Gold layers, and provides safe, analytics-ready data to business users.

## Architecture

### Data Ingestion Layer
We ingest data from multiple upstream systems:

1. **CRM System (HubSpot)**: Contact and company data
2. **Data Warehouse (Snowflake)**: Historical analytics data
3. **Analytics Platform (Redshift)**: Real-time behavioral data
4. **Streaming Platform (Kafka)**: Real-time order events
5. **FX Vendor**: Foreign exchange rate data

All credentials are securely stored in AWS Secrets Manager.

### Data Lake Architecture

#### Bronze Layer (Raw Storage)
- **Purpose**: Store data exactly as received from source systems
- **Format**: Delta Lake
- **Partitioning**: By date and source
- **Access**: Data Engineers only
- **Validation**: Schema contract validation on ingestion

#### Silver Layer (Cleaned Data)
- **Purpose**: Cleaned, standardized, deduplicated data
- **Format**: Delta Lake
- **Transformations**: 
  - Deduplication
  - Standardization
  - Null handling
  - Data type conversions
- **Access**: Data Engineers + Data Analysts
- **Validation**: Data quality checks

#### Gold Layer (Business-Ready Data)
- **Purpose**: Analytics-ready, business-focused tables
- **Format**: Delta Lake
- **Transformations**:
  - Business logic application
  - Data masking (PII protection)
  - Aggregation and enrichment
  - Fact and dimension tables
- **Access**: All roles (Engineers, Analysts, Business Users)
- **Security**: PII masked, safe for analytics

### Business Modeling

#### Fact Tables
- **gold_fact_sales**: Sales transactions and revenue
- **gold_marketing_attribution**: Marketing campaign performance

#### Dimension Tables
- **gold_dim_customers**: Customer information (masked)
- **gold_dim_products**: Product catalog
- **gold_dim_date**: Date dimension for time-series analysis

#### Metric Tables
- **gold_customer_segments**: Customer segmentation analysis
- **gold_product_performance**: Product performance metrics

### Data Quality Framework

We implement comprehensive data quality checks:

**Freshness Checks**: Ensure data is up-to-date
**Volume Checks**: Detect anomalies in data volume
**Null Percentage**: Monitor data completeness
**Referential Integrity**: Validate relationships
**Custom Business Rules**: Enforce business logic

Quality gates must pass before data moves from Bronze→Silver→Gold.

### Observability and Lineage

Every pipeline execution:
- Creates unique run IDs
- Emits lineage events to CloudWatch
- Tracks data flow from source to target
- Logs row counts and processing time
- Provides audit trail for compliance

### Data Governance

**Data Classification**:
- PII: Email, phone, SSN, credit card
- Sensitive: Salary, revenue, profit margin

**Access Control**:
- Bronze: Data Engineers only
- Silver: Data Engineers + Analysts
- Gold: All roles with appropriate masking

**Data Masking**:
- PII columns hashed or masked
- Sensitive data statistically masked
- Audit trails maintained

### Infrastructure

**Compute**: AWS EMR Serverless
**Storage**: S3 with Delta Lake format
**Orchestration**: Apache Airflow (MWAA)
**Catalog**: AWS Glue Catalog
**Monitoring**: CloudWatch
**Access Control**: Lake Formation
**Security**: IAM roles with least privilege

### CI/CD Pipeline

**Automated Testing**:
- Unit tests for all transformations
- Integration tests for complete pipelines
- DAG import validation
- Schema contract validation
- Configuration validation

**Deployment**:
- Terraform for infrastructure
- Automated artifact builds
- DAG synchronization to MWAA
- Zero-downtime deployments

## Operational Procedures

### Daily Operations
1. Bronze ingestion runs at 02:00 UTC
2. Silver transformation completes by 02:30 UTC
3. Gold business logic completes by 03:00 UTC
4. Data quality checks run throughout
5. Monitoring and alerting active 24/7

### Data Quality Monitoring
- Independent DQ watchdog DAG runs hourly
- Alerts on SLA violations
- Quality score tracking
- Automated remediation when possible

### Backfill and Recovery
- Controlled backfill for specific dates/sources
- Rebuild procedures for corrupted data
- Late-arriving data handling
- Documented recovery procedures

### Service Level Agreements
- Bronze ingestion: 02:15 UTC ±15 minutes
- Silver transformation: 02:30 UTC ±15 minutes
- Gold business logic: 03:00 UTC ±30 minutes
- Streaming delay: <5 minutes

## Security and Compliance

**Encryption**: At-rest and in-transit
**Access Control**: IAM roles with least privilege
**Audit Logging**: All operations logged
**Data Masking**: PII protection enforced
**Compliance**: GDPR, CCPA, SOX, HIPAA ready

## Monitoring and Alerting

**CloudWatch Metrics**:
- Pipeline execution status
- Data quality scores
- Processing times
- Row counts

**Alerting**:
- SLA violations → PagerDuty
- Data quality issues → Slack #data-alerts
- System failures → Email + Slack

**Dashboards**:
- Real-time pipeline status
- Data quality trends
- Performance metrics
- Incident history

## Best Practices

**Development**:
- Schema contracts for all sources
- Unit tests for transformations
- Documentation for all changes

**Operations**:
- Least privilege access
- Regular access reviews
- Automated testing
- Continuous monitoring

**Security**:
- Never hardcode credentials
- Use Secrets Manager
- Encrypt sensitive data
- Audit all access

## Team and Responsibilities

**Data Engineering Team**:
- Pipeline development and maintenance
- Data quality enforcement
- Backfill and recovery operations
- On-call support

**Data Analytics Team**:
- Business metrics definition
- Gold layer modeling
- Analytics support
- Business user support

**Platform Team**:
- Infrastructure provisioning
- Performance optimization
- Security enforcement
- Capacity planning

## Getting Started

### For Data Engineers
1. Review `docs/guides/BEGINNERS_GUIDE.md`
2. Set up local environment
3. Run test pipeline
4. Review backfill procedures

### For Data Analysts
1. Access Gold layer tables in Athena
2. Review `docs/guides/BUSINESS_METRICS_DICTIONARY.md`
3. Understand data governance policies
4. Learn reporting tools

### For Business Users
1. Access Gold layer dashboards
2. Understand data refresh schedule
3. Report issues to data-team@company.com

## Contact and Support

**General Questions**: data-team@company.com
**Incidents**: #data-alerts Slack channel
**On-Call**: Check PagerDuty
**Documentation**: This repository

---

*Last Updated: 2025-01-27*
*Version: 1.0*
