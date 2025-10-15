# External Data Sources Integration Guide

This document describes the integration of external data sources (Redshift and HubSpot) into the PySpark ETL pipeline.

## Overview

The pipeline now supports ingestion from two major external data sources:

1. **Amazon Redshift** - Data warehouse for marketing campaigns, customer behavior, and web analytics
2. **HubSpot CRM** - Customer relationship management system for contacts, deals, companies, and tickets

## Data Sources Configuration

### Redshift Configuration

The Redshift integration extracts data from three main tables:

#### Marketing Campaigns
- **Source**: Google Ads API data stored in Redshift
- **Frequency**: Hourly
- **Key Fields**: campaign_id, campaign_name, status, budget, spend, impressions, clicks, conversions
- **Use Case**: Marketing performance analysis and ROI tracking

#### Customer Behavior
- **Source**: Amplitude events data stored in Redshift
- **Frequency**: Real-time
- **Key Fields**: user_id, event_name, event_timestamp, session_id, page_url, device_type
- **Use Case**: User behavior analysis and product optimization

#### Web Analytics
- **Source**: Google Analytics 4 data stored in Redshift
- **Frequency**: Daily
- **Key Fields**: date, page_path, sessions, users, pageviews, bounce_rate, revenue
- **Use Case**: Website performance and conversion analysis

### HubSpot Configuration

The HubSpot integration extracts data from four main endpoints:

#### Contacts
- **Endpoint**: `/crm/v3/objects/contacts`
- **Frequency**: Hourly
- **Key Fields**: id, email, firstname, lastname, phone, company, lifecyclestage
- **Use Case**: Customer segmentation and lead management

#### Deals
- **Endpoint**: `/crm/v3/objects/deals`
- **Frequency**: Hourly
- **Key Fields**: id, dealname, amount, dealstage, closedate, pipeline
- **Use Case**: Sales pipeline analysis and revenue forecasting

#### Companies
- **Endpoint**: `/crm/v3/objects/companies`
- **Frequency**: Daily
- **Key Fields**: id, name, domain, industry, city, state, country
- **Use Case**: Account-based marketing and territory analysis

#### Tickets
- **Endpoint**: `/crm/v3/objects/tickets`
- **Frequency**: Hourly
- **Key Fields**: id, subject, content, hs_ticket_priority, hs_ticket_category, hs_resolution
- **Use Case**: Customer support analytics and SLA monitoring

## Configuration Setup

### Environment Variables

Set the following environment variables for your deployment:

```bash
# Redshift Configuration
export REDSHIFT_CLUSTER_ID="your-cluster-id"
export REDSHIFT_DATABASE="your-database"
export REDSHIFT_USERNAME="your-username"
export REDSHIFT_PASSWORD="your-password"
export AWS_REGION="us-east-1"

# HubSpot Configuration
export HUBSPOT_API_KEY="your-hubspot-api-key"
```

### Configuration Files

The data sources are configured in the following files:

- `config/default.yaml` - Default configuration for local development
- `config/aws.yaml` - AWS-specific configuration for production
- `config/azure.yaml` - Azure-specific configuration for production

Example configuration:

```yaml
data_sources:
  redshift:
    cluster_identifier: "${REDSHIFT_CLUSTER_ID}"
    database: "${REDSHIFT_DATABASE}"
    port: 5439
    username: "${REDSHIFT_USERNAME}"
    password: "${REDSHIFT_PASSWORD}"
    schema: "public"
    tables:
      - name: "marketing_campaigns"
        source: "google_ads_api"
        frequency: "hourly"
      - name: "customer_behavior"
        source: "amplitude_events"
        frequency: "real-time"
      - name: "web_analytics"
        source: "google_analytics_4"
        frequency: "daily"
  
  hubspot:
    base_url: "https://api.hubapi.com"
    api_key: "${HUBSPOT_API_KEY}"
    rate_limit: 100
    endpoints:
      - name: "contacts"
        path: "/crm/v3/objects/contacts"
        frequency: "hourly"
      - name: "deals"
        path: "/crm/v3/objects/deals"
        frequency: "hourly"
      - name: "companies"
        path: "/crm/v3/objects/companies"
        frequency: "daily"
      - name: "tickets"
        path: "/crm/v3/objects/tickets"
        frequency: "hourly"
```

## Data Pipeline Architecture

### Bronze Layer
External data is first ingested into the Bronze layer with minimal transformation:

```
data/lakehouse/bronze/
├── redshift/
│   ├── marketing_campaigns/
│   ├── customer_behavior/
│   └── web_analytics/
└── hubspot/
    ├── contacts/
    ├── deals/
    ├── companies/
    └── tickets/
```

### Data Quality Checks

The pipeline includes comprehensive data quality checks:

#### Redshift Data Quality
- Record count validation
- Null value checks on key columns
- Duplicate record detection
- Data freshness validation
- Negative value detection in numeric fields

#### HubSpot Data Quality
- Record count validation
- Null value checks on key properties
- Duplicate record detection by ID
- Email format validation for contacts
- Amount presence validation for deals
- Data freshness validation

### Airflow Orchestration

The pipeline uses two main DAGs:

1. **external_data_ingestion** - Handles external data source ingestion
2. **pyspark_delta_etl** - Main ETL pipeline that processes all data

#### External Data Ingestion DAG
- Runs hourly
- Extracts data from Redshift and HubSpot
- Performs data quality checks
- Loads data into Bronze layer

#### Main ETL DAG
- Runs daily
- Triggers external data ingestion first
- Processes Bronze to Silver to Gold layers
- Includes all data sources in final output

## Usage Examples

### Running Redshift Extraction

```python
from pyspark_interview_project.jobs.redshift_to_bronze import extract_redshift_data
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config

# Load configuration
config = load_config()

# Build Spark session
spark = build_spark(config)

# Extract marketing campaigns data
df = extract_redshift_data(spark, config, "marketing_campaigns")
```

### Running HubSpot Extraction

```python
from pyspark_interview_project.jobs.hubspot_to_bronze import extract_hubspot_data
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config

# Load configuration
config = load_config()

# Build Spark session
spark = build_spark(config)

# Extract contacts data
df = extract_hubspot_data(spark, config, "contacts")
```

### Running Data Quality Checks

```python
from pyspark_interview_project.dq.external_data_quality import ExternalDataQualityChecker
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config

# Load configuration
config = load_config()

# Build Spark session
spark = build_spark(config)

# Run quality checks
checker = ExternalDataQualityChecker(spark, config)
results = checker.run_all_external_data_quality_checks()
```

## Monitoring and Alerting

### Metrics Tracked
- Record counts for each data source
- Data quality check results
- Extraction duration and success rates
- API rate limit usage
- Error rates and failure patterns

### Alerts
- Data quality failures
- API connection issues
- Missing data or empty extractions
- Performance degradation

## Troubleshooting

### Common Issues

#### Redshift Connection Issues
- Verify cluster identifier and credentials
- Check network connectivity and security groups
- Ensure proper IAM permissions

#### HubSpot API Issues
- Verify API key validity and permissions
- Check rate limiting and quota usage
- Monitor API response codes and error messages

#### Data Quality Issues
- Review null value patterns
- Check for schema changes in source systems
- Validate data freshness and completeness

### Debug Commands

```bash
# Test Redshift connection
python -c "
from pyspark_interview_project.jobs.redshift_to_bronze import RedshiftExtractor
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config

config = load_config()
spark = build_spark(config)
extractor = RedshiftExtractor(spark, config)
print('Redshift connection successful')
"

# Test HubSpot connection
python -c "
from pyspark_interview_project.jobs.hubspot_to_bronze import HubSpotAPI
from pyspark_interview_project.config_loader import load_config

config = load_config()
api = HubSpotAPI(config)
print('HubSpot API connection successful')
"

# Run data quality checks
python -m pyspark_interview_project.dq.external_data_quality
```

## Security Considerations

### Credential Management
- Use environment variables for sensitive data
- Implement proper secret management (AWS Secrets Manager, Azure Key Vault)
- Rotate credentials regularly

### Network Security
- Use VPC endpoints for Redshift connections
- Implement proper firewall rules
- Use SSL/TLS for all connections

### Data Privacy
- Implement data masking for sensitive fields
- Follow GDPR and CCPA compliance requirements
- Implement proper data retention policies

## Performance Optimization

### Redshift Optimization
- Use appropriate batch sizes for extraction
- Implement incremental loading strategies
- Optimize SQL queries for better performance

### HubSpot Optimization
- Implement proper rate limiting
- Use pagination for large datasets
- Cache frequently accessed data

### Spark Optimization
- Tune Spark configuration for your cluster
- Use appropriate partitioning strategies
- Implement data compression and optimization

## Future Enhancements

### Planned Features
- Real-time streaming from Redshift
- Webhook integration with HubSpot
- Advanced data quality rules
- Automated schema evolution
- Cross-source data correlation

### Integration Opportunities
- Additional CRM systems (Salesforce, Pipedrive)
- Marketing platforms (Google Ads, Facebook Ads)
- Analytics platforms (Mixpanel, Amplitude)
- Support systems (Zendesk, Freshdesk)
