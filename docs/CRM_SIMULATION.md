# CRM Data Simulation - Phase 2

## Overview

This project includes a **CRM data simulation** that mimics Salesforce data structure for local development and testing. In production, this would connect to the actual Salesforce API.

## 🎯 Purpose

- **No API Authentication**: Avoid Salesforce API setup and billing
- **Realistic Data Model**: Maintain Salesforce-compatible field structure
- **Joinable Data**: Perfect for joining with Snowflake orders/customers/products
- **ETL Pipeline Ready**: Promotable to S3 Bronze → Silver → Gold layers

## 📊 Data Sources

### CRM Data Location
```
aws/data/crm/
├── accounts.csv      (20,000 records, 29 columns)
├── contacts.csv      (60,000 records, 24 columns)
└── opportunities.csv (100,000 records, 22 columns)
```

### Salesforce-Compatible Fields

#### Accounts (`aws/data/crm/accounts.csv`)
- **Required**: `Name` (Account Name)
- **High-Value**: `Industry`, `AnnualRevenue`, `NumberOfEmployees`
- **Address**: `BillingStreet`, `BillingCity`, `BillingState`, `BillingCountry`
- **Sales**: `Rating`, `Type`, `AccountSource`
- **Analytics**: `CustomerSegment`, `GeographicRegion`, `AccountStatus`

#### Contacts (`data/crm/contacts.csv`)
- **Required**: `LastName`, `AccountId` (links to Account Name)
- **Contact Info**: `FirstName`, `Email`, `Phone`, `Title`, `Department`
- **Address**: `MailingStreet`, `MailingCity`, `MailingState`, `MailingCountry`
- **Analytics**: `ContactRole`, `ContactLevel`, `EngagementScore`

#### Opportunities (`data/crm/opportunities.csv`)
- **Required**: `Name`, `AccountId`, `StageName`, `CloseDate`, `Amount`
- **Sales Process**: `Probability`, `Type`, `ForecastCategory`, `IsClosed`, `IsWon`
- **Analytics**: `DealSize`, `SalesCycle`, `ProductInterest`, `Budget`, `Timeline`

## 🔄 ETL Pipeline

### Extraction Jobs
- `src/pyspark_interview_project/extract/crm_accounts.py`
- `src/pyspark_interview_project/extract/crm_contacts.py`
- `src/pyspark_interview_project/extract/crm_opportunities.py`

### Pipeline Driver
- `crm_etl_pipeline.py` - Complete end-to-end ETL pipeline

### Data Flow
```
CSV Files → Bronze Layer → Silver Layer → Gold Layer → Analytics
```

## 🚀 Usage

### Run Complete ETL Pipeline
```bash
python crm_etl_pipeline.py
```

### Pipeline Phases
1. **EXTRACT**: Load CRM data from CSV files
2. **BRONZE**: Raw data landing in Parquet format
3. **SILVER**: Cleaned and validated data
4. **GOLD**: Business-ready analytics tables
5. **ANALYTICS**: Revenue reports and insights

## 📈 Analytics Output

### Gold Layer Tables
- `revenue_by_industry` - Revenue breakdown by industry
- `revenue_by_geography` - Revenue by country/state
- `customer_segmentation` - Revenue by customer segment

### Sample Analytics
- Top industries by revenue
- Geographic revenue distribution
- Customer segment performance
- Sales pipeline analysis

## 🔗 Data Relationships

### Perfect Joins
- **Contacts → Accounts**: All 60,000 contacts link to accounts
- **Opportunities → Accounts**: All 100,000 opportunities link to accounts
- **Snowflake Integration**: Account names match Snowflake customer data

### Join Keys
- `contacts.AccountId` → `accounts.Name`
- `opportunities.AccountId` → `accounts.Name`
- `accounts.Name` → `snowflake_orders.customer_name`

## 🏭 Production Notes

### In Production (AWS)
- Replace CSV files with Salesforce API calls
- Use `simple_salesforce` library for API integration
- Store credentials in AWS Secrets Manager
- Deploy to EMR Serverless for processing

### Local Development
- CSV files simulate Salesforce data
- No API authentication required
- Perfect for testing and development
- Identical data structure to production

## 📊 Data Quality

### Validation
- All required Salesforce fields present
- No empty required fields
- Valid email formats
- Perfect relationship integrity
- Realistic data distributions

### Volume
- **Total Records**: 180,000 CRM records
- **Data Size**: ~64 MB of realistic CRM data
- **Coverage**: Complete customer lifecycle from prospect to closed deals

## 🎯 Benefits

✅ **No API Costs**: Avoid Salesforce API billing  
✅ **No Authentication**: Skip OAuth setup  
✅ **Realistic Testing**: Full Salesforce data model  
✅ **Perfect Joins**: Seamless integration with Snowflake data  
✅ **Production Ready**: Same structure as real Salesforce  
✅ **Analytics Ready**: Rich data for business intelligence  

## 🔄 Migration Path

### Phase 2 (Current)
- Local CSV simulation
- Complete ETL pipeline
- Analytics and reporting

### Phase 3 (Production)
- Salesforce API integration
- AWS EMR Serverless deployment
- Real-time data sync

This CRM simulation provides a complete, production-ready data model without the complexity of API integration, perfect for Phase 2 development and testing.
