# CRM Data Schema Documentation

## Overview
This document defines the schema contracts for CRM data ingestion and processing in our enterprise ETL pipeline. These schemas ensure data quality, consistency, and audit compliance.

## Bronze Layer Schemas

### CRM Accounts (`crm_accounts_bronze`)

**Source**: Salesforce API / CRM System  
**Target**: `s3://company-data-lake-ACCOUNT_ID/bronze/crm/accounts/`  
**Format**: Delta Lake (Parquet)

#### Required Fields
| Field Name | Data Type | Constraints | Description |
|------------|-----------|-------------|-------------|
| `Id` | STRING | NOT NULL, PRIMARY KEY | Unique account identifier |
| `Name` | STRING | NOT NULL | Account name |
| `Type` | STRING | NULLABLE | Account type (Customer, Prospect, etc.) |
| `Industry` | STRING | NULLABLE | Industry classification |
| `AnnualRevenue` | DOUBLE | NULLABLE, >= 0 | Annual revenue amount |
| `NumberOfEmployees` | INTEGER | NULLABLE, >= 0 | Employee count |
| `BillingCountry` | STRING | NULLABLE | Billing country |
| `BillingState` | STRING | NULLABLE | Billing state/province |
| `BillingCity` | STRING | NULLABLE | Billing city |
| `Phone` | STRING | NULLABLE | Primary phone number |
| `Website` | STRING | NULLABLE | Company website URL |
| `Rating` | STRING | NULLABLE | Account rating |
| `AccountSource` | STRING | NULLABLE | Lead source |
| `CreatedDate` | TIMESTAMP | NOT NULL | Account creation date |
| `LastModifiedDate` | TIMESTAMP | NOT NULL | Last modification date |

#### Metadata Fields
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `_source_system` | STRING | Source system identifier |
| `_ingestion_ts` | TIMESTAMP | Data ingestion timestamp |
| `_job_id` | STRING | ETL job identifier |

#### Data Quality Rules
1. **Primary Key Constraint**: `Id` must be unique and non-null
2. **Business Rule**: `AnnualRevenue` must be >= 0 if not null
3. **Business Rule**: `NumberOfEmployees` must be >= 0 if not null
4. **Freshness Check**: `LastModifiedDate` must be within last 7 days
5. **Referential Integrity**: Account must exist before associated contacts/opportunities

### CRM Contacts (`crm_contacts_bronze`)

**Source**: Salesforce API / CRM System  
**Target**: `s3://company-data-lake-ACCOUNT_ID/bronze/crm/contacts/`  
**Format**: Delta Lake (Parquet)

#### Required Fields
| Field Name | Data Type | Constraints | Description |
|------------|-----------|-------------|-------------|
| `Id` | STRING | NOT NULL, PRIMARY KEY | Unique contact identifier |
| `AccountId` | STRING | NOT NULL, FOREIGN KEY | Reference to Account.Id |
| `FirstName` | STRING | NULLABLE | Contact first name |
| `LastName` | STRING | NOT NULL | Contact last name |
| `Email` | STRING | NULLABLE | Email address |
| `Phone` | STRING | NULLABLE | Primary phone number |
| `MobilePhone` | STRING | NULLABLE | Mobile phone number |
| `Title` | STRING | NULLABLE | Job title |
| `Department` | STRING | NULLABLE | Department |
| `LeadSource` | STRING | NULLABLE | Lead source |
| `CreatedDate` | TIMESTAMP | NOT NULL | Contact creation date |
| `LastModifiedDate` | TIMESTAMP | NOT NULL | Last modification date |

#### Metadata Fields
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `_source_system` | STRING | Source system identifier |
| `_ingestion_ts` | TIMESTAMP | Data ingestion timestamp |
| `_job_id` | STRING | ETL job identifier |

#### Data Quality Rules
1. **Primary Key Constraint**: `Id` must be unique and non-null
2. **Foreign Key Constraint**: `AccountId` must reference valid Account.Id
3. **Business Rule**: `LastName` must be non-null (Salesforce requirement)
4. **Email Validation**: `Email` must be valid email format if not null
5. **Freshness Check**: `LastModifiedDate` must be within last 7 days

### CRM Opportunities (`crm_opportunities_bronze`)

**Source**: Salesforce API / CRM System  
**Target**: `s3://company-data-lake-ACCOUNT_ID/bronze/crm/opportunities/`  
**Format**: Delta Lake (Parquet)

#### Required Fields
| Field Name | Data Type | Constraints | Description |
|------------|-----------|-------------|-------------|
| `Id` | STRING | NOT NULL, PRIMARY KEY | Unique opportunity identifier |
| `Name` | STRING | NOT NULL | Opportunity name |
| `AccountId` | STRING | NOT NULL, FOREIGN KEY | Reference to Account.Id |
| `StageName` | STRING | NOT NULL | Sales stage |
| `CloseDate` | DATE | NOT NULL | Expected close date |
| `Amount` | DOUBLE | NOT NULL, > 0 | Opportunity amount |
| `Probability` | INTEGER | NULLABLE, 0-100 | Win probability percentage |
| `LeadSource` | STRING | NULLABLE | Lead source |
| `Type` | STRING | NULLABLE | Opportunity type |
| `IsClosed` | BOOLEAN | NOT NULL | Whether opportunity is closed |
| `IsWon` | BOOLEAN | NOT NULL | Whether opportunity was won |
| `CreatedDate` | TIMESTAMP | NOT NULL | Opportunity creation date |
| `LastModifiedDate` | TIMESTAMP | NOT NULL | Last modification date |

#### Metadata Fields
| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `_source_system` | STRING | Source system identifier |
| `_ingestion_ts` | TIMESTAMP | Data ingestion timestamp |
| `_job_id` | STRING | ETL job identifier |

#### Data Quality Rules
1. **Primary Key Constraint**: `Id` must be unique and non-null
2. **Foreign Key Constraint**: `AccountId` must reference valid Account.Id
3. **Business Rule**: `Amount` must be > 0
4. **Business Rule**: `Probability` must be between 0 and 100 if not null
5. **Business Rule**: `CloseDate` must be >= `CreatedDate`
6. **Freshness Check**: `LastModifiedDate` must be within last 7 days

## Silver Layer Schemas

### Dimension Tables

#### Dim Accounts (`dim_accounts`)
**Source**: `crm_accounts_bronze`  
**Target**: `s3://company-data-lake-ACCOUNT_ID/silver/crm/dim_accounts/`

**Enrichments**:
- `CustomerSegment`: Derived from `AnnualRevenue` (Enterprise/Mid-Market/SMB)
- `GeographicRegion`: Derived from `BillingCountry`
- `AccountStatus`: Active/Inactive based on recent activity

#### Dim Contacts (`dim_contacts`)
**Source**: `crm_contacts_bronze`  
**Target**: `s3://company-data-lake-ACCOUNT_ID/silver/crm/dim_contacts/`

**Enrichments**:
- `ContactLevel`: Derived from `Title` (Executive/Manager/Individual)
- `EngagementScore`: Calculated based on activity

### Fact Tables

#### Fact Opportunities (`fact_opportunities`)
**Source**: `crm_opportunities_bronze`  
**Target**: `s3://company-data-lake-ACCOUNT_ID/silver/crm/fact_opportunities/`

**Enrichments**:
- `DealSize`: Derived from `Amount` (Large/Medium/Small)
- `SalesCycle`: Calculated days from creation to close
- `ProductInterest`: Derived from opportunity details

## Gold Layer Schemas

### Analytics Tables

#### Revenue by Industry (`revenue_by_industry`)
**Source**: `fact_opportunities` + `dim_accounts`  
**Target**: `s3://company-data-lake-ACCOUNT_ID/gold/crm/revenue_by_industry/`

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `Industry` | STRING | Industry name |
| `total_revenue` | DOUBLE | Sum of opportunity amounts |
| `opportunity_count` | INTEGER | Count of opportunities |

#### Revenue by Geography (`revenue_by_geography`)
**Source**: `fact_opportunities` + `dim_accounts`  
**Target**: `s3://company-data-lake-ACCOUNT_ID/gold/crm/revenue_by_geography/`

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `GeographicRegion` | STRING | Geographic region |
| `total_revenue` | DOUBLE | Sum of opportunity amounts |
| `opportunity_count` | INTEGER | Count of opportunities |

#### Customer Segmentation (`customer_segmentation`)
**Source**: `fact_opportunities` + `dim_accounts`  
**Target**: `s3://company-data-lake-ACCOUNT_ID/gold/crm/customer_segmentation/`

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| `CustomerSegment` | STRING | Customer segment |
| `total_revenue` | DOUBLE | Sum of opportunity amounts |
| `avg_deal_size` | DOUBLE | Average deal size |
| `deal_count` | INTEGER | Count of deals |

## Data Quality Monitoring

### Bronze Layer DQ Checks
- **Freshness**: Data must be ingested within SLA (daily for CRM)
- **Volume**: Record count must be within expected range (±10%)
- **Completeness**: Required fields must be non-null
- **Referential Integrity**: Foreign keys must reference valid records

### Silver Layer DQ Checks
- **Business Rules**: Enrichment logic validation
- **Data Consistency**: Cross-table consistency checks
- **Duplicate Detection**: Identify and handle duplicates

### Gold Layer DQ Checks
- **Aggregation Accuracy**: Verify sum/count calculations
- **Trend Analysis**: Detect unusual patterns
- **Data Freshness**: Ensure analytics are current

## Compliance Notes

### PII Handling
- **Email addresses**: Masked in non-production environments
- **Phone numbers**: Masked in non-production environments
- **Names**: Available for business use, logged for audit

### Data Retention
- **Bronze**: 7 years (regulatory requirement)
- **Silver**: 3 years (business requirement)
- **Gold**: 1 year (analytics requirement)

### Access Control
- **Bronze**: Data Engineers only
- **Silver**: Data Engineers + Data Analysts
- **Gold**: All authorized users

### Audit Trail
- All data transformations logged with timestamps
- Schema changes tracked in version control
- Data lineage documented and maintained

---

*This document is maintained by the Data Engineering team and reviewed quarterly for accuracy and compliance.*
