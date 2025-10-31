# CRM Data Dictionary

This document defines the CRM (Salesforce) data structure and field mappings used in the data platform.

## Overview

CRM data is ingested from Salesforce and consists of three main objects:
- **Accounts**: Company/organization records
- **Contacts**: Individual person records
- **Opportunities**: Sales opportunity records

## Accounts

**Object Name**: `crm_accounts`  
**Schema File**: `config/schema_definitions/crm_accounts.schema.json`

### Required Fields
- `Id` (string): Unique identifier for the account
- `Name` (string): Account name
- `CreatedDate` (timestamp): Record creation date

### Key Fields
- `Type` (string): Account type (e.g., "Customer", "Prospect", "Partner")
- `Industry` (string): Industry classification
- `AnnualRevenue` (double): Annual revenue in USD
- `NumberOfEmployees` (integer): Employee count
- `ParentId` (string): Parent account ID (for hierarchy)
- `Rating` (string): Account rating (e.g., "Hot", "Warm", "Cold")
- `Phone` (string): **PII** - Account phone number
- `Website` (string): Account website URL

### Address Fields
- `BillingStreet`, `BillingCity`, `BillingState`, `BillingPostalCode`, `BillingCountry`
- `ShippingStreet`, `ShippingCity`, `ShippingState`, `ShippingPostalCode`, `ShippingCountry`

### Timestamps
- `CreatedDate` (timestamp): Required
- `LastModifiedDate` (timestamp): Required

### Data Quality Rules
- `Id` and `Name` must not be null
- `AnnualRevenue` must be >= 0 if present
- `CreatedDate` must be in the past
- `LastModifiedDate` >= `CreatedDate`

### PII Fields
- `Phone`: Requires masking for non-authorized access
- `Fax`: Requires masking

---

## Contacts

**Object Name**: `crm_contacts`  
**Schema File**: `config/schema_definitions/crm_contacts.schema.json`

### Required Fields (Salesforce)
- `Id` (string): Unique identifier
- `LastName` (string): **REQUIRED BY SALESFORCE** - Contact last name
- `CreatedDate` (timestamp): Record creation date

### Key Fields
- `FirstName` (string): Contact first name
- `Email` (string): **PII** - Contact email address
- `Phone` (string): **PII** - Contact phone number
- `Title` (string): Job title
- `Department` (string): Department name
- `AccountId` (string): **FOREIGN KEY** - Links to `crm_accounts.Id`
- `AccountName` (string): Denormalized account name (added in Silver)
- `LeadSource` (string): Lead source (e.g., "Web", "Referral")

### Address Fields
- `MailingStreet`, `MailingCity`, `MailingState`, `MailingPostalCode`, `MailingCountry`
- `OtherStreet`, `OtherCity`, `OtherState`, `OtherPostalCode`, `OtherCountry`

### Timestamps
- `CreatedDate` (timestamp): Required
- `LastModifiedDate` (timestamp): Required

### Data Quality Rules
- `Id` and `LastName` must not be null
- `Email` must be valid format if present
- `AccountId` must reference valid account (referential integrity)
- `CreatedDate` must be in the past

### PII Fields
- `Email`: Requires hashing/masking
- `Phone`: Requires masking
- `MailingStreet`, `MailingCity`: Address PII

### Relationships
- **Many-to-One**: `Contacts` → `Accounts` (via `AccountId`)

---

## Opportunities

**Object Name**: `crm_opportunities`  
**Schema File**: `config/schema_definitions/crm_opportunities.schema.json`

### Required Fields (Salesforce)
- `Id` (string): Unique identifier
- `Name` (string): Opportunity name
- `StageName` (string): **REQUIRED** - Sales stage (e.g., "Qualification", "Closed Won")
- `CloseDate` (date): **REQUIRED** - Expected close date
- `CreatedDate` (timestamp): Record creation date

### Key Fields
- `AccountId` (string): **FOREIGN KEY** - Links to `crm_accounts.Id`
- `Amount` (double): Opportunity amount in USD
- `Probability` (double): Probability percentage (0-100)
- `Type` (string): Opportunity type
- `LeadSource` (string): Lead source
- `OwnerId` (string): Sales owner user ID
- `Description` (string): Opportunity description

### Timestamps
- `CreatedDate` (timestamp): Required
- `LastModifiedDate` (timestamp): Required

### Data Quality Rules
- `Id`, `Name`, `StageName`, `CloseDate` must not be null
- `Amount` must be >= 0 if present
- `Probability` must be between 0 and 100 if present
- `AccountId` must reference valid account (referential integrity)
- `CloseDate` must be a valid date

### Relationships
- **Many-to-One**: `Opportunities` → `Accounts` (via `AccountId`)

---

## Field Mappings

### Bronze → Silver

| Bronze Field | Silver Field | Transformation |
|-------------|-------------|----------------|
| `Id` | `account_id` / `contact_id` / `opportunity_id` | Rename |
| `Name` | `account_name` / `opportunity_name` | Rename |
| `FirstName` + `LastName` | `full_name` | Concatenate |
| `AccountId` | `account_id` | Join to get `account_name` |
| `Email` | `email_masked` | Hash for PII |
| `Phone` | `phone_masked` | Mask for PII |

### Silver → Gold

| Silver Source | Gold Target | Transformation |
|-------------|------------|---------------|
| `silver.contacts` | `gold.dim_customers` | SCD2 dimension |
| `silver.opportunities` + `silver.accounts` | `gold.fact_sales` | Join and aggregate |
| `silver.accounts` | `gold.dim_accounts` | Dimension table |

---

## Data Generation

Synthetic CRM data is generated using `scripts/local/generate_crm_synthetic.py`:

- **Accounts**: 10,000+ accounts with realistic industries and revenue
- **Contacts**: 50,000+ contacts with proper relationships to accounts
- **Opportunities**: 25,000+ opportunities with realistic stages and amounts

### Relationships
- Each contact links to one account (`AccountId`)
- Each opportunity links to one account (`AccountId`)
- Accounts can have multiple contacts and opportunities

---

**Last Updated**: 2024-01-15  
**Source System**: Salesforce  
**Maintained By**: Data Engineering Team

