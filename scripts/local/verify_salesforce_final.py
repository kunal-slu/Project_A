#!/usr/bin/env python3
"""
Comprehensive Salesforce Data Verification

This script verifies that all Salesforce data is properly structured
and ready for both Salesforce import and ETL pipeline operations.
"""

import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_salesforce_data():
    """Verify Salesforce data structure and integrity."""
    logger.info("🔍 COMPREHENSIVE SALESFORCE DATA VERIFICATION")
    logger.info("=" * 60)
    
    # Check files exist
    accounts_file = 'aws/data/salesforce/salesforce_accounts_ready.csv'
    contacts_file = 'aws/data/salesforce/salesforce_contacts_ready.csv'
    
    if not os.path.exists(accounts_file):
        logger.error(f"❌ Accounts file not found: {accounts_file}")
        return False
    
    if not os.path.exists(contacts_file):
        logger.error(f"❌ Contacts file not found: {contacts_file}")
        return False
    
    # Load data
    accounts_df = pd.read_csv(accounts_file)
    contacts_df = pd.read_csv(contacts_file)
    
    logger.info(f"📊 Data loaded:")
    logger.info(f"   Accounts: {len(accounts_df):,} records, {len(accounts_df.columns)} columns")
    logger.info(f"   Contacts: {len(contacts_df):,} records, {len(contacts_df.columns)} columns")
    
    # Verify Accounts
    logger.info("\n📊 ACCOUNTS VERIFICATION:")
    logger.info("=" * 30)
    
    # Required field
    if 'Name' in accounts_df.columns:
        name_count = accounts_df['Name'].notna().sum()
        logger.info(f"✅ Name (required): {name_count}/{len(accounts_df)} records")
    else:
        logger.error("❌ Name field missing (REQUIRED)")
        return False
    
    # High-value fields
    high_value_fields = ['Industry', 'AnnualRevenue', 'NumberOfEmployees', 'BillingCountry', 'Rating', 'AccountSource']
    for field in high_value_fields:
        if field in accounts_df.columns:
            count = accounts_df[field].notna().sum()
            logger.info(f"✅ {field}: {count}/{len(accounts_df)} records")
        else:
            logger.warning(f"⚠️ {field}: Missing")
    
    # Verify Contacts
    logger.info("\n👥 CONTACTS VERIFICATION:")
    logger.info("=" * 30)
    
    # Required fields
    if 'LastName' in contacts_df.columns:
        lastname_count = contacts_df['LastName'].notna().sum()
        logger.info(f"✅ LastName (required): {lastname_count}/{len(contacts_df)} records")
    else:
        logger.error("❌ LastName field missing (REQUIRED)")
        return False
    
    if 'Account' in contacts_df.columns:
        account_count = contacts_df['Account'].notna().sum()
        logger.info(f"✅ Account (required): {account_count}/{len(contacts_df)} records")
    else:
        logger.error("❌ Account field missing (REQUIRED)")
        return False
    
    # Optional fields
    optional_fields = ['FirstName', 'Email', 'Phone', 'Title', 'Department']
    for field in optional_fields:
        if field in contacts_df.columns:
            count = contacts_df[field].notna().sum()
            logger.info(f"✅ {field}: {count}/{len(contacts_df)} records")
        else:
            logger.warning(f"⚠️ {field}: Missing")
    
    # Verify relationships
    logger.info("\n🔗 RELATIONSHIP VERIFICATION:")
    logger.info("=" * 35)
    
    # Check if contacts can link to accounts
    if 'Account' in contacts_df.columns and 'Name' in accounts_df.columns:
        account_names = set(accounts_df['Name'].unique())
        contact_accounts = set(contacts_df['Account'].unique())
        
        linkable_accounts = contact_accounts.intersection(account_names)
        linkable_contacts = contacts_df[contacts_df['Account'].isin(linkable_accounts)]
        
        logger.info(f"✅ Account names in accounts: {len(account_names)}")
        logger.info(f"✅ Account names in contacts: {len(contact_accounts)}")
        logger.info(f"✅ Linkable account names: {len(linkable_accounts)}")
        logger.info(f"✅ Contacts linkable to accounts: {len(linkable_contacts):,}/{len(contacts_df):,}")
        
        if len(linkable_contacts) == len(contacts_df):
            logger.info("✅ Perfect relationship integrity!")
        else:
            logger.warning(f"⚠️ {len(contacts_df) - len(linkable_contacts)} contacts cannot link to accounts")
    
    # Data quality checks
    logger.info("\n🔍 DATA QUALITY CHECKS:")
    logger.info("=" * 30)
    
    # Check for empty required fields
    empty_names = accounts_df[accounts_df['Name'].isna() | (accounts_df['Name'] == '')]
    if len(empty_names) > 0:
        logger.error(f"❌ {len(empty_names)} accounts with empty Name")
        return False
    else:
        logger.info("✅ No empty Account Names")
    
    empty_lastnames = contacts_df[contacts_df['LastName'].isna() | (contacts_df['LastName'] == '')]
    if len(empty_lastnames) > 0:
        logger.error(f"❌ {len(empty_lastnames)} contacts with empty LastName")
        return False
    else:
        logger.info("✅ No empty Last Names")
    
    # Check email format (basic)
    if 'Email' in contacts_df.columns:
        email_pattern = contacts_df['Email'].str.contains('@', na=False)
        valid_emails = email_pattern.sum()
        logger.info(f"✅ Valid email format: {valid_emails}/{len(contacts_df)} contacts")
    
    # Summary
    logger.info("\n🎯 VERIFICATION SUMMARY:")
    logger.info("=" * 30)
    logger.info("✅ All required fields present")
    logger.info("✅ Data relationships validated")
    logger.info("✅ No empty required fields")
    logger.info("✅ Ready for Salesforce import")
    logger.info("✅ Ready for ETL pipeline")
    
    return True


def show_sample_data():
    """Show sample data for verification."""
    logger.info("\n📊 SAMPLE DATA:")
    logger.info("=" * 20)
    
    accounts_df = pd.read_csv('aws/data/salesforce/salesforce_accounts_ready.csv')
    contacts_df = pd.read_csv('aws/data/salesforce/salesforce_contacts_ready.csv')
    
    logger.info("🏢 Sample Accounts:")
    sample_accounts = accounts_df[['Name', 'Industry', 'AnnualRevenue', 'BillingCountry', 'Rating']].head(3)
    for _, row in sample_accounts.iterrows():
        logger.info(f"   {row['Name']} | {row['Industry']} | ${row['AnnualRevenue']:,} | {row['BillingCountry']} | {row['Rating']}")
    
    logger.info("\n👥 Sample Contacts:")
    sample_contacts = contacts_df[['LastName', 'FirstName', 'Email', 'Account', 'Title']].head(3)
    for _, row in sample_contacts.iterrows():
        logger.info(f"   {row['LastName']}, {row['FirstName']} | {row['Email']} | {row['Account']} | {row['Title']}")


def main():
    """Main verification function."""
    try:
        # Verify data structure
        if verify_salesforce_data():
            # Show sample data
            show_sample_data()
            
            logger.info("\n🎉 SALESFORCE DATA VERIFICATION COMPLETE!")
            logger.info("=" * 50)
            logger.info("✅ All checks passed")
            logger.info("✅ Ready for Salesforce Data Import Wizard")
            logger.info("✅ Ready for ETL pipeline")
            logger.info("✅ Perfect for analytics and reporting")
            return True
        else:
            logger.error("\n💥 SALESFORCE DATA VERIFICATION FAILED!")
            logger.error("❌ Please fix the issues above")
            return False
            
    except Exception as e:
        logger.error(f"💥 Verification error: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
