#!/usr/bin/env python3
"""
Simple Salesforce Data Verification Test

This script verifies that the Salesforce data files are properly structured
and can be used for ETL operations.
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
    """Verify Salesforce data files."""
    logger.info("🚀 Starting Salesforce Data Verification")
    logger.info("=" * 50)
    
    # Define Salesforce files to check
    salesforce_files = [
        'aws/data/salesforce/salesforce_accounts.csv',
        'aws/data/salesforce/salesforce_contacts.csv',
        'aws/data/salesforce/salesforce_leads.csv',
        'aws/data/salesforce/salesforce_solutions.csv'
    ]
    
    results = {}
    
    for file_path in salesforce_files:
        try:
            logger.info(f"📊 Checking {file_path}...")
            
            # Check if file exists
            if not os.path.exists(file_path):
                logger.error(f"❌ File not found: {file_path}")
                results[file_path] = {'success': False, 'error': 'File not found'}
                continue
            
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Basic checks
            record_count = len(df)
            column_count = len(df.columns)
            file_size = os.path.getsize(file_path)
            
            results[file_path] = {
                'success': True,
                'records': record_count,
                'columns': column_count,
                'file_size': file_size
            }
            
            logger.info(f"✅ {record_count:,} records, {column_count} columns, {file_size:,} bytes")
            
            # Show sample data
            if record_count > 0:
                sample = df.iloc[0]
                logger.info(f"   Sample: {sample.to_dict()}")
            
            # Specific checks for each file type
            if 'contacts' in file_path:
                # Check for required last_name column
                if 'last_name' in df.columns:
                    logger.info("✅ Contacts has required last_name column")
                else:
                    logger.error("❌ Contacts missing last_name column")
                    results[file_path]['success'] = False
                    results[file_path]['error'] = 'Missing last_name column'
            
            elif 'accounts' in file_path:
                # Check for account_id
                if 'account_id' in df.columns:
                    logger.info("✅ Accounts has account_id column")
                else:
                    logger.error("❌ Accounts missing account_id column")
                    results[file_path]['success'] = False
                    results[file_path]['error'] = 'Missing account_id column'
            
            elif 'leads' in file_path:
                # Check for lead_id
                if 'lead_id' in df.columns:
                    logger.info("✅ Leads has lead_id column")
                else:
                    logger.error("❌ Leads missing lead_id column")
                    results[file_path]['success'] = False
                    results[file_path]['error'] = 'Missing lead_id column'
            
            elif 'solutions' in file_path:
                # Check for solution_id
                if 'solution_id' in df.columns:
                    logger.info("✅ Solutions has solution_id column")
                else:
                    logger.error("❌ Solutions missing solution_id column")
                    results[file_path]['success'] = False
                    results[file_path]['error'] = 'Missing solution_id column'
            
        except Exception as e:
            logger.error(f"❌ Error reading {file_path}: {str(e)}")
            results[file_path] = {'success': False, 'error': str(e)}
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("🎯 SALESFORCE DATA VERIFICATION SUMMARY")
    logger.info("=" * 50)
    
    successful_files = sum(1 for r in results.values() if r.get('success', False))
    total_files = len(salesforce_files)
    
    for file_path, result in results.items():
        filename = os.path.basename(file_path)
        if result.get('success', False):
            logger.info(f"✅ {filename}: {result['records']:,} records")
        else:
            logger.error(f"❌ {filename}: {result.get('error', 'Unknown error')}")
    
    logger.info(f"\n📊 Overall: {successful_files}/{total_files} files verified successfully")
    
    if successful_files == total_files:
        logger.info("🎉 ALL SALESFORCE DATA FILES VERIFIED!")
        logger.info("✅ Ready for ETL pipeline")
        return True
    else:
        logger.error("💥 Some files failed verification")
        return False


def test_data_relationships():
    """Test data relationships for joins."""
    logger.info("\n🔗 Testing Data Relationships...")
    
    try:
        # Load data
        accounts_df = pd.read_csv('aws/data/salesforce/salesforce_accounts.csv')
        contacts_df = pd.read_csv('aws/data/salesforce/salesforce_contacts.csv')
        
        # Check account_id relationship
        if 'account_id' in contacts_df.columns and 'account_id' in accounts_df.columns:
            # Find contacts that can join with accounts
            joinable_contacts = contacts_df[contacts_df['account_id'].isin(accounts_df['account_id'])]
            total_contacts = len(contacts_df)
            joinable_count = len(joinable_contacts)
            
            logger.info(f"✅ {joinable_count:,}/{total_contacts:,} contacts can join with accounts")
            
            if joinable_count > 0:
                logger.info("✅ Data relationships are valid for ETL joins")
                return True
            else:
                logger.warning("⚠️ No contacts can join with accounts")
                return False
        else:
            logger.error("❌ Missing account_id columns for relationship testing")
            return False
            
    except Exception as e:
        logger.error(f"❌ Relationship test failed: {str(e)}")
        return False


def main():
    """Main verification function."""
    try:
        # Verify data files
        data_verified = verify_salesforce_data()
        
        # Test relationships
        relationships_ok = test_data_relationships()
        
        # Final result
        logger.info("\n" + "=" * 50)
        logger.info("🎯 FINAL VERIFICATION RESULT")
        logger.info("=" * 50)
        
        if data_verified and relationships_ok:
            logger.info("🎉 SALESFORCE DATA IS READY FOR ETL!")
            logger.info("✅ All files verified")
            logger.info("✅ Data relationships valid")
            logger.info("✅ Ready for Bronze → Silver → Gold pipeline")
            return True
        else:
            logger.error("💥 Salesforce data verification failed")
            return False
            
    except Exception as e:
        logger.error(f"💥 Fatal error: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
