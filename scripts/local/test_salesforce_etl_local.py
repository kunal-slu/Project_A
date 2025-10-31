#!/usr/bin/env python3
"""
Local Salesforce ETL Pipeline Test

This script tests the complete Salesforce ETL pipeline locally:
1. Extract Salesforce data (Accounts, Contacts, Leads, Solutions)
2. Validate schema contracts
3. Run data quality checks
4. Simulate Bronze → Silver → Gold transformations
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.extract.salesforce_accounts import extract_salesforce_accounts
from pyspark_interview_project.extract.salesforce_contacts import extract_salesforce_contacts
from pyspark_interview_project.extract.salesforce_leads import extract_salesforce_leads
from pyspark_interview_project.extract.salesforce_solutions import extract_salesforce_solutions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_salesforce_extraction(spark, config: Dict[str, Any]) -> bool:
    """Test Salesforce data extraction."""
    logger.info("🚀 Testing Salesforce data extraction...")
    
    try:
        # Test each Salesforce object
        objects_to_test = [
            ('accounts', extract_salesforce_accounts),
            ('contacts', extract_salesforce_contacts),
            ('leads', extract_salesforce_leads),
            ('solutions', extract_salesforce_solutions)
        ]
        
        results = {}
        
        for obj_name, extract_func in objects_to_test:
            try:
                logger.info(f"📊 Testing {obj_name} extraction...")
                df = extract_func(spark, config, obj_name)
                
                count = df.count()
                results[obj_name] = {
                    'success': True,
                    'count': count,
                    'columns': len(df.columns)
                }
                
                logger.info(f"✅ {obj_name}: {count} records, {len(df.columns)} columns")
                
                # Show sample data
                if count > 0:
                    sample = df.limit(3).toPandas()
                    logger.info(f"   Sample: {sample.iloc[0].to_dict()}")
                
            except Exception as e:
                logger.error(f"❌ {obj_name} extraction failed: {str(e)}")
                results[obj_name] = {
                    'success': False,
                    'error': str(e)
                }
        
        # Check overall success
        successful_extractions = sum(1 for r in results.values() if r.get('success', False))
        total_extractions = len(objects_to_test)
        
        logger.info(f"🎯 Extraction results: {successful_extractions}/{total_extractions} successful")
        
        return successful_extractions == total_extractions
        
    except Exception as e:
        logger.error(f"❌ Salesforce extraction test failed: {str(e)}")
        return False


def test_data_quality(spark, config: Dict[str, Any]) -> bool:
    """Test data quality checks."""
    logger.info("🔍 Testing data quality checks...")
    
    try:
        # Test contacts data quality (most important for CRM)
        contacts_df = extract_salesforce_contacts(spark, config, 'contacts')
        
        if contacts_df.count() == 0:
            logger.error("❌ No contacts data to test")
            return False
        
        # Check required fields
        required_fields = ['contact_id', 'first_name', 'last_name', 'email']
        
        for field in required_fields:
            null_count = contacts_df.filter(contacts_df[field].isNull()).count()
            total_count = contacts_df.count()
            
            if null_count > 0:
                logger.warning(f"⚠️ {field}: {null_count}/{total_count} null values")
            else:
                logger.info(f"✅ {field}: No null values")
        
        # Check email format (basic validation)
        email_count = contacts_df.filter(contacts_df['email'].rlike('^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')).count()
        total_count = contacts_df.count()
        
        if email_count == total_count:
            logger.info(f"✅ Email format: All {total_count} emails valid")
        else:
            logger.warning(f"⚠️ Email format: {email_count}/{total_count} valid emails")
        
        logger.info("✅ Data quality checks completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data quality test failed: {str(e)}")
        return False


def test_bronze_to_silver_transformation(spark, config: Dict[str, Any]) -> bool:
    """Test Bronze to Silver transformation."""
    logger.info("🔄 Testing Bronze to Silver transformation...")
    
    try:
        # Extract and clean contacts data
        contacts_df = extract_salesforce_contacts(spark, config, 'contacts')
        
        if contacts_df.count() == 0:
            logger.error("❌ No contacts data for transformation")
            return False
        
        # Simulate Silver layer transformation
        silver_contacts = contacts_df.select(
            'contact_id',
            'first_name',
            'last_name',
            'email',
            'phone',
            'title',
            'department',
            'account_id',
            'lead_source',
            'created_date',
            'last_modified_date',
            'owner_id',
            'active'
        ).filter(contacts_df['active'] == True)  # Only active contacts
        
        silver_count = silver_contacts.count()
        bronze_count = contacts_df.count()
        
        logger.info(f"✅ Silver transformation: {silver_count}/{bronze_count} active contacts")
        
        # Show sample Silver data
        if silver_count > 0:
            sample = silver_contacts.limit(3).toPandas()
            logger.info(f"   Sample Silver: {sample.iloc[0]['first_name']} {sample.iloc[0]['last_name']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Bronze to Silver transformation failed: {str(e)}")
        return False


def test_silver_to_gold_transformation(spark, config: Dict[str, Any]) -> bool:
    """Test Silver to Gold transformation."""
    logger.info("💎 Testing Silver to Gold transformation...")
    
    try:
        # Extract accounts and contacts for Gold layer
        accounts_df = extract_salesforce_accounts(spark, config, 'accounts')
        contacts_df = extract_salesforce_contacts(spark, config, 'contacts')
        
        if accounts_df.count() == 0 or contacts_df.count() == 0:
            logger.error("❌ No data for Gold transformation")
            return False
        
        # Create Gold layer: Customer dimension
        gold_customers = contacts_df.join(
            accounts_df,
            contacts_df['account_id'] == accounts_df['account_id'],
            'inner'
        ).select(
            contacts_df['contact_id'],
            contacts_df['first_name'],
            contacts_df['last_name'],
            contacts_df['email'],
            contacts_df['phone'],
            contacts_df['title'],
            contacts_df['department'],
            accounts_df['account_name'],
            accounts_df['industry'],
            accounts_df['annual_revenue'],
            accounts_df['number_of_employees'],
            accounts_df['billing_city'],
            accounts_df['billing_state'],
            accounts_df['billing_country']
        ).filter(contacts_df['active'] == True)
        
        gold_count = gold_customers.count()
        
        logger.info(f"✅ Gold transformation: {gold_count} customer records")
        
        # Show sample Gold data
        if gold_count > 0:
            sample = gold_customers.limit(3).toPandas()
            logger.info(f"   Sample Gold: {sample.iloc[0]['first_name']} {sample.iloc[0]['last_name']} at {sample.iloc[0]['account_name']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Silver to Gold transformation failed: {str(e)}")
        return False


def main():
    """Main test execution."""
    try:
        logger.info("🚀 Starting Salesforce ETL Pipeline Test")
        logger.info("=" * 60)
        
        # Load configuration
        config_path = 'config/dev.yaml'
        config = load_conf(config_path)
        config['environment'] = 'local'  # Force local mode
        
        logger.info(f"⚙️ Using config: {config_path}")
        
        # Build Spark session
        spark = build_spark(
            app_name="SalesforceETLTest",
            config=config
        )
        
        # Run tests
        tests = [
            ("Salesforce Extraction", test_salesforce_extraction),
            ("Data Quality", test_data_quality),
            ("Bronze to Silver", test_bronze_to_silver_transformation),
            ("Silver to Gold", test_silver_to_gold_transformation)
        ]
        
        results = {}
        
        for test_name, test_func in tests:
            logger.info(f"\n🧪 Running {test_name} test...")
            try:
                success = test_func(spark, config)
                results[test_name] = success
                
                if success:
                    logger.info(f"✅ {test_name} test PASSED")
                else:
                    logger.error(f"❌ {test_name} test FAILED")
                    
            except Exception as e:
                logger.error(f"💥 {test_name} test ERROR: {str(e)}")
                results[test_name] = False
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("🎯 SALESFORCE ETL PIPELINE TEST SUMMARY")
        logger.info("=" * 60)
        
        passed_tests = sum(1 for success in results.values() if success)
        total_tests = len(results)
        
        for test_name, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            logger.info(f"{status} {test_name}")
        
        logger.info(f"\n📊 Overall: {passed_tests}/{total_tests} tests passed")
        
        if passed_tests == total_tests:
            logger.info("🎉 ALL TESTS PASSED! Salesforce ETL pipeline is ready!")
            return True
        else:
            logger.error("💥 Some tests failed. Please check the logs above.")
            return False
            
    except Exception as e:
        logger.error(f"💥 Fatal error: {str(e)}")
        return False
    
    finally:
        # Clean up Spark session
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
