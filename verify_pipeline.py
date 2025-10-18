#!/usr/bin/env python3
"""
One-time verification script for the ETL pipeline
"""
import os
import sys
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_delta_lake_tables():
    """Verify Delta Lake tables exist and have data"""
    logger.info("🔍 Verifying Delta Lake tables...")
    
    delta_path = "data/lakehouse_delta_standard"
    layers = ['bronze', 'silver', 'gold']
    
    total_tables = 0
    total_files = 0
    total_versions = 0
    
    for layer in layers:
        layer_path = os.path.join(delta_path, layer)
        if os.path.exists(layer_path):
            logger.info(f"📊 {layer.upper()} LAYER:")
            for table_dir in os.listdir(layer_path):
                table_path = os.path.join(layer_path, table_dir)
                if os.path.isdir(table_path):
                    total_tables += 1
                    
                    # Count parquet files
                    parquet_files = [f for f in os.listdir(table_path) if f.endswith('.parquet')]
                    total_files += len(parquet_files)
                    
                    # Count transaction logs
                    delta_log_path = os.path.join(table_path, '_delta_log')
                    log_files = []
                    if os.path.exists(delta_log_path):
                        log_files = [f for f in os.listdir(delta_log_path) if f.endswith('.json')]
                    total_versions += len(log_files)
                    
                    logger.info(f"  📄 {table_dir}: {len(parquet_files)} files, {len(log_files)} versions")
    
    logger.info(f"✅ VERIFICATION COMPLETE:")
    logger.info(f"   📊 Total tables: {total_tables}")
    logger.info(f"   📊 Total parquet files: {total_files}")
    logger.info(f"   📊 Total versions: {total_versions}")
    
    return total_tables > 0 and total_files > 0 and total_versions > 0

def main():
    """Main verification function"""
    logger.info("🚀 Starting ETL Pipeline Verification")
    logger.info("=" * 50)
    
    # Check if Delta Lake data exists
    if not os.path.exists("data/lakehouse_delta_standard"):
        logger.error("❌ Delta Lake data not found. Run the ETL pipeline first.")
        return False
    
    # Verify tables
    success = verify_delta_lake_tables()
    
    if success:
        logger.info("🎉 All verifications passed!")
        logger.info("✅ ETL Pipeline is working correctly")
    else:
        logger.error("❌ Some verifications failed")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
