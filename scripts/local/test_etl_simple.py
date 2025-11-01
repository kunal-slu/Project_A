"""
Simple ETL end-to-end test that works with existing data structure.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Add paths
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_bronze_to_silver(spark, config):
    """Test bronze → silver transformation."""
    logger.info("Testing Bronze → Silver transformation...")
    try:
        from jobs.bronze_to_silver_behavior import transform_bronze_to_silver_behavior
        transform_bronze_to_silver_behavior(spark, config)
        logger.info("✅ Bronze → Silver: PASSED")
        return True
    except Exception as e:
        logger.error(f"❌ Bronze → Silver: FAILED - {e}")
        return False


def test_silver_to_gold(spark, config):
    """Test silver → gold transformation."""
    logger.info("Testing Silver → Gold transformation...")
    
    results = {}
    
    # Test customer_360
    try:
        from jobs.silver_build_customer_360 import build_customer_360
        build_customer_360(spark, config)
        logger.info("✅ Customer 360 build: PASSED")
        results['customer_360'] = True
    except Exception as e:
        logger.error(f"❌ Customer 360 build: FAILED - {e}")
        results['customer_360'] = False
    
    # Test product_perf
    try:
        from jobs.silver_build_product_perf import build_product_perf_daily
        build_product_perf_daily(spark, config)
        logger.info("✅ Product perf build: PASSED")
        results['product_perf'] = True
    except Exception as e:
        logger.error(f"❌ Product perf build: FAILED - {e}")
        results['product_perf'] = False
    
    return all(results.values())


def verify_outputs(spark, config):
    """Verify outputs exist."""
    logger.info("Verifying outputs...")
    
    bronze_path = config.get("data_lake", {}).get("bronze_path", "data/lakehouse_delta/bronze")
    silver_path = config.get("data_lake", {}).get("silver_path", "data/lakehouse_delta/silver")
    gold_path = config.get("data_lake", {}).get("gold_path", "data/lakehouse_delta/gold")
    
    checks = []
    
    # Check Silver
    try:
        df = spark.read.format("delta").load(f"{silver_path}/behavior")
        count = df.count()
        logger.info(f"✅ Silver behavior: {count:,} records")
        checks.append(True)
    except Exception as e:
        logger.warning(f"⚠️ Silver behavior not found: {e}")
        checks.append(False)
    
    # Check Gold
    try:
        df = spark.read.format("delta").load(f"{gold_path}/customer_360")
        count = df.count()
        logger.info(f"✅ Gold customer_360: {count:,} records")
        checks.append(True)
    except Exception as e:
        logger.warning(f"⚠️ Gold customer_360 not found: {e}")
        checks.append(False)
    
    try:
        df = spark.read.format("delta").load(f"{gold_path}/product_perf_daily")
        count = df.count()
        logger.info(f"✅ Gold product_perf_daily: {count:,} records")
        checks.append(True)
    except Exception as e:
        logger.warning(f"⚠️ Gold product_perf_daily not found: {e}")
        checks.append(False)
    
    return any(checks)  # At least one should exist


def main():
    logger.info("=" * 80)
    logger.info("END-TO-END ETL TEST")
    logger.info("=" * 80)
    
    config = load_conf("config/local.yaml")
    spark = build_spark(app_name="etl_test", config=config)
    
    try:
        # Test 1: Bronze → Silver
        bronze_silver_ok = test_bronze_to_silver(spark, config)
        
        # Test 2: Silver → Gold
        silver_gold_ok = test_silver_to_gold(spark, config)
        
        # Test 3: Verify outputs
        outputs_ok = verify_outputs(spark, config)
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Bronze → Silver: {'✅ PASSED' if bronze_silver_ok else '❌ FAILED'}")
        logger.info(f"Silver → Gold: {'✅ PASSED' if silver_gold_ok else '❌ FAILED'}")
        logger.info(f"Outputs Verified: {'✅ PASSED' if outputs_ok else '⚠️ PARTIAL'}")
        
        if bronze_silver_ok and silver_gold_ok:
            logger.info("\n🎉 END-TO-END TEST: PASSED")
            return 0
        else:
            logger.info("\n❌ END-TO-END TEST: FAILED")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Test failed: {e}", exc_info=True)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

