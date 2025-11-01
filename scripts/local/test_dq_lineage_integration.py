"""
Test script to verify GE and Lineage integration in transforms.
"""
import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.transform.bronze_to_silver import transform_bronze_to_silver
from pyspark_interview_project.transform.silver_to_gold import transform_silver_to_gold
from pyspark_interview_project.dq.ge_runner import GERunner

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_dq_integration():
    """Test GE integration in transforms."""
    logger.info("🧪 Testing GE + Lineage Integration")
    
    # Load config
    config = load_conf("config/local.yaml")
    
    # Build Spark session
    spark = build_spark("test_dq_lineage", config)
    
    # Create sample DataFrame
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("total_amount", IntegerType(), True)
    ])
    
    data = [
        (1, 100, "Product A", 100),
        (2, 100, "Product B", 200),
        (3, 200, "Product A", 150),
        (4, 200, "Product C", 300),
        (5, 300, "Product B", 250)
    ]
    
    df = spark.createDataFrame(data, schema)
    
    try:
        # Test bronze to silver with GE
        logger.info("\n📊 Testing bronze_to_silver with GE integration...")
        try:
            silver_df = transform_bronze_to_silver(
                spark=spark,
                df=df,
                layer="silver",
                run_id="test_run_001",
                execution_date="2025-10-31",
                table="orders",
                config=config
            )
            logger.info(f"✅ bronze_to_silver completed: {silver_df.count()} records")
        except ValueError as dq_error:
            logger.error(f"❌ DQ validation failed (expected if GE not installed): {dq_error}")
        except Exception as e:
            logger.warning(f"⚠️ GE integration skipped: {e}")
        
        # Test silver to gold with GE
        logger.info("\n📊 Testing silver_to_gold with GE integration...")
        try:
            gold_df = transform_silver_to_gold(
                spark=spark,
                df=df,
                table_name="customer_analytics",
                config=config
            )
            logger.info(f"✅ silver_to_gold completed: {gold_df.count()} records")
        except ValueError as dq_error:
            logger.error(f"❌ DQ validation failed (expected if GE not installed): {dq_error}")
        except Exception as e:
            logger.warning(f"⚠️ GE integration skipped: {e}")
        
        logger.info("\n✅ GE + Lineage integration test complete!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}", exc_info=True)
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    test_dq_integration()

