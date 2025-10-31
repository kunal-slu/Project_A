"""
Main pipeline orchestration script.
"""

import sys
import os
import logging
import argparse
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pyspark.sql import SparkSession
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.watermark_utils import upsert_watermark
from pyspark_interview_project.pipeline.bronze_to_silver import bronze_to_silver
from pyspark_interview_project.pipeline.silver_to_gold import silver_to_gold
from pyspark_interview_project.monitoring.lineage_emitter import emit_start, emit_complete, emit_fail
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def run_pipeline(
    config_path: str = None,
    env: str = None,
    phase: str = "all"
) -> bool:
    """
    Orchestrate the complete ETL pipeline (Bronze → Silver → Gold).
    
    Args:
        config_path: Path to config file
        env: Environment (dev/prod/local)
        phase: Pipeline phase ('all', 'bronze_silver', 'silver_gold')
        
    Returns:
        True if successful, False otherwise
    """
    if config_path is None:
        if env:
            config_path = f"config/{env}.yaml"
        else:
            env = os.getenv("ENV", "dev")
            config_path = f"config/{env}.yaml"
    
    config = load_conf(config_path)
    spark = build_spark(app_name="run_pipeline", config=config)
    job_name = "full_etl_pipeline"
    run_id = f"{job_name}_{os.getenv('AIRFLOW_RUN_ID', 'local')}"
    
    try:
        # Emit lineage start
        emit_start(
            job_name=job_name,
            inputs=[{"name": "bronze/*"}],
            outputs=[{"name": "gold/*"}],
            config=config,
            run_id=run_id
        )
        
        # Bronze → Silver
        if phase in ["all", "bronze_silver"]:
            logger.info("Starting Bronze → Silver transformation")
            bronze_to_silver(spark, config)
            logger.info("✅ Bronze → Silver completed")
        
        # Silver → Gold
        if phase in ["all", "silver_gold"]:
            logger.info("Starting Silver → Gold transformation")
            silver_to_gold(spark, config)
            logger.info("✅ Silver → Gold completed")
        
        # Update watermarks for sources that were processed
        logger.info("Updating watermarks for successful run")
        current_ts = datetime.now(timezone.utc)
        upsert_watermark("snowflake_orders", current_ts, config, spark)
        upsert_watermark("redshift_behavior", current_ts, config, spark)
        
        # Emit lineage complete
        emit_complete(
            job_name=job_name,
            inputs=[{"name": "bronze/*"}],
            outputs=[{"name": "gold/*"}],
            config=config,
            run_id=run_id,
            metadata={"records_processed": "N/A"}  # Would track actual counts
        )
        
        logger.info("✅ Pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        emit_fail(
            job_name=job_name,
            inputs=[{"name": "bronze/*"}],
            outputs=[{"name": "gold/*"}],
            config=config,
            run_id=run_id,
            error=str(e)
        )
        return False
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL Pipeline")
    parser.add_argument("--config", help="Configuration file path")
    parser.add_argument("--env", choices=["dev", "prod", "local"],
                       help="Environment (dev/prod/local)")
    parser.add_argument("--phase", choices=["all", "bronze_silver", "silver_gold"],
                       default="all", help="Pipeline phase to run")
    
    args = parser.parse_args()
    
    success = run_pipeline(config_path=args.config, env=args.env, phase=args.phase)
    sys.exit(0 if success else 1)

