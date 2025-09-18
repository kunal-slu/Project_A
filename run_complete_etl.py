#!/usr/bin/env python3
"""
Complete ETL Pipeline Runner
Runs the entire data pipeline from extraction to loading with all optimizations.
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.pipeline import run_pipeline
import yaml

def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def run_complete_etl(config_path="config/config-dev.yaml"):
    """
    Run the complete ETL pipeline.

    Args:
        config_path: Path to configuration file
    """
    logger = logging.getLogger(__name__)

    try:
        logger.info("🚀 Starting Complete ETL Pipeline")

        # Load configuration
        logger.info("📋 Loading configuration...")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"✅ Configuration loaded from {config_path}")

        # Initialize Spark session
        logger.info("⚡ Initializing Spark session...")
        spark = build_spark(config)
        logger.info("✅ Spark session initialized")

        # Run the complete pipeline
        logger.info("🔄 Starting pipeline execution...")
        import uuid
        run_id = uuid.uuid4().hex[:12]
        run_pipeline(spark, config, run_id)

        logger.info("🎉 Complete ETL Pipeline finished successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed: {str(e)}")
        raise
    finally:
        try:
            spark.stop()
            logger.info("🔌 Spark session stopped")
        except:
            pass

if __name__ == "__main__":
    setup_logging()

    # Get config path from command line or use default
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/config-dev.yaml"

    success = run_complete_etl(config_path)
    sys.exit(0 if success else 1)
