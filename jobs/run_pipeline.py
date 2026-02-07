#!/usr/bin/env python3
"""
Project A - Unified ETL Pipeline Entry Point

Single entry point for all ETL jobs. Routes to appropriate job based on --job argument.

Usage:
    python jobs/run_pipeline.py --job bronze_to_silver --env dev --config config/dev.yaml

Supported jobs:
    Bronze ingestion:
        - snowflake_to_bronze
        - redshift_to_bronze
        - crm_to_bronze
        - fx_to_bronze
        - kafka_events_to_bronze
    
    Transformations:
        - bronze_to_silver
        - silver_to_gold
    
    Optional:
        - kafka_producer (local simulation)
"""

import argparse
import logging
import sys
from pathlib import Path

# Add src to path for local execution
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from project_a.core.config import ProjectConfig
from project_a.core.base_job import BaseJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Project A - Unified ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--job",
        required=True,
        help="Job name (e.g., bronze_to_silver, silver_to_gold)",
    )
    parser.add_argument(
        "--env",
        required=True,
        help="Environment (dev/staging/prod)",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Config file path (local or s3://...)",
    )
    parser.add_argument(
        "--run-date",
        help="Optional run date override (YYYY-MM-DD)",
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = ProjectConfig(args.config, env=args.env)
        logger.info(f"✅ Configuration loaded from: {args.config}")
    except Exception as e:
        logger.error(f"❌ Failed to load configuration: {e}")
        sys.exit(1)
    
    # Route to appropriate job
    job_name = args.job.lower()
    
    try:
        if job_name == "bronze_to_silver":
            from jobs.transform.bronze_to_silver import BronzeToSilverJob
            job = BronzeToSilverJob(config)
        elif job_name == "silver_to_gold":
            from jobs.transform.silver_to_gold import SilverToGoldJob
            job = SilverToGoldJob(config)
        elif job_name == "snowflake_to_bronze":
            from jobs.ingest.snowflake_to_bronze import SnowflakeToBronzeJob
            job = SnowflakeToBronzeJob(config)
        elif job_name == "redshift_to_bronze":
            from jobs.ingest.redshift_to_bronze import RedshiftToBronzeJob
            job = RedshiftToBronzeJob(config)
        elif job_name == "crm_to_bronze":
            from jobs.ingest.crm_to_bronze import CrmToBronzeJob
            job = CrmToBronzeJob(config)
        elif job_name == "fx_to_bronze":
            from jobs.ingest.fx_to_bronze import FxToBronzeJob
            job = FxToBronzeJob(config)
        elif job_name == "kafka_events_to_bronze":
            from jobs.ingest.kafka_events_to_bronze import KafkaEventsToBronzeJob
            job = KafkaEventsToBronzeJob(config)
        elif job_name == "kafka_producer":
            from jobs.streaming.kafka_producer import KafkaProducerJob
            job = KafkaProducerJob(config)
        else:
            logger.error(f"❌ Unknown job: {job_name}")
            logger.info(f"Supported jobs: bronze_to_silver, silver_to_gold, snowflake_to_bronze, redshift_to_bronze, crm_to_bronze, fx_to_bronze, kafka_events_to_bronze, kafka_producer")
            sys.exit(1)
        
        # Execute job
        logger.info(f"🚀 Starting job: {job_name}")
        result = job.execute()
        logger.info(f"✅ Job completed: {job_name}")
        logger.info(f"Result: {result}")
        
    except ImportError as e:
        logger.error(f"❌ Failed to import job module: {e}")
        logger.info("Make sure all job modules are implemented in project_a.transform/ and project_a.ingest/")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Job failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

