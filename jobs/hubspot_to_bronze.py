#!/usr/bin/env python3
"""
HubSpot to Bronze Job - EMR Wrapper

Thin wrapper for EMR Serverless execution.
Reads HubSpot contacts and writes to S3 Bronze layer.
"""

import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.path_resolver import bronze_path
from pyspark_interview_project.config_loader import load_config_resolved

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for EMR job."""
    # Load configuration
    config = load_config_resolved("config/prod.yaml")
    
    # Build Spark session
    spark = build_spark(config)
    
    logger.info("Starting HubSpot to Bronze job")
    
    # Read HubSpot contacts (placeholder - would call extract.hubspot_contacts)
    # For now, create dummy data
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    from pyspark.sql import Row
    
    rows = [
        Row(contact_id="1", name="John Doe", email="john@example.com", company_id="10"),
        Row(contact_id="2", name="Jane Smith", email="jane@example.com", company_id="11"),
        Row(contact_id="3", name="Bob Johnson", email="bob@example.com", company_id="12"),
    ]
    
    # Create DataFrame
    contacts_df = spark.createDataFrame(rows)
    
    # Get bronze path
    lake_bucket = config["lake"]["root"].replace("s3://", "").split("/")[0]
    output_path = bronze_path(lake_bucket, "hubspot", "contacts")
    
    logger.info(f"Writing to: {output_path}")
    
    # Write to bronze
    contacts_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .save(output_path)
    
    logger.info("HubSpot to Bronze job completed successfully")
    
    spark.stop()


if __name__ == "__main__":
    main()

