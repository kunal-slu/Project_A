#!/usr/bin/env python3
"""
Redshift to Bronze ingestion job.

Extracts customer behavior data from Redshift and stores in S3 Bronze layer.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark_interview_project.utils import build_spark
from pyspark_interview_project.utils.path_resolver import bronze_path
from pyspark_interview_project.utils.io import write_delta
from pyspark_interview_project.extract.redshift_behavior import extract_redshift_behavior
from pyspark_interview_project.config_loader import load_config_resolved


def main():
    """Main entry point for Redshift to Bronze job."""
    config = load_config_resolved('config/config-prod.yaml')
    spark = build_spark(config)
    
    try:
        # Extract from Redshift
        df = extract_redshift_behavior(spark, config)
        
        # Write to Bronze
        bronze_location = bronze_path(
            config['lake']['root'],
            'redshift',
            'customer_behavior'
        )
        
        write_delta(df, bronze_location, mode='overwrite', config=config)
        
        print(f"✅ Successfully ingested to Bronze: {bronze_location}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

