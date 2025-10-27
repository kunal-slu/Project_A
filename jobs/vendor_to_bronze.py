#!/usr/bin/env python3
"""
Vendor to Bronze ingestion job.

Extracts FX rates and financial metrics from vendor sources and stores in S3 Bronze layer.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark_interview_project.utils import build_spark
from pyspark_interview_project.utils.path_resolver import bronze_path
from pyspark_interview_project.utils.io import write_delta
from pyspark_interview_project.extract.fx_rates import extract_fx_rates
from pyspark_interview_project.config_loader import load_config_resolved


def main():
    """Main entry point for Vendor to Bronze job."""
    config = load_config_resolved('config/config-prod.yaml')
    spark = build_spark(config)
    
    try:
        # Extract FX rates
        df_fx = extract_fx_rates(spark, config)
        
        # Write FX rates to Bronze
        bronze_fx = bronze_path(
            config['lake']['root'],
            'fx_vendor',
            'fx_rates'
        )
        write_delta(df_fx, bronze_fx, mode='append', config=config)
        
        # Extract financial metrics
        # (Add similar logic for other vendor sources)
        
        print(f"✅ Successfully ingested vendor data to Bronze")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

