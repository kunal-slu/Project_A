#!/usr/bin/env python3
"""
Kafka streaming to Bronze ingestion job.

Continuously ingests streaming orders from Kafka and stores in S3 Bronze layer.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark_interview_project.utils import build_spark
from pyspark_interview_project.utils.path_resolver import bronze_path
from pyspark_interview_project.utils.io import write_streaming
from pyspark_interview_project.extract.kafka_orders_stream import extract_kafka_orders_stream
from pyspark_interview_project.config_loader import load_config_resolved


def main():
    """Main entry point for Kafka streaming to Bronze job."""
    config = load_config_resolved('config/config-prod.yaml')
    spark = build_spark(config)
    
    try:
        # Extract streaming data from Kafka
        df = extract_kafka_orders_stream(spark, config)
        
        # Write to Bronze (streaming)
        bronze_location = bronze_path(
            config['lake']['root'],
            'kafka',
            'orders_stream'
        )
        
        write_streaming(
            df,
            bronze_location,
            checkpoint_location=config['lake']['checkpoint'],
            trigger="1 minute",
            config=config
        )
        
        print(f"✅ Streaming job started: {bronze_location}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

