#!/usr/bin/env python3
"""
Bronze to Silver Transformation
- Canonicalize schemas
- Deduplicate on natural keys
- Multi-source join (Snowflake + Redshift)
"""
import sys
import argparse
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import yaml

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.transform.bronze_to_silver_multi_source import (
    bronze_to_silver_multi_source
)


def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Bronze to Silver transformation job")
    parser.add_argument("--env", default="dev", help="Environment (dev/prod)")
    parser.add_argument("--config", help="Config file path (local or S3)")
    args = parser.parse_args()
    
    # Load config
    if args.config:
        config_path = args.config
        # If S3 path, read it using Spark's native S3 support
        if config_path.startswith("s3://"):
            from pyspark.sql import SparkSession
            spark_temp = SparkSession.builder \
                .appName("config_loader") \
                .config("spark.sql.adaptive.enabled", "false") \
                .getOrCreate()
            try:
                config_lines = spark_temp.sparkContext.textFile(config_path).collect()
                config_content = "\n".join(config_lines)
                tmp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8")
                tmp_file.write(config_content)
                tmp_file.close()
                config_path = tmp_file.name
            finally:
                spark_temp.stop()
        else:
            config_path = str(Path(config_path))
    else:
        config_path = Path(f"config/{args.env}.yaml")
        if not config_path.exists():
            config_path = Path("config/prod.yaml")
        if not config_path.exists():
            config_path = Path("config/local.yaml")
        config_path = str(config_path)
    
    config = load_config_resolved(config_path)
    
    # Ensure environment is set correctly for EMR
    if not config.get("environment"):
        config["environment"] = "emr"
    
    # Build Spark session
    spark = build_spark(config)
    
    try:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")
        
        # Use the multi-source transformation function
        results = bronze_to_silver_multi_source(spark, config, run_date=run_date)
        
        print(f"✅ Bronze to Silver transformation completed")
        print(f"  - Customers: {results['customers'].count():,} rows")
        print(f"  - Orders: {results['orders'].count():,} rows")
        print(f"  - Customer Activity: {results['customer_activity'].count():,} rows")
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

