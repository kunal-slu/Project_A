#!/usr/bin/env python3
"""
DQ Gate: Enforces Great Expectations suites as hard gates
Fails pipeline on critical violations
"""
import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved
from pyspark_interview_project.dq.gate import DQGate


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="DQ Gate")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--layer", required=True, help="Layer (bronze/silver/gold)")
    args = parser.parse_args()
    
    # Load config
    config_path = Path("config/dev.yaml")
    if not config_path.exists():
        config_path = Path("config/prod.yaml")
    config = load_config_resolved(str(config_path))
    
    # Build Spark session
    spark = build_spark(config)
    
    try:
        lake_bucket = config["buckets"]["lake"]
        
        # Read table
        table_path = f"s3a://{lake_bucket}/{args.layer}/{args.table}/"
        df = spark.read.format("delta").load(table_path)
        
        # Run DQ gate (pass config)
        gate = DQGate(config)
        result = gate.check_and_block(spark, df, args.table, layer=args.layer)
        
        print(f"✅ DQ Gate passed for {args.layer}.{args.table}")
        print(f"  - Critical failures: {result.get('critical_failures', 0)}")
        print(f"  - Warnings: {result.get('warnings', 0)}")
        sys.exit(0)
            
    except Exception as e:
        print(f"❌ DQ Gate error: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

