#!/usr/bin/env python3
"""
Load Testing Script

Tests pipeline performance at different scales (1M, 10M, 100M records).
"""

import sys
import os
import time
import argparse
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from pyspark.sql import SparkSession
from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf


def generate_test_data(spark: SparkSession, record_count: int) -> Path:
    """Generate test data of specified size."""
    from pyspark.sql.functions import lit, rand
    
    test_data_dir = Path(f"/tmp/load_test_{record_count}")
    
    print(f"Generating {record_count:,} test records...")
    df = spark.range(record_count) \
        .withColumn("account_id", lit("ACC") + (rand() * 10000).cast("int").cast("string")) \
        .withColumn("amount", rand() * 1000) \
        .withColumn("order_date", lit("2024-01-15"))
    
    df.write.mode("overwrite").parquet(str(test_data_dir))
    
    actual_count = df.count()
    size_mb = sum(f.stat().st_size for f in test_data_dir.rglob("*") if f.is_file()) / (1024 * 1024)
    
    print(f"✅ Generated {actual_count:,} records ({size_mb:.2f} MB)")
    return test_data_dir


def benchmark_ingestion(spark: SparkSession, test_data: Path, output_path: str):
    """Benchmark Bronze ingestion."""
    start = time.time()
    
    df = spark.read.parquet(str(test_data))
    df.write.format("delta").mode("overwrite").save(output_path)
    
    duration = time.time() - start
    records = df.count()
    throughput = records / duration if duration > 0 else 0
    
    return {
        "stage": "bronze_ingestion",
        "duration_seconds": duration,
        "records_processed": records,
        "throughput_rps": throughput
    }


def run_load_test(record_counts: list, config_path: str = "config/local.yaml"):
    """Run load tests at different scales."""
    config = load_conf(config_path)
    spark = build_spark(app_name="load_test", config=config)
    
    results = []
    
    try:
        for count in record_counts:
            print(f"\n{'='*60}")
            print(f"Load Test: {count:,} records")
            print(f"{'='*60}")
            
            # Generate test data
            test_data = generate_test_data(spark, count)
            
            # Benchmark ingestion
            result = benchmark_ingestion(
                spark,
                test_data,
                f"data/lakehouse_delta/bronze/load_test_{count}"
            )
            result["record_count"] = count
            results.append(result)
            
            # Cleanup
            import shutil
            shutil.rmtree(test_data, ignore_errors=True)
            
            print(f"✅ Completed: {result['duration_seconds']:.2f}s ({result['throughput_rps']:.0f} records/sec)")
    
    finally:
        spark.stop()
    
    # Print summary
    print(f"\n{'='*60}")
    print("Load Test Results Summary")
    print(f"{'='*60}")
    print(f"{'Records':>12} | {'Duration (s)':>15} | {'Throughput (rps)':>20}")
    print("-" * 60)
    for r in results:
        print(f"{r['record_count']:>12,} | {r['duration_seconds']:>15.2f} | {r['throughput_rps']:>20,.0f}")
    
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load test pipeline")
    parser.add_argument("--scales", nargs="+", type=int, default=[1000000, 10000000],
                       help="Record counts to test (e.g., 1000000 10000000)")
    parser.add_argument("--config", default="config/local.yaml", help="Config file")
    
    args = parser.parse_args()
    
    results = run_load_test(args.scales, args.config)

