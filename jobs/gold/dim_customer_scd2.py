#!/usr/bin/env python3
"""
Gold Layer: SCD Type-2 for dim_customer
Slowly Changing Dimension implementation using Delta MERGE
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.config_loader import load_config_resolved


def main():
    """Main entry point."""
    # Load config
    config_path = Path("config/dev.yaml")
    if not config_path.exists():
        config_path = Path("config/prod.yaml")
    config = load_config_resolved(str(config_path))
    
    # Build Spark session
    spark = build_spark(config)
    
    try:
        lake_bucket = config["buckets"]["lake"]
        
        # Read Silver customers
        silver_customers = spark.read.format("delta").load(f"s3a://{lake_bucket}/silver/customers/")
        
        # Prepare source with SCD2 columns
        source = silver_customers \
            .withColumn("is_current", F.lit(True)) \
            .withColumn("valid_from", F.current_timestamp()) \
            .withColumn("valid_to", F.lit(None).cast("timestamp")) \
            .withColumn("customer_sk", F.monotonically_increasing_id())
        
        # Load existing Gold table (create if doesn't exist)
        gold_path = f"s3a://{lake_bucket}/gold/dim_customer/"
        
        try:
            gold_table = DeltaTable.forPath(spark, gold_path)
            print("✅ Existing dim_customer table found")
        except Exception:
            # First run: create initial table
            source.write.format("delta").mode("overwrite").save(gold_path)
            print("✅ Created initial dim_customer table")
            spark.stop()
            return
        
        # SCD2 MERGE
        merge_key = "customer_id"
        
        # Update existing records that changed
        gold_table.alias("target").merge(
            source.alias("source"),
            f"target.{merge_key} = source.{merge_key} AND target.is_current = true"
        ).whenMatchedUpdate(
            condition="target.email != source.email OR target.country != source.country",
            set={
                "is_current": F.lit(False),
                "valid_to": F.current_timestamp()
            }
        ).whenNotMatchedInsert(
            values={
                "customer_sk": "source.customer_sk",
                "customer_id": "source.customer_id",
                "email": "source.email",
                "country": "source.country",
                "is_current": "source.is_current",
                "valid_from": "source.valid_from",
                "valid_to": "source.valid_to"
            }
        ).execute()
        
        # Insert new current records
        new_records = source.join(
            gold_table.toDF().filter(F.col("is_current") == True).select("customer_id"),
            "customer_id",
            "left_anti"
        )
        
        if not new_records.isEmpty():
            new_records.write.format("delta").mode("append").save(gold_path)
            print(f"✅ Inserted {new_records.count()} new customer records")
        
        print("✅ SCD2 merge completed")
        
    except Exception as e:
        print(f"❌ SCD2 failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

