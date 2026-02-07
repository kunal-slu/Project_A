#!/usr/bin/env python3
"""
Quick check script to verify pipeline fixes.

Checks:
1. No delta errors in local mode
2. FX rates schema is present even if empty
3. fact_orders.customer_sk is not 100% -1
4. All critical tables have data
"""

import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from project_a.utils.spark_session import build_spark
from project_a.pyspark_interview_project.utils.config_loader import load_config_resolved
from project_a.utils.path_resolver import resolve_data_path

def main():
    """Run quick checks."""
    print("=" * 80)
    print("🔍 QUICK PIPELINE CHECK")
    print("=" * 80)
    
    # Load config
    config = load_config_resolved("local/config/local.yaml", env="local")
    config["environment"] = "local"
    
    # Build Spark
    spark = build_spark(app_name="quick_check", config=config)
    
    try:
        checks_passed = 0
        checks_total = 0
        
        # Check 1: Read orders_silver (should be parquet, no delta errors)
        print("\n[Check 1] Reading orders_silver (parquet only, no delta)...")
        checks_total += 1
        try:
            silver_root = resolve_data_path(config, "silver")
            orders_df = spark.read.parquet(f"{silver_root}/orders_silver")
            count = orders_df.count()
            print(f"  ✅ orders_silver: {count:,} rows (parquet read successful)")
            checks_passed += 1
        except Exception as e:
            if "delta" in str(e).lower() or "DefaultSource" in str(e):
                print(f"  ❌ ERROR: Delta error detected: {e}")
            else:
                print(f"  ⚠️  orders_silver read failed: {e}")
        
        # Check 2: FX rates schema (even if empty)
        print("\n[Check 2] Checking fx_rates_silver schema...")
        checks_total += 1
        try:
            fx_df = spark.read.parquet(f"{silver_root}/fx_rates_silver")
            fx_count = fx_df.count()
            schema_fields = [f.name for f in fx_df.schema.fields]
            expected_fields = ["trade_date", "base_ccy", "counter_ccy", "rate"]
            
            if all(f in schema_fields for f in expected_fields):
                print(f"  ✅ fx_rates_silver schema correct: {schema_fields}")
                print(f"     Row count: {fx_count:,} (empty is OK if schema present)")
                checks_passed += 1
            else:
                print(f"  ⚠️  Schema mismatch. Expected: {expected_fields}, Got: {schema_fields}")
        except Exception as e:
            if "UNABLE_TO_INFER_SCHEMA" in str(e):
                print(f"  ❌ ERROR: Schema inference failed: {e}")
            else:
                print(f"  ⚠️  fx_rates_silver read failed: {e}")
        
        # Check 3: fact_orders.customer_sk not 100% -1
        print("\n[Check 3] Checking fact_orders.customer_sk...")
        checks_total += 1
        try:
            gold_root = resolve_data_path(config, "gold")
            fact_df = spark.read.parquet(f"{gold_root}/fact_orders")
            total = fact_df.count()
            
            if "customer_sk" in fact_df.columns:
                missing = fact_df.filter(F.col("customer_sk") == -1).count()
                missing_pct = (missing / total * 100.0) if total > 0 else 0.0
                
                if missing_pct < 50.0:  # Less than 50% missing is acceptable
                    print(f"  ✅ fact_orders.customer_sk: {missing_pct:.2f}% missing ({missing:,}/{total:,})")
                    checks_passed += 1
                else:
                    print(f"  ❌ fact_orders.customer_sk: {missing_pct:.2f}% missing (too high!)")
            else:
                print(f"  ⚠️  customer_sk column not found in fact_orders")
        except Exception as e:
            print(f"  ⚠️  fact_orders check failed: {e}")
        
        # Check 4: Critical tables have data
        print("\n[Check 4] Checking critical tables have data...")
        critical_tables = [
            ("customers_silver", f"{silver_root}/customers_silver"),
            ("orders_silver", f"{silver_root}/orders_silver"),
            ("products_silver", f"{silver_root}/products_silver"),
            ("fact_orders", f"{gold_root}/fact_orders"),
            ("dim_customer", f"{gold_root}/dim_customer"),
        ]
        
        for table_name, path in critical_tables:
            checks_total += 1
            try:
                df = spark.read.parquet(path)
                count = df.count()
                if count > 0:
                    print(f"  ✅ {table_name}: {count:,} rows")
                    checks_passed += 1
                else:
                    print(f"  ❌ {table_name}: 0 rows (FAIL)")
            except Exception as e:
                print(f"  ❌ {table_name}: Read failed - {e}")
        
        # Summary
        print("\n" + "=" * 80)
        print(f"📊 CHECK SUMMARY: {checks_passed}/{checks_total} passed")
        print("=" * 80)
        
        if checks_passed == checks_total:
            print("✅ All checks passed!")
            return 0
        else:
            print("⚠️  Some checks failed")
            return 1
            
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())

