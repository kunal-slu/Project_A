#!/usr/bin/env python3
"""
Safe Local ETL Pipeline Runner

Runs the ETL pipeline with better error handling and fallback options.
If SparkSession fails, provides helpful diagnostics and alternatives.
"""
import sys
import os
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent


def check_spark_environment():
    """Check if Spark environment is properly configured."""
    issues = []
    
    # Check Java
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            issues.append("❌ Java not found or not working")
        else:
            # Check Java version
            java_version = result.stdout + result.stderr
            if "version \"17" in java_version or "openjdk version \"17" in java_version:
                issues.append("⚠️  Java 17 detected - PySpark may have compatibility issues. Use Java 11: export JAVA_HOME=$(/usr/libexec/java_home -v 11)")
            elif "version \"11" in java_version or "version \"1.8" in java_version:
                logger.info("✅ Java found (compatible version)")
            else:
                logger.info("✅ Java found")
    except FileNotFoundError:
        issues.append("❌ Java not installed")
    except subprocess.TimeoutExpired:
        issues.append("⚠️  Java check timed out")
    
    # Check PySpark
    try:
        import pyspark
        logger.info(f"✅ PySpark found: {pyspark.__version__}")
    except ImportError:
        issues.append("❌ PySpark not installed")
    
    # Check JAVA_HOME
    if not os.getenv('JAVA_HOME'):
        issues.append("⚠️  JAVA_HOME not set (may cause issues)")
    
    return issues


def run_bronze_to_silver(config_path: str = "local/config/local.yaml") -> bool:
    """Run Bronze→Silver transformation locally."""
    logger.info("=" * 80)
    logger.info("STEP 1: BRONZE → SILVER")
    logger.info("=" * 80)
    
    # Set PYTHONPATH
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src") + (":" + env.get("PYTHONPATH", "") if env.get("PYTHONPATH") else "")
    
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "jobs/transform/bronze_to_silver.py"),
        "--env", "local",
        "--config", str(PROJECT_ROOT / config_path),
    ]
    
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, env=env, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"❌ Bronze→Silver failed with exit code {result.returncode}")
        logger.error("STDOUT:")
        logger.error(result.stdout)
        logger.error("STDERR:")
        logger.error(result.stderr)
        
        # Check for SparkSession error
        if "Py4JError" in result.stderr or "SparkSession" in result.stderr:
            logger.error("")
            logger.error("=" * 80)
            logger.error("🔧 SPARKSESSION ERROR DETECTED")
            logger.error("=" * 80)
            logger.error("")
            logger.error("Common fixes:")
            logger.error("  1. Check Java version: java -version (should be Java 8 or 11)")
            logger.error("  2. Set JAVA_HOME: export JAVA_HOME=$(/usr/libexec/java_home)")
            logger.error("  3. Reinstall PySpark: pip install --upgrade pyspark")
            logger.error("  4. Use AWS EMR instead (recommended): Both local and AWS use S3 data")
            logger.error("")
            logger.error("Alternative: Check S3 data directly (no Spark required):")
            logger.error(f"  python3 {PROJECT_ROOT}/local/scripts/dq/check_s3_data_quality.py --layer all")
        
        return False
    
    logger.info("✅ Bronze→Silver completed successfully")
    return True


def run_silver_to_gold(config_path: str = "local/config/local.yaml") -> bool:
    """Run Silver→Gold transformation locally."""
    logger.info("=" * 80)
    logger.info("STEP 2: SILVER → GOLD")
    logger.info("=" * 80)
    
    # Set PYTHONPATH
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src") + (":" + env.get("PYTHONPATH", "") if env.get("PYTHONPATH") else "")
    
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "jobs/transform/silver_to_gold.py"),
        "--env", "local",
        "--config", str(PROJECT_ROOT / config_path),
    ]
    
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, env=env, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"❌ Silver→Gold failed with exit code {result.returncode}")
        logger.error("STDOUT:")
        logger.error(result.stdout)
        logger.error("STDERR:")
        logger.error(result.stderr)
        return False
    
    logger.info("✅ Silver→Gold completed successfully")
    return True


def verify_outputs(config_path: str = "local/config/local.yaml"):
    """Verify output tables exist and show row counts."""
    logger.info("=" * 80)
    logger.info("STEP 3: VERIFICATION")
    logger.info("=" * 80)
    
    try:
        from pyspark.sql import SparkSession
        from project_a.utils.spark_session import build_spark
        from project_a.config_loader import load_config_resolved
        
        config = load_config_resolved(str(PROJECT_ROOT / config_path))
        spark = build_spark(config=config)
        
        try:
            silver_root = config["paths"]["silver_root"]
            gold_root = config["paths"]["gold_root"]
            
            # Check Silver tables
            logger.info("\n📊 Silver Layer:")
            silver_tables = {
                "customers": f"{silver_root}/customers_silver",
                "orders": f"{silver_root}/orders_silver",
                "products": f"{silver_root}/products_silver",
                "behavior": f"{silver_root}/customer_behavior_silver",
                "fx_rates": f"{silver_root}/fx_rates_silver",
                "order_events": f"{silver_root}/order_events_silver",
            }
            
            for table_name, path in silver_tables.items():
                try:
                    # Try Delta first, then Parquet
                    try:
                        df = spark.read.format("delta").load(path)
                    except Exception:
                        df = spark.read.format("parquet").load(path)
                    count = df.count()
                    logger.info(f"  ✅ {table_name}: {count:,} rows")
                except Exception as e:
                    logger.warning(f"  ⚠️ {table_name}: {e}")
            
            # Check Gold tables
            logger.info("\n📊 Gold Layer:")
            gold_tables = {
                "fact_orders": f"{gold_root}/fact_orders",
                "dim_customer": f"{gold_root}/dim_customer",
                "dim_product": f"{gold_root}/dim_product",
                "dim_date": f"{gold_root}/dim_date",
                "customer_360": f"{gold_root}/customer_360",
                "product_performance": f"{gold_root}/product_performance",
            }
            
            for table_name, path in gold_tables.items():
                try:
                    # Try Delta first, then Parquet
                    try:
                        df = spark.read.format("delta").load(path)
                    except Exception:
                        df = spark.read.format("parquet").load(path)
                    count = df.count()
                    logger.info(f"  ✅ {table_name}: {count:,} rows")
                except Exception as e:
                    logger.warning(f"  ⚠️ {table_name}: {e}")
        
        finally:
            spark.stop()
    
    except Exception as e:
        logger.warning(f"⚠️  Could not verify outputs with Spark: {e}")
        logger.info("")
        logger.info("💡 Alternative: Check S3 data directly (no Spark required):")
        logger.info(f"   python3 {PROJECT_ROOT}/local/scripts/dq/check_s3_data_quality.py --layer all")


def main():
    """Run full ETL pipeline locally with safety checks."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run full ETL pipeline locally (safe mode)")
    parser.add_argument("--config", default="local/config/local.yaml", help="Config file path")
    parser.add_argument("--skip-bronze", action="store_true", help="Skip Bronze→Silver step")
    parser.add_argument("--skip-gold", action="store_true", help="Skip Silver→Gold step")
    parser.add_argument("--verify-only", action="store_true", help="Only verify outputs, don't run ETL")
    parser.add_argument("--check-env", action="store_true", help="Check Spark environment before running")
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("🚀 STARTING LOCAL ETL PIPELINE (SAFE MODE)")
    logger.info("=" * 80)
    logger.info(f"Config: {args.config}")
    logger.info(f"Project Root: {PROJECT_ROOT}")
    logger.info("")
    
    # Check environment if requested
    if args.check_env:
        logger.info("Checking Spark environment...")
        issues = check_spark_environment()
        if issues:
            logger.warning("Environment issues found:")
            for issue in issues:
                logger.warning(f"  {issue}")
            logger.info("")
            logger.info("You can still try to run, but errors may occur.")
            logger.info("")
        else:
            logger.info("✅ Environment looks good!")
            logger.info("")
    
    if args.verify_only:
        verify_outputs(args.config)
        return
    
    # Step 1: Bronze → Silver
    if not args.skip_bronze:
        if not run_bronze_to_silver(args.config):
            logger.error("Pipeline failed at Bronze→Silver step")
            sys.exit(1)
    else:
        logger.info("⏭️  Skipping Bronze→Silver step")
    
    # Step 2: Silver → Gold
    if not args.skip_gold:
        if not run_silver_to_gold(args.config):
            logger.error("Pipeline failed at Silver→Gold step")
            sys.exit(1)
    else:
        logger.info("⏭️  Skipping Silver→Gold step")
    
    # Step 3: Verify outputs
    verify_outputs(args.config)
    
    logger.info("")
    logger.info("=" * 80)
    logger.info("🎉 LOCAL ETL PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

