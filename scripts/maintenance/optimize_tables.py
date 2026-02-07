#!/usr/bin/env python3
"""
Delta table optimization script (P6-16).

Performs:
- OPTIMIZE (small file compaction)
- ZORDER (data clustering)
- VACUUM (old file removal)
"""

import sys
import argparse
import logging
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from project_a.utils.spark_session import build_spark
from project_a.utils.path_resolver import resolve_path
from project_a.config_loader import load_config_resolved

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def optimize_table(
    spark,
    table_path: str,
    zorder_by: List[str] = None,
    retain_hours: int = 168
) -> None:
    """
    Optimize Delta table with OPTIMIZE, ZORDER, and VACUUM.
    
    Args:
        spark: SparkSession
        table_path: Path to Delta table
        zorder_by: Columns for ZORDER clustering
        retain_hours: Hours to retain for VACUUM (default 7 days)
    """
    logger.info(f"🔄 Optimizing table: {table_path}")
    
    # Register as temporary table for SQL
    table_name = table_path.split("/")[-1]
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW {table_name}_temp AS
        SELECT * FROM delta.`{table_path}`
    """)
    
    # 1. OPTIMIZE (small file compaction)
    logger.info("📦 Running OPTIMIZE (compacting small files)...")
    
    if zorder_by:
        zorder_cols = ", ".join(zorder_by)
        optimize_sql = f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_cols})"
        logger.info(f"  ZORDER columns: {zorder_cols}")
    else:
        optimize_sql = f"OPTIMIZE delta.`{table_path}`"
    
    spark.sql(optimize_sql)
    logger.info("✅ OPTIMIZE complete")
    
    # 2. VACUUM (remove old files)
    logger.info(f"🧹 Running VACUUM (retaining {retain_hours} hours)...")
    
    vacuum_sql = f"VACUUM delta.`{table_path}` RETAIN {retain_hours} HOURS"
    spark.sql(vacuum_sql)
    logger.info("✅ VACUUM complete")
    
    logger.info(f"✅ Table optimization complete: {table_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Optimize Delta tables")
    parser.add_argument("--table", required=True, help="Table name (e.g., silver.orders)")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], help="Data layer")
    parser.add_argument("--zorder", nargs="+", help="Columns for ZORDER (e.g., customer_id order_date)")
    parser.add_argument("--retain-hours", type=int, default=168, help="Hours to retain (default 168 = 7 days)")
    parser.add_argument("--config", default="config/local.yaml", help="Config file")
    
    args = parser.parse_args()
    
    # Load config
    config = load_config_resolved(args.config)
    spark = build_spark(config)
    
    try:
        # Resolve table path
        if args.layer:
            table_path = resolve_path(f"lake://{args.layer}", args.table, config=config)
        else:
            # Assume full path
            table_path = args.table
        
        # Optimize
        optimize_table(
            spark,
            table_path,
            zorder_by=args.zorder,
            retain_hours=args.retain_hours
        )
        
        logger.info("🎉 Optimization complete")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Optimization failed: {e}", exc_info=True)
        return 1
        
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

