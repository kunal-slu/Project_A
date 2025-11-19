#!/usr/bin/env python3
"""
Organize data/ directory to match aws/data/ structure.

Target structure (matching aws/data/):
aws/data/
  samples/
    crm/
      accounts.csv
      contacts.csv
      opportunities.csv
    snowflake/
      snowflake_customers_50000.csv
      snowflake_orders_100000.csv
      snowflake_products_10000.csv
    redshift/
      redshift_customer_behavior_50000.csv
    fx/
      fx_rates_historical.json
    kafka/
      stream_kafka_events_100000.csv
"""
import shutil
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path("/Users/kunal/IdeaProjects/Project_A")
DATA_DIR = PROJECT_ROOT / "data"
AWS_DATA_DIR = PROJECT_ROOT / "aws" / "data"
SAMPLES_DIR = DATA_DIR / "samples"


def organize_data_directory():
    """Organize data/ to match aws/data/ structure."""
    logger.info("🔧 Organizing data/ directory to match aws/data/ structure...")
    
    # Create samples directory structure
    samples_dirs = {
        "crm": SAMPLES_DIR / "crm",
        "snowflake": SAMPLES_DIR / "snowflake",
        "redshift": SAMPLES_DIR / "redshift",
        "fx": SAMPLES_DIR / "fx",
        "kafka": SAMPLES_DIR / "kafka"
    }
    
    for dir_name, dir_path in samples_dirs.items():
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"✅ Created directory: {dir_path}")
    
    # Map files from current data/ structure to new structure
    file_mappings = []
    
    # Check for CRM files
    crm_sources = [
        DATA_DIR / "crm" / "accounts.csv",
        DATA_DIR / "crm" / "contacts.csv",
        DATA_DIR / "crm" / "opportunities.csv",
    ]
    
    for src in crm_sources:
        if src.exists():
            dst = SAMPLES_DIR / "crm" / src.name
            file_mappings.append((src, dst))
    
    # Check for Snowflake files
    snowflake_sources = [
        DATA_DIR / "snowflake" / "snowflake_customers_50000.csv",
        DATA_DIR / "snowflake" / "snowflake_orders_100000.csv",
        DATA_DIR / "snowflake" / "snowflake_products_10000.csv",
    ]
    
    for src in snowflake_sources:
        if src.exists():
            dst = SAMPLES_DIR / "snowflake" / src.name
            file_mappings.append((src, dst))
    
    # Check for Redshift files
    redshift_sources = [
        DATA_DIR / "redshift" / "redshift_customer_behavior_50000.csv",
    ]
    
    for src in redshift_sources:
        if src.exists():
            dst = SAMPLES_DIR / "redshift" / src.name
            file_mappings.append((src, dst))
    
    # Check for FX files
    fx_sources = [
        DATA_DIR / "fx" / "fx_rates_historical.json",
        DATA_DIR / "fx" / "fx_rates_historical.csv",
    ]
    
    for src in fx_sources:
        if src.exists():
            dst = SAMPLES_DIR / "fx" / src.name
            file_mappings.append((src, dst))
    
    # Check for Kafka files
    kafka_sources = [
        DATA_DIR / "kafka" / "stream_kafka_events_100000.csv",
    ]
    
    for src in kafka_sources:
        if src.exists():
            dst = SAMPLES_DIR / "kafka" / src.name
            file_mappings.append((src, dst))
    
    # Also check root data/ directory for files
    for file in DATA_DIR.glob("*.csv"):
        # Try to identify and move to appropriate directory
        if "crm" in file.name.lower() or "account" in file.name.lower() or "contact" in file.name.lower():
            dst = SAMPLES_DIR / "crm" / file.name
            file_mappings.append((file, dst))
        elif "snowflake" in file.name.lower() or "customer" in file.name.lower() or "order" in file.name.lower() or "product" in file.name.lower():
            dst = SAMPLES_DIR / "snowflake" / file.name
            file_mappings.append((file, dst))
        elif "redshift" in file.name.lower() or "behavior" in file.name.lower():
            dst = SAMPLES_DIR / "redshift" / file.name
            file_mappings.append((file, dst))
        elif "fx" in file.name.lower() or "rate" in file.name.lower():
            dst = SAMPLES_DIR / "fx" / file.name
            file_mappings.append((file, dst))
        elif "kafka" in file.name.lower() or "stream" in file.name.lower() or "event" in file.name.lower():
            dst = SAMPLES_DIR / "kafka" / file.name
            file_mappings.append((file, dst))
    
    for file in DATA_DIR.glob("*.json"):
        if "fx" in file.name.lower() or "rate" in file.name.lower():
            dst = SAMPLES_DIR / "fx" / file.name
            file_mappings.append((file, dst))
    
    # Move files
    moved_count = 0
    for src, dst in file_mappings:
        if src.exists() and not dst.exists():
            try:
                shutil.move(str(src), str(dst))
                logger.info(f"✅ Moved: {src.name} → {dst.relative_to(PROJECT_ROOT)}")
                moved_count += 1
            except Exception as e:
                logger.error(f"❌ Failed to move {src}: {e}")
        elif dst.exists():
            logger.info(f"⏭️  Skipped (already exists): {dst.name}")
    
    logger.info(f"✅ Moved {moved_count} files")
    
    # Clean up empty directories (but keep bronze, silver, gold for ETL output)
    keep_dirs = {"bronze", "silver", "gold", "checkpoints", "_dq_results", "_checkpoints", "samples", "metrics"}
    
    for item in DATA_DIR.iterdir():
        if item.is_dir() and item.name not in keep_dirs:
            try:
                # Check if directory is empty
                if not any(item.iterdir()):
                    item.rmdir()
                    logger.info(f"✅ Removed empty directory: {item.name}")
                else:
                    logger.info(f"⚠️  Directory not empty, keeping: {item.name}")
            except Exception as e:
                logger.warning(f"Could not remove {item}: {e}")
    
    logger.info("✅ Data directory organization complete!")
    logger.info(f"📁 New structure: {SAMPLES_DIR.relative_to(PROJECT_ROOT)}/")
    
    # Show final structure
    logger.info("\n📋 Final structure:")
    for dir_name in ["crm", "snowflake", "redshift", "fx", "kafka"]:
        dir_path = SAMPLES_DIR / dir_name
        if dir_path.exists():
            files = list(dir_path.glob("*"))
            logger.info(f"  {dir_name}/: {len(files)} files")


if __name__ == "__main__":
    organize_data_directory()

