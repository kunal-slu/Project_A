#!/usr/bin/env python3
"""
Delete Week-Old Output Data Script

This script deletes output data that is older than 7 days from various Delta Lake tables.
"""

import sys
import os
import logging
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pyspark_interview_project.utils import get_spark_session, load_config

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_file_age_days(file_path):
    """Get the age of a file in days."""
    try:
        stat = os.stat(file_path)
        file_time = datetime.fromtimestamp(stat.st_mtime)
        age = datetime.now() - file_time
        return age.days
    except Exception as e:
        logger.error(f"Error getting file age for {file_path}: {str(e)}")
        return 0


def delete_old_files(directory, days_old=7):
    """Delete files older than specified days from a directory."""
    deleted_count = 0
    deleted_size = 0

    if not os.path.exists(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return deleted_count, deleted_size

    try:
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                age_days = get_file_age_days(file_path)

                if age_days > days_old:
                    try:
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        deleted_count += 1
                        deleted_size += file_size
                        logger.info(f"Deleted: {file_path} (age: {age_days} days, size: {file_size} bytes)")
                    except Exception as e:
                        logger.error(f"Failed to delete {file_path}: {str(e)}")

        # Remove empty directories
        for root, dirs, files in os.walk(directory, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.listdir(dir_path):  # Directory is empty
                        os.rmdir(dir_path)
                        logger.info(f"Removed empty directory: {dir_path}")
                except Exception as e:
                    logger.error(f"Failed to remove directory {dir_path}: {str(e)}")

    except Exception as e:
        logger.error(f"Error processing directory {directory}: {str(e)}")

    return deleted_count, deleted_size


def delete_old_delta_data(spark, table_path, date_column, days_old=7):
    """Delete old data from Delta tables based on date column."""
    try:
        if not os.path.exists(table_path):
            logger.warning(f"Delta table does not exist: {table_path}")
            return 0

        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_old)
        cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")

        # Read current data
        df = spark.read.format("delta").load(table_path)

        # Get initial count
        initial_count = df.count()

        # Filter out old data
        filtered_df = df.filter(df[date_column] >= cutoff_date_str)

        # Get final count
        final_count = filtered_df.count()
        deleted_count = initial_count - final_count

        if deleted_count > 0:
            # Write back filtered data
            filtered_df.write.format("delta").mode("overwrite").save(table_path)
            logger.info(f"Deleted {deleted_count} rows from {table_path} (older than {cutoff_date_str})")
        else:
            logger.info(f"No old data found in {table_path}")

        return deleted_count

    except Exception as e:
        logger.error(f"Failed to delete old data from {table_path}: {str(e)}")
        return 0


def cleanup_output_directories(days_old=7):
    """Clean up output directories by removing old files."""
    output_dirs = [
        "data/output_data",
        "data/lakehouse/bronze",
        "data/lakehouse/silver",
        "data/lakehouse/gold",
        "logs"
    ]

    total_deleted_files = 0
    total_deleted_size = 0

    logger.info(f"Cleaning up files older than {days_old} days...")

    for directory in output_dirs:
        if os.path.exists(directory):
            deleted_files, deleted_size = delete_old_files(directory, days_old)
            total_deleted_files += deleted_files
            total_deleted_size += deleted_size

            if deleted_files > 0:
                logger.info(f"Directory {directory}: Deleted {deleted_files} files ({deleted_size} bytes)")
        else:
            logger.info(f"Directory {directory}: Does not exist, skipping")

    return total_deleted_files, total_deleted_size


def cleanup_delta_tables(spark, days_old=7):
    """Clean up Delta tables by removing old data based on date columns."""
    delta_tables = [
        {
            "path": "data/lakehouse/silver/orders_cleansed",
            "date_column": "order_date"
        },
        {
            "path": "data/lakehouse/gold/fact_orders",
            "date_column": "order_date"
        },
        {
            "path": "data/lakehouse/silver/dim_customers_scd2",
            "date_column": "effective_from"
        }
    ]

    total_deleted_rows = 0

    logger.info(f"Cleaning up Delta tables older than {days_old} days...")

    for table in delta_tables:
        deleted_rows = delete_old_delta_data(
            spark,
            table["path"],
            table["date_column"],
            days_old
        )
        total_deleted_rows += deleted_rows

    return total_deleted_rows


def get_directory_stats(directory):
    """Get statistics about a directory."""
    if not os.path.exists(directory):
        return {"exists": False, "files": 0, "size": 0}

    file_count = 0
    total_size = 0

    try:
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    total_size += os.path.getsize(file_path)
                    file_count += 1
                except:
                    pass
    except Exception as e:
        logger.error(f"Error getting stats for {directory}: {str(e)}")

    return {
        "exists": True,
        "files": file_count,
        "size": total_size
    }


def main():
    """Main function to delete week-old output data."""

    # Load configuration
    config_path = "config/config-dev.yaml"
    cfg = load_config(config_path)

    # Create Spark session
    spark = get_spark_session(cfg)

    try:
        logger.info("=== Delete Week-Old Output Data ===")

        # Show initial statistics
        logger.info("\n📊 Initial directory statistics:")
        directories = [
            "data/output_data",
            "data/lakehouse/bronze",
            "data/lakehouse/silver",
            "data/lakehouse/gold",
            "logs"
        ]

        total_initial_files = 0
        total_initial_size = 0

        for directory in directories:
            stats = get_directory_stats(directory)
            if stats["exists"]:
                logger.info(f"📁 {directory}: {stats['files']} files, {stats['size']} bytes")
                total_initial_files += stats["files"]
                total_initial_size += stats["size"]
            else:
                logger.info(f"📁 {directory}: Does not exist")

        logger.info(f"\n📊 Total initial: {total_initial_files} files, {total_initial_size} bytes")

        # Clean up output directories
        logger.info("\n🧹 Step 1: Cleaning up output directories...")
        deleted_files, deleted_size = cleanup_output_directories(days_old=7)

        # Clean up Delta tables
        logger.info("\n🧹 Step 2: Cleaning up Delta tables...")
        deleted_rows = cleanup_delta_tables(spark, days_old=7)

        # Show final statistics
        logger.info("\n📊 Final directory statistics:")
        total_final_files = 0
        total_final_size = 0

        for directory in directories:
            stats = get_directory_stats(directory)
            if stats["exists"]:
                logger.info(f"📁 {directory}: {stats['files']} files, {stats['size']} bytes")
                total_final_files += stats["files"]
                total_final_size += stats["size"]
            else:
                logger.info(f"📁 {directory}: Does not exist")

        logger.info(f"\n📊 Total final: {total_final_files} files, {total_final_size} bytes")

        # Summary
        logger.info("\n📈 Cleanup Summary:")
        logger.info(f"🗑️  Files deleted: {deleted_files}")
        logger.info(f"🗑️  Bytes freed: {deleted_size}")
        logger.info(f"🗑️  Delta rows deleted: {deleted_rows}")
        logger.info(f"💾 Space saved: {total_initial_size - total_final_size} bytes")

        logger.info("\n✅ Week-old data cleanup completed successfully!")

    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
