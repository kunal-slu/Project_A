"""
FX rates ingestion job - REST API to Bronze layer.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from project_a.utils.config import load_conf
from project_a.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def fetch_fx_rates(base_currency: str = "USD", days_back: int = 7) -> list[dict[str, Any]]:
    """
    Legacy helper kept for compatibility. In this project, FX rates are
    expected to be landed to Bronze by a separate integration process, so
    this function is no longer used by EMR Serverless jobs.

    It now returns an empty list to avoid any dependency on external HTTP
    libraries like ``requests``.
    """
    logger.info(
        "fetch_fx_rates() is deprecated in this project; FX data should be "
        "landed to Bronze by an upstream integration job."
    )
    return []


def create_fx_schema() -> StructType:
    """Create schema for FX rates data."""
    return StructType([
        StructField("ccy", StringType(), False),
        StructField("rate_to_base", DoubleType(), False),
        StructField("as_of_date", DateType(), False),
        StructField("base_currency", StringType(), False),
        StructField("ingestion_timestamp", StringType(), False),
        StructField("_proc_date", StringType(), False)
    ])


def main():
    """Main entry point for FX to Bronze job."""
    parser = argparse.ArgumentParser(description="FX rates ingestion to Bronze layer")
    parser.add_argument("--config", required=True, help="Configuration file path")
    parser.add_argument("--lake-root", required=True, help="Data lake root path")
    parser.add_argument("--days-back", type=int, default=7, help="Days to fetch back")
    parser.add_argument("--base-currency", default="USD", help="Base currency")

    args = parser.parse_args()

    # Setup logging
    setup_json_logging()
    logger.info("Starting FX to Bronze ingestion job")

    # Load configuration
    load_conf(args.config)
    lake_root = args.lake_root

    # Create Spark session with Delta config
    spark = get_spark_session(
        app_name="fx-to-bronze",
        config=get_delta_config()
    )

    try:
        # Fetch FX rates
        fx_rates = fetch_fx_rates(args.base_currency, args.days_back)

        if not fx_rates:
            logger.warning("No FX rates fetched, exiting")
            return

        # Create DataFrame
        df = spark.createDataFrame(fx_rates, create_fx_schema())

        # Add processing metadata
        df = df.withColumn("_proc_date", lit(datetime.now().strftime("%Y-%m-%d")))
        df = df.withColumn("_proc_timestamp", current_timestamp())

        # Write to Bronze layer
        bronze_path = f"{lake_root}/bronze/fx_rates"
        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(bronze_path)

        logger.info(f"Successfully wrote {df.count()} records to {bronze_path}")

        # Show sample data
        df.show(5, truncate=False)

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
