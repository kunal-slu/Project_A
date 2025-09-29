"""
FX rates ingestion job - REST API to Bronze layer.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from pyspark_interview_project.utils.spark import get_spark_session, get_delta_config
from pyspark_interview_project.utils.config import load_conf
from pyspark_interview_project.utils.logging import setup_json_logging

logger = logging.getLogger(__name__)


def fetch_fx_rates(base_currency: str = "USD", days_back: int = 7) -> list[Dict[str, Any]]:
    """
    Fetch FX rates from exchangerate.host API.
    
    Args:
        base_currency: Base currency for rates
        days_back: Number of days to fetch
        
    Returns:
        List of FX rate records
    """
    logger.info(f"Fetching FX rates for {base_currency} for {days_back} days")
    
    rates = []
    base_url = "https://api.exchangerate.host"
    
    for i in range(days_back):
        date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        url = f"{base_url}/{date}"
        
        try:
            response = requests.get(url, params={"base": base_currency}, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get("success"):
                for currency, rate in data.get("rates", {}).items():
                    rates.append({
                        "ccy": currency,
                        "rate_to_base": float(rate),
                        "as_of_date": date,
                        "base_currency": base_currency,
                        "ingestion_timestamp": datetime.now().isoformat()
                    })
                logger.info(f"Fetched {len(data.get('rates', {}))} rates for {date}")
            else:
                logger.warning(f"API returned success=false for {date}")
                
        except Exception as e:
            logger.error(f"Failed to fetch rates for {date}: {e}")
            continue
    
    logger.info(f"Total rates fetched: {len(rates)}")
    return rates


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
    config = load_conf(args.config)
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
