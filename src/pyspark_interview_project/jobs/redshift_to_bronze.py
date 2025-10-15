"""
Redshift Data Extraction Module
Extracts data from Amazon Redshift data warehouse and loads into Bronze layer.
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

logger = logging.getLogger(__name__)


class RedshiftExtractor:
    """
    Handles data extraction from Amazon Redshift data warehouse.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.redshift_config = config.get("data_sources", {}).get("redshift", {})
        
        # Redshift connection parameters
        self.cluster_id = self.redshift_config.get("cluster_identifier")
        self.database = self.redshift_config.get("database")
        self.port = self.redshift_config.get("port", 5439)
        self.username = self.redshift_config.get("username")
        self.password = self.redshift_config.get("password")
        self.schema = self.redshift_config.get("schema", "public")
        
        # Build JDBC URL
        self.jdbc_url = self.redshift_config.get("jdbc_url") or \
            f"jdbc:redshift://{self.cluster_id}.{os.getenv('AWS_REGION', 'us-east-1')}.redshift.amazonaws.com:{self.port}/{self.database}"
        
        # Connection properties
        self.connection_properties = {
            "user": self.username,
            "password": self.password,
            "driver": "com.amazon.redshift.jdbc42.Driver"
        }
        
        logger.info(f"RedshiftExtractor initialized for cluster: {self.cluster_id}")
    
    def extract_table_data(
        self, 
        table_name: str, 
        query: Optional[str] = None,
        batch_size: int = 10000
    ) -> DataFrame:
        """
        Extract data from a Redshift table.
        
        Args:
            table_name: Name of the table to extract
            query: Custom SQL query (optional)
            batch_size: Batch size for extraction
            
        Returns:
            Spark DataFrame with Redshift data
        """
        logger.info(f"Extracting data from Redshift table: {table_name}")
        
        try:
            # Use custom query if provided, otherwise select all from table
            if query:
                sql_query = query
            else:
                sql_query = f"SELECT * FROM {self.schema}.{table_name}"
            
            # Extract data using JDBC
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", sql_query) \
                .option("fetchsize", batch_size) \
                .options(**self.connection_properties) \
                .load()
            
            # Add metadata columns
            df = self._add_metadata_columns(df, "redshift", table_name)
            
            logger.info(f"Successfully extracted {df.count()} records from {table_name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract data from Redshift table {table_name}: {e}")
            raise
    
    def extract_marketing_campaigns(self) -> DataFrame:
        """Extract marketing campaigns data from Redshift."""
        query = """
        SELECT 
            campaign_id,
            campaign_name,
            campaign_type,
            status,
            start_date,
            end_date,
            budget,
            spend,
            impressions,
            clicks,
            conversions,
            created_at,
            updated_at
        FROM marketing_campaigns
        WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY updated_at DESC
        """
        return self.extract_table_data("marketing_campaigns", query)
    
    def extract_customer_behavior(self) -> DataFrame:
        """Extract customer behavior analytics from Redshift."""
        query = """
        SELECT 
            user_id,
            event_name,
            event_timestamp,
            session_id,
            page_url,
            referrer,
            device_type,
            browser,
            os,
            country,
            city,
            properties
        FROM customer_behavior
        WHERE event_timestamp >= CURRENT_DATE - INTERVAL '1 day'
        ORDER BY event_timestamp DESC
        """
        return self.extract_table_data("customer_behavior", query)
    
    def extract_web_analytics(self) -> DataFrame:
        """Extract web analytics data from Redshift."""
        query = """
        SELECT 
            date,
            page_path,
            page_title,
            sessions,
            users,
            pageviews,
            bounce_rate,
            avg_session_duration,
            goal_completions,
            revenue
        FROM web_analytics
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY date DESC
        """
        return self.extract_table_data("web_analytics", query)
    
    def _add_metadata_columns(self, df: DataFrame, source: str, table_name: str) -> DataFrame:
        """Add metadata columns to the DataFrame."""
        return df.withColumn("_source_system", lit(source)) \
                 .withColumn("_source_table", lit(table_name)) \
                 .withColumn("_extraction_timestamp", current_timestamp()) \
                 .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    
    def validate_data_quality(self, df: DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Perform data quality checks on extracted data.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table for logging
            
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Performing data quality checks for {table_name}")
        
        validation_results = {
            "table_name": table_name,
            "total_records": df.count(),
            "null_checks": {},
            "duplicate_checks": {},
            "schema_validation": True
        }
        
        try:
            # Check for null values in key columns
            for col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                validation_results["null_checks"][col_name] = null_count
            
            # Check for duplicates (if primary key exists)
            if "id" in df.columns:
                duplicate_count = df.count() - df.dropDuplicates(["id"]).count()
                validation_results["duplicate_checks"]["id_duplicates"] = duplicate_count
            
            # Schema validation
            if df.count() == 0:
                validation_results["schema_validation"] = False
                logger.warning(f"No data found in {table_name}")
            
            logger.info(f"Data quality validation completed for {table_name}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Data quality validation failed for {table_name}: {e}")
            validation_results["error"] = str(e)
            return validation_results


def extract_redshift_data(
    spark: SparkSession,
    config: Dict[str, Any],
    table_name: str,
    custom_query: Optional[str] = None
) -> DataFrame:
    """
    Main function to extract data from Redshift.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
        table_name: Name of the table to extract
        custom_query: Optional custom SQL query
        
    Returns:
        Spark DataFrame with Redshift data
    """
    extractor = RedshiftExtractor(spark, config)
    
    # Map table names to specific extraction methods
    extraction_methods = {
        "marketing_campaigns": extractor.extract_marketing_campaigns,
        "customer_behavior": extractor.extract_customer_behavior,
        "web_analytics": extractor.extract_web_analytics
    }
    
    if table_name in extraction_methods:
        df = extraction_methods[table_name]()
    else:
        df = extractor.extract_table_data(table_name, custom_query)
    
    # Perform data quality validation
    validation_results = extractor.validate_data_quality(df, table_name)
    logger.info(f"Validation results for {table_name}: {validation_results}")
    
    return df


def save_redshift_data_to_bronze(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    config: Dict[str, Any]
) -> None:
    """
    Save extracted Redshift data to Bronze layer.
    
    Args:
        spark: Spark session
        df: DataFrame to save
        table_name: Name of the source table
        config: Configuration dictionary
    """
    logger.info(f"Saving Redshift data to Bronze layer: {table_name}")
    
    try:
        # Get Bronze layer path from config
        bronze_path = config.get("output", {}).get("bronze_path", "data/lakehouse/bronze")
        redshift_bronze_path = f"{bronze_path}/redshift/{table_name}"
        
        # Write to Delta format
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(redshift_bronze_path)
        
        logger.info(f"Successfully saved {df.count()} records to {redshift_bronze_path}")
        
    except Exception as e:
        logger.error(f"Failed to save Redshift data to Bronze layer: {e}")
        raise


def main():
    """Main execution function for Redshift data extraction."""
    import sys
    from pyspark_interview_project.utils.spark_session import build_spark
    from pyspark_interview_project.config_loader import load_config
    
    # Load configuration
    config = load_config()
    
    # Build Spark session
    spark = build_spark(config)
    
    try:
        # Extract data from different Redshift tables
        tables_to_extract = ["marketing_campaigns", "customer_behavior", "web_analytics"]
        
        for table_name in tables_to_extract:
            logger.info(f"Processing Redshift table: {table_name}")
            
            # Extract data
            df = extract_redshift_data(spark, config, table_name)
            
            # Save to Bronze layer
            save_redshift_data_to_bronze(spark, df, table_name, config)
            
            logger.info(f"Completed processing for {table_name}")
        
        logger.info("Redshift data extraction completed successfully")
        
    except Exception as e:
        logger.error(f"Redshift data extraction failed: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
