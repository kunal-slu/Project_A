"""
HubSpot API Integration Module
Extracts data from HubSpot CRM API and loads into Bronze layer.
"""

import logging
import requests
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType

logger = logging.getLogger(__name__)


class HubSpotAPI:
    """
    Handles API interactions with HubSpot CRM.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.hubspot_config = config.get("data_sources", {}).get("hubspot", {})
        
        self.base_url = self.hubspot_config.get("base_url", "https://api.hubapi.com")
        self.api_key = self.hubspot_config.get("api_key")
        self.rate_limit = self.hubspot_config.get("rate_limit", 100)  # requests per 10 seconds
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms between requests
        
        # Headers for API requests
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        logger.info("HubSpotAPI initialized")
    
    def _rate_limit_check(self):
        """Ensure we don't exceed rate limits."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def make_api_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make a request to HubSpot API with rate limiting.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            API response as dictionary
        """
        self._rate_limit_check()
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HubSpot API request failed for {endpoint}: {e}")
            raise
    
    def get_contacts(self, limit: int = 100, after: Optional[str] = None) -> Dict[str, Any]:
        """Get contacts from HubSpot CRM."""
        params = {
            "limit": limit,
            "properties": "email,firstname,lastname,phone,company,createdate,lastmodifieddate,lifecyclestage,leadstatus"
        }
        
        if after:
            params["after"] = after
        
        return self.make_api_request("/crm/v3/objects/contacts", params)
    
    def get_deals(self, limit: int = 100, after: Optional[str] = None) -> Dict[str, Any]:
        """Get deals from HubSpot CRM."""
        params = {
            "limit": limit,
            "properties": "dealname,amount,dealstage,closedate,createdate,lastmodifieddate,pipeline,dealtype"
        }
        
        if after:
            params["after"] = after
        
        return self.make_api_request("/crm/v3/objects/deals", params)
    
    def get_companies(self, limit: int = 100, after: Optional[str] = None) -> Dict[str, Any]:
        """Get companies from HubSpot CRM."""
        params = {
            "limit": limit,
            "properties": "name,domain,industry,city,state,country,createdate,lastmodifieddate,lifecyclestage"
        }
        
        if after:
            params["after"] = after
        
        return self.make_api_request("/crm/v3/objects/companies", params)
    
    def get_tickets(self, limit: int = 100, after: Optional[str] = None) -> Dict[str, Any]:
        """Get tickets from HubSpot Service Hub."""
        params = {
            "limit": limit,
            "properties": "subject,content,hs_ticket_priority,hs_ticket_category,hs_ticket_owner,createdate,lastmodifieddate,hs_resolution"
        }
        
        if after:
            params["after"] = after
        
        return self.make_api_request("/crm/v3/objects/tickets", params)


class HubSpotExtractor:
    """
    Handles data extraction from HubSpot CRM API.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.api = HubSpotAPI(config)
        
        logger.info("HubSpotExtractor initialized")
    
    def extract_contacts(self, batch_size: int = 100) -> DataFrame:
        """Extract contacts data from HubSpot."""
        logger.info("Extracting contacts from HubSpot")
        
        all_contacts = []
        after = None
        
        try:
            while True:
                response = self.api.get_contacts(limit=batch_size, after=after)
                contacts = response.get("results", [])
                
                if not contacts:
                    break
                
                all_contacts.extend(contacts)
                
                # Check if there are more pages
                paging = response.get("paging", {})
                after = paging.get("next", {}).get("after")
                
                if not after:
                    break
                
                logger.info(f"Extracted {len(contacts)} contacts, total: {len(all_contacts)}")
            
            # Convert to DataFrame
            df = self.spark.createDataFrame(all_contacts)
            df = self._add_metadata_columns(df, "hubspot", "contacts")
            
            logger.info(f"Successfully extracted {len(all_contacts)} contacts")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract contacts from HubSpot: {e}")
            raise
    
    def extract_deals(self, batch_size: int = 100) -> DataFrame:
        """Extract deals data from HubSpot."""
        logger.info("Extracting deals from HubSpot")
        
        all_deals = []
        after = None
        
        try:
            while True:
                response = self.api.get_deals(limit=batch_size, after=after)
                deals = response.get("results", [])
                
                if not deals:
                    break
                
                all_deals.extend(deals)
                
                # Check if there are more pages
                paging = response.get("paging", {})
                after = paging.get("next", {}).get("after")
                
                if not after:
                    break
                
                logger.info(f"Extracted {len(deals)} deals, total: {len(all_deals)}")
            
            # Convert to DataFrame
            df = self.spark.createDataFrame(all_deals)
            df = self._add_metadata_columns(df, "hubspot", "deals")
            
            logger.info(f"Successfully extracted {len(all_deals)} deals")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract deals from HubSpot: {e}")
            raise
    
    def extract_companies(self, batch_size: int = 100) -> DataFrame:
        """Extract companies data from HubSpot."""
        logger.info("Extracting companies from HubSpot")
        
        all_companies = []
        after = None
        
        try:
            while True:
                response = self.api.get_companies(limit=batch_size, after=after)
                companies = response.get("results", [])
                
                if not companies:
                    break
                
                all_companies.extend(companies)
                
                # Check if there are more pages
                paging = response.get("paging", {})
                after = paging.get("next", {}).get("after")
                
                if not after:
                    break
                
                logger.info(f"Extracted {len(companies)} companies, total: {len(all_companies)}")
            
            # Convert to DataFrame
            df = self.spark.createDataFrame(all_companies)
            df = self._add_metadata_columns(df, "hubspot", "companies")
            
            logger.info(f"Successfully extracted {len(all_companies)} companies")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract companies from HubSpot: {e}")
            raise
    
    def extract_tickets(self, batch_size: int = 100) -> DataFrame:
        """Extract tickets data from HubSpot Service Hub."""
        logger.info("Extracting tickets from HubSpot")
        
        all_tickets = []
        after = None
        
        try:
            while True:
                response = self.api.get_tickets(limit=batch_size, after=after)
                tickets = response.get("results", [])
                
                if not tickets:
                    break
                
                all_tickets.extend(tickets)
                
                # Check if there are more pages
                paging = response.get("paging", {})
                after = paging.get("next", {}).get("after")
                
                if not after:
                    break
                
                logger.info(f"Extracted {len(tickets)} tickets, total: {len(all_tickets)}")
            
            # Convert to DataFrame
            df = self.spark.createDataFrame(all_tickets)
            df = self._add_metadata_columns(df, "hubspot", "tickets")
            
            logger.info(f"Successfully extracted {len(all_tickets)} tickets")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract tickets from HubSpot: {e}")
            raise
    
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
            "schema_validation": True
        }
        
        try:
            # Check for null values in key columns
            for col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                validation_results["null_checks"][col_name] = null_count
            
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


def extract_hubspot_data(
    spark: SparkSession,
    config: Dict[str, Any],
    endpoint_name: str,
    batch_size: int = 100
) -> DataFrame:
    """
    Main function to extract data from HubSpot.
    
    Args:
        spark: Spark session
        config: Configuration dictionary
        endpoint_name: Name of the endpoint to extract (contacts, deals, companies, tickets)
        batch_size: Batch size for API requests
        
    Returns:
        Spark DataFrame with HubSpot data
    """
    extractor = HubSpotExtractor(spark, config)
    
    # Map endpoint names to specific extraction methods
    extraction_methods = {
        "contacts": extractor.extract_contacts,
        "deals": extractor.extract_deals,
        "companies": extractor.extract_companies,
        "tickets": extractor.extract_tickets
    }
    
    if endpoint_name not in extraction_methods:
        raise ValueError(f"Unknown endpoint: {endpoint_name}")
    
    df = extraction_methods[endpoint_name](batch_size)
    
    # Perform data quality validation
    validation_results = extractor.validate_data_quality(df, endpoint_name)
    logger.info(f"Validation results for {endpoint_name}: {validation_results}")
    
    return df


def save_hubspot_data_to_bronze(
    spark: SparkSession,
    df: DataFrame,
    endpoint_name: str,
    config: Dict[str, Any]
) -> None:
    """
    Save extracted HubSpot data to Bronze layer.
    
    Args:
        spark: Spark session
        df: DataFrame to save
        endpoint_name: Name of the source endpoint
        config: Configuration dictionary
    """
    logger.info(f"Saving HubSpot data to Bronze layer: {endpoint_name}")
    
    try:
        # Get Bronze layer path from config
        bronze_path = config.get("output", {}).get("bronze_path", "data/lakehouse/bronze")
        hubspot_bronze_path = f"{bronze_path}/hubspot/{endpoint_name}"
        
        # Write to Delta format
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(hubspot_bronze_path)
        
        logger.info(f"Successfully saved {df.count()} records to {hubspot_bronze_path}")
        
    except Exception as e:
        logger.error(f"Failed to save HubSpot data to Bronze layer: {e}")
        raise


def main():
    """Main execution function for HubSpot data extraction."""
    import sys
    from pyspark_interview_project.utils.spark_session import build_spark
    from pyspark_interview_project.config_loader import load_config
    
    # Load configuration
    config = load_config()
    
    # Build Spark session
    spark = build_spark(config)
    
    try:
        # Extract data from different HubSpot endpoints
        endpoints_to_extract = ["contacts", "deals", "companies", "tickets"]
        
        for endpoint_name in endpoints_to_extract:
            logger.info(f"Processing HubSpot endpoint: {endpoint_name}")
            
            # Extract data
            df = extract_hubspot_data(spark, config, endpoint_name)
            
            # Save to Bronze layer
            save_hubspot_data_to_bronze(spark, df, endpoint_name, config)
            
            logger.info(f"Completed processing for {endpoint_name}")
        
        logger.info("HubSpot data extraction completed successfully")
        
    except Exception as e:
        logger.error(f"HubSpot data extraction failed: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
