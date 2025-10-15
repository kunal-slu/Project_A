"""
Data Quality Checks for External Data Sources (Redshift and HubSpot)
"""

import logging
from typing import Dict, List, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, isnan, isnull, when, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

logger = logging.getLogger(__name__)


class ExternalDataQualityChecker:
    """
    Data quality checker for external data sources (Redshift and HubSpot).
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        
    def check_redshift_data_quality(self, df: DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Perform data quality checks on Redshift data.
        
        Args:
            df: DataFrame to check
            table_name: Name of the Redshift table
            
        Returns:
            Dictionary with quality check results
        """
        logger.info(f"Performing data quality checks for Redshift table: {table_name}")
        
        quality_results = {
            "table_name": table_name,
            "source": "redshift",
            "total_records": df.count(),
            "checks": {}
        }
        
        try:
            # Basic record count check
            record_count = df.count()
            quality_results["checks"]["record_count"] = {
                "value": record_count,
                "status": "pass" if record_count > 0 else "fail",
                "message": f"Found {record_count} records"
            }
            
            # Check for null values in key columns
            if table_name == "marketing_campaigns":
                key_columns = ["campaign_id", "campaign_name", "status"]
            elif table_name == "customer_behavior":
                key_columns = ["user_id", "event_name", "event_timestamp"]
            elif table_name == "web_analytics":
                key_columns = ["date", "page_path", "sessions"]
            else:
                key_columns = ["id"]  # Generic fallback
            
            for col_name in key_columns:
                if col_name in df.columns:
                    null_count = df.filter(col(col_name).isNull()).count()
                    null_percentage = (null_count / record_count * 100) if record_count > 0 else 0
                    
                    quality_results["checks"][f"{col_name}_nulls"] = {
                        "value": null_count,
                        "percentage": round(null_percentage, 2),
                        "status": "pass" if null_percentage < 10 else "warn" if null_percentage < 25 else "fail",
                        "message": f"{null_percentage:.2f}% null values in {col_name}"
                    }
            
            # Check for duplicate records
            if "id" in df.columns:
                duplicate_count = record_count - df.dropDuplicates(["id"]).count()
                duplicate_percentage = (duplicate_count / record_count * 100) if record_count > 0 else 0
                
                quality_results["checks"]["duplicates"] = {
                    "value": duplicate_count,
                    "percentage": round(duplicate_percentage, 2),
                    "status": "pass" if duplicate_percentage < 5 else "warn" if duplicate_percentage < 15 else "fail",
                    "message": f"{duplicate_percentage:.2f}% duplicate records"
                }
            
            # Check data freshness (if timestamp columns exist)
            timestamp_columns = [col for col in df.columns if "timestamp" in col.lower() or "date" in col.lower()]
            if timestamp_columns:
                for ts_col in timestamp_columns[:1]:  # Check first timestamp column
                    try:
                        # Get the most recent timestamp
                        max_timestamp = df.agg({ts_col: "max"}).collect()[0][0]
                        if max_timestamp:
                            quality_results["checks"]["data_freshness"] = {
                                "value": str(max_timestamp),
                                "status": "pass",
                                "message": f"Most recent data: {max_timestamp}"
                            }
                    except Exception as e:
                        quality_results["checks"]["data_freshness"] = {
                            "value": None,
                            "status": "warn",
                            "message": f"Could not check data freshness: {e}"
                        }
            
            # Check for negative values in numeric columns
            numeric_columns = [col for col in df.columns if df.schema[col].dataType in [IntegerType(), DoubleType()]]
            for num_col in numeric_columns[:3]:  # Check first 3 numeric columns
                try:
                    negative_count = df.filter(col(num_col) < 0).count()
                    if negative_count > 0:
                        quality_results["checks"][f"{num_col}_negatives"] = {
                            "value": negative_count,
                            "status": "warn",
                            "message": f"Found {negative_count} negative values in {num_col}"
                        }
                except Exception:
                    pass  # Skip if column can't be compared
            
            logger.info(f"Data quality checks completed for {table_name}")
            return quality_results
            
        except Exception as e:
            logger.error(f"Data quality check failed for {table_name}: {e}")
            quality_results["error"] = str(e)
            return quality_results
    
    def check_hubspot_data_quality(self, df: DataFrame, endpoint_name: str) -> Dict[str, Any]:
        """
        Perform data quality checks on HubSpot data.
        
        Args:
            df: DataFrame to check
            endpoint_name: Name of the HubSpot endpoint
            
        Returns:
            Dictionary with quality check results
        """
        logger.info(f"Performing data quality checks for HubSpot endpoint: {endpoint_name}")
        
        quality_results = {
            "table_name": endpoint_name,
            "source": "hubspot",
            "total_records": df.count(),
            "checks": {}
        }
        
        try:
            # Basic record count check
            record_count = df.count()
            quality_results["checks"]["record_count"] = {
                "value": record_count,
                "status": "pass" if record_count > 0 else "fail",
                "message": f"Found {record_count} records"
            }
            
            # Check for null values in key columns based on endpoint
            if endpoint_name == "contacts":
                key_columns = ["id", "properties.email", "properties.firstname"]
            elif endpoint_name == "deals":
                key_columns = ["id", "properties.dealname", "properties.amount"]
            elif endpoint_name == "companies":
                key_columns = ["id", "properties.name", "properties.domain"]
            elif endpoint_name == "tickets":
                key_columns = ["id", "properties.subject", "properties.hs_ticket_priority"]
            else:
                key_columns = ["id"]  # Generic fallback
            
            for col_name in key_columns:
                if col_name in df.columns:
                    null_count = df.filter(col(col_name).isNull()).count()
                    null_percentage = (null_count / record_count * 100) if record_count > 0 else 0
                    
                    quality_results["checks"][f"{col_name}_nulls"] = {
                        "value": null_count,
                        "percentage": round(null_percentage, 2),
                        "status": "pass" if null_percentage < 20 else "warn" if null_percentage < 40 else "fail",
                        "message": f"{null_percentage:.2f}% null values in {col_name}"
                    }
            
            # Check for duplicate records by ID
            if "id" in df.columns:
                duplicate_count = record_count - df.dropDuplicates(["id"]).count()
                duplicate_percentage = (duplicate_count / record_count * 100) if record_count > 0 else 0
                
                quality_results["checks"]["duplicates"] = {
                    "value": duplicate_count,
                    "percentage": round(duplicate_percentage, 2),
                    "status": "pass" if duplicate_percentage < 1 else "warn" if duplicate_percentage < 5 else "fail",
                    "message": f"{duplicate_percentage:.2f}% duplicate records"
                }
            
            # Check for required properties based on endpoint
            if endpoint_name == "contacts":
                # Check for email format (basic validation)
                if "properties.email" in df.columns:
                    email_count = df.filter(col("properties.email").isNotNull()).count()
                    quality_results["checks"]["email_presence"] = {
                        "value": email_count,
                        "status": "pass" if email_count > 0 else "warn",
                        "message": f"Found {email_count} contacts with email addresses"
                    }
            
            elif endpoint_name == "deals":
                # Check for deal amount
                if "properties.amount" in df.columns:
                    amount_count = df.filter(col("properties.amount").isNotNull()).count()
                    quality_results["checks"]["amount_presence"] = {
                        "value": amount_count,
                        "status": "pass" if amount_count > 0 else "warn",
                        "message": f"Found {amount_count} deals with amounts"
                    }
            
            # Check data freshness
            if "createdAt" in df.columns:
                try:
                    max_created = df.agg({"createdAt": "max"}).collect()[0][0]
                    if max_created:
                        quality_results["checks"]["data_freshness"] = {
                            "value": str(max_created),
                            "status": "pass",
                            "message": f"Most recent record created: {max_created}"
                        }
                except Exception as e:
                    quality_results["checks"]["data_freshness"] = {
                        "value": None,
                        "status": "warn",
                        "message": f"Could not check data freshness: {e}"
                    }
            
            logger.info(f"Data quality checks completed for {endpoint_name}")
            return quality_results
            
        except Exception as e:
            logger.error(f"Data quality check failed for {endpoint_name}: {e}")
            quality_results["error"] = str(e)
            return quality_results
    
    def run_all_external_data_quality_checks(self) -> Dict[str, Any]:
        """
        Run data quality checks for all external data sources.
        
        Returns:
            Dictionary with all quality check results
        """
        logger.info("Running data quality checks for all external data sources")
        
        all_results = {
            "redshift": {},
            "hubspot": {},
            "summary": {
                "total_checks": 0,
                "passed": 0,
                "warnings": 0,
                "failed": 0
            }
        }
        
        try:
            # Get Bronze layer paths
            bronze_path = self.config.get("output", {}).get("bronze_path", "data/lakehouse/bronze")
            
            # Check Redshift data
            redshift_tables = ["marketing_campaigns", "customer_behavior", "web_analytics"]
            for table_name in redshift_tables:
                try:
                    table_path = f"{bronze_path}/redshift/{table_name}"
                    df = self.spark.read.format("delta").load(table_path)
                    results = self.check_redshift_data_quality(df, table_name)
                    all_results["redshift"][table_name] = results
                    
                    # Update summary
                    for check_name, check_result in results.get("checks", {}).items():
                        all_results["summary"]["total_checks"] += 1
                        status = check_result.get("status", "unknown")
                        if status == "pass":
                            all_results["summary"]["passed"] += 1
                        elif status == "warn":
                            all_results["summary"]["warnings"] += 1
                        elif status == "fail":
                            all_results["summary"]["failed"] += 1
                            
                except Exception as e:
                    logger.warning(f"Could not check Redshift table {table_name}: {e}")
            
            # Check HubSpot data
            hubspot_endpoints = ["contacts", "deals", "companies", "tickets"]
            for endpoint_name in hubspot_endpoints:
                try:
                    endpoint_path = f"{bronze_path}/hubspot/{endpoint_name}"
                    df = self.spark.read.format("delta").load(endpoint_path)
                    results = self.check_hubspot_data_quality(df, endpoint_name)
                    all_results["hubspot"][endpoint_name] = results
                    
                    # Update summary
                    for check_name, check_result in results.get("checks", {}).items():
                        all_results["summary"]["total_checks"] += 1
                        status = check_result.get("status", "unknown")
                        if status == "pass":
                            all_results["summary"]["passed"] += 1
                        elif status == "warn":
                            all_results["summary"]["warnings"] += 1
                        elif status == "fail":
                            all_results["summary"]["failed"] += 1
                            
                except Exception as e:
                    logger.warning(f"Could not check HubSpot endpoint {endpoint_name}: {e}")
            
            logger.info(f"External data quality checks completed. Summary: {all_results['summary']}")
            return all_results
            
        except Exception as e:
            logger.error(f"External data quality checks failed: {e}")
            all_results["error"] = str(e)
            return all_results


def main():
    """Main function to run external data quality checks."""
    import sys
    from pyspark_interview_project.utils.spark_session import build_spark
    from pyspark_interview_project.config_loader import load_config
    
    # Load configuration
    config = load_config()
    
    # Build Spark session
    spark = build_spark(config)
    
    try:
        # Run quality checks
        checker = ExternalDataQualityChecker(spark, config)
        results = checker.run_all_external_data_quality_checks()
        
        # Print results
        print("External Data Quality Check Results:")
        print("=" * 50)
        
        for source, tables in results.items():
            if source == "summary":
                continue
            print(f"\n{source.upper()}:")
            for table, checks in tables.items():
                print(f"  {table}:")
                for check_name, check_result in checks.get("checks", {}).items():
                    status = check_result.get("status", "unknown")
                    message = check_result.get("message", "")
                    print(f"    {check_name}: {status.upper()} - {message}")
        
        print(f"\nSUMMARY:")
        summary = results.get("summary", {})
        print(f"  Total checks: {summary.get('total_checks', 0)}")
        print(f"  Passed: {summary.get('passed', 0)}")
        print(f"  Warnings: {summary.get('warnings', 0)}")
        print(f"  Failed: {summary.get('failed', 0)}")
        
        # Exit with error code if there are failures
        if summary.get("failed", 0) > 0:
            print(f"\nERROR: {summary.get('failed', 0)} quality checks failed!")
            sys.exit(1)
        else:
            print("\nAll external data quality checks passed!")
        
    except Exception as e:
        logger.error(f"External data quality checks failed: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
