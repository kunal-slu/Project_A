#!/usr/bin/env python3
"""
Great Expectations data quality checks runner.
Runs GE validations in Spark and writes results to S3.
"""

import os
import sys
import json
import logging
import yaml
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pyspark_interview_project.utils.spark import get_spark_session
from pyspark_interview_project.utils.config import load_conf

logger = logging.getLogger(__name__)


def load_expectation_suite(suite_path: str) -> Dict[str, Any]:
    """
    Load expectation suite from YAML file.
    
    Args:
        suite_path: Path to the expectation suite YAML file
        
    Returns:
        Expectation suite dictionary
    """
    with open(suite_path, 'r') as f:
        return yaml.safe_load(f)


def run_yaml_policy(df, policy: dict, key_cols=None):
    """
    Run YAML-based data quality policy checks.
    
    Args:
        df: Spark DataFrame to check
        policy: YAML policy configuration
        key_cols: Optional list of key columns (if not provided, uses policy.get("unique"))
        
    Returns:
        Results dictionary with check outcomes
    """
    # Use key_cols if provided, else try policy.get("unique")
    uniq = key_cols or policy.get("unique") or []
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "checks": [],
        "summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "critical_failures": 0
        }
    }
    
    # Run uniqueness checks if specified
    if uniq:
        for col in uniq:
            if col in df.columns:
                total_count = df.count()
                unique_count = df.select(col).distinct().count()
                if total_count == unique_count:
                    results["checks"].append({
                        "check": f"uniqueness_{col}",
                        "passed": True,
                        "severity": "critical"
                    })
                    results["summary"]["passed"] += 1
                else:
                    results["checks"].append({
                        "check": f"uniqueness_{col}",
                        "passed": False,
                        "severity": "critical",
                        "error": f"Column '{col}' has {total_count - unique_count} duplicate values"
                    })
                    results["summary"]["failed"] += 1
                    results["summary"]["critical_failures"] += 1
                results["summary"]["total_checks"] += 1
    
    # Run null checks
    for col in policy.get("not_null", []):
        if col in df.columns:
            null_count = df.filter(df[col].isNull()).count()
            if null_count == 0:
                results["checks"].append({
                    "check": f"not_null_{col}",
                    "passed": True,
                    "severity": "critical"
                })
                results["summary"]["passed"] += 1
            else:
                results["checks"].append({
                    "check": f"not_null_{col}",
                    "passed": False,
                    "severity": "critical",
                    "error": f"Column '{col}' has {null_count} null values"
                })
                results["summary"]["failed"] += 1
                results["summary"]["critical_failures"] += 1
            results["summary"]["total_checks"] += 1
    
    # Run range checks
    for col, range_config in policy.get("range", {}).items():
        if col in df.columns:
            min_val = range_config.get("min")
            max_val = range_config.get("max")
            
            condition = df[col].isNotNull()
            if min_val is not None:
                condition = condition & (df[col] >= min_val)
            if max_val is not None:
                condition = condition & (df[col] <= max_val)
                
            invalid_count = df.filter(~condition).count()
            if invalid_count == 0:
                results["checks"].append({
                    "check": f"range_{col}",
                    "passed": True,
                    "severity": "warning"
                })
                results["summary"]["passed"] += 1
            else:
                results["checks"].append({
                    "check": f"range_{col}",
                    "passed": False,
                    "severity": "warning",
                    "error": f"Column '{col}' has {invalid_count} values outside range [{min_val}, {max_val}]"
                })
                results["summary"]["failed"] += 1
            results["summary"]["total_checks"] += 1
    
    return results


def run_expectation_checks(
    spark,
    table_location: str,
    suite: Dict[str, Any],
    table_name: str
) -> Dict[str, Any]:
    """
    Run expectation checks on a table.
    
    Args:
        spark: Spark session
        table_location: S3 location of the Delta table
        suite: Expectation suite configuration
        table_name: Name of the table being checked
        
    Returns:
        Results dictionary with check outcomes
    """
    logger.info(f"Running DQ checks on {table_name} at {table_location}")
    
    # Read the Delta table
    df = spark.read.format("delta").load(table_location)
    
    results = {
        "table_name": table_name,
        "table_location": table_location,
        "timestamp": datetime.now().isoformat(),
        "checks": [],
        "summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "critical_failures": 0
        }
    }
    
    # Run each expectation
    for expectation in suite.get("expectations", []):
        check_result = run_single_expectation(df, expectation, table_name)
        results["checks"].append(check_result)
        results["summary"]["total_checks"] += 1
        
        if check_result["passed"]:
            results["summary"]["passed"] += 1
        else:
            results["summary"]["failed"] += 1
            if check_result.get("severity") == "critical":
                results["summary"]["critical_failures"] += 1
    
    return results


def run_single_expectation(df, expectation: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """
    Run a single expectation check.
    
    Args:
        df: Spark DataFrame
        expectation: Expectation configuration
        table_name: Name of the table being checked
        
    Returns:
        Check result dictionary
    """
    expectation_type = expectation["expectation_type"]
    kwargs = expectation.get("kwargs", {})
    meta = expectation.get("meta", {})
    severity = meta.get("severity", "warning")
    
    check_result = {
        "expectation_type": expectation_type,
        "kwargs": kwargs,
        "severity": severity,
        "description": meta.get("description", ""),
        "passed": False,
        "error": None
    }
    
    try:
        if expectation_type == "expect_column_to_exist":
            column = kwargs["column"]
            if column in df.columns:
                check_result["passed"] = True
            else:
                check_result["error"] = f"Column '{column}' does not exist"
                
        elif expectation_type == "expect_column_values_to_not_be_null":
            column = kwargs["column"]
            null_count = df.filter(df[column].isNull()).count()
            if null_count == 0:
                check_result["passed"] = True
            else:
                check_result["error"] = f"Column '{column}' has {null_count} null values"
                
        elif expectation_type == "expect_column_values_to_be_unique":
            column = kwargs["column"]
            total_count = df.count()
            unique_count = df.select(column).distinct().count()
            if total_count == unique_count:
                check_result["passed"] = True
            else:
                check_result["error"] = f"Column '{column}' has {total_count - unique_count} duplicate values"
                
        elif expectation_type == "expect_compound_columns_to_be_unique":
            columns = kwargs["column_list"]
            total_count = df.count()
            unique_count = df.select(*columns).distinct().count()
            if total_count == unique_count:
                check_result["passed"] = True
            else:
                check_result["error"] = f"Compound columns {columns} have {total_count - unique_count} duplicate values"
                
        elif expectation_type == "expect_column_values_to_be_between":
            column = kwargs["column"]
            min_value = kwargs.get("min_value")
            max_value = kwargs.get("max_value")
            
            # Count values outside range
            condition = df[column].isNotNull()
            if min_value is not None:
                condition = condition & (df[column] >= min_value)
            if max_value is not None:
                condition = condition & (df[column] <= max_value)
                
            invalid_count = df.filter(~condition).count()
            if invalid_count == 0:
                check_result["passed"] = True
            else:
                check_result["error"] = f"Column '{column}' has {invalid_count} values outside range [{min_value}, {max_value}]"
                
        elif expectation_type == "expect_column_values_to_be_of_type":
            column = kwargs["column"]
            expected_type = kwargs["type_"]
            actual_type = str(df.schema[column].dataType)
            
            # Map Spark types to expected types
            type_mapping = {
                "string": "StringType",
                "double": "DoubleType",
                "integer": "IntegerType",
                "date": "DateType",
                "timestamp": "TimestampType"
            }
            
            expected_spark_type = type_mapping.get(expected_type, expected_type)
            if expected_spark_type in actual_type:
                check_result["passed"] = True
            else:
                check_result["error"] = f"Column '{column}' has type {actual_type}, expected {expected_type}"
                
        else:
            check_result["error"] = f"Unsupported expectation type: {expectation_type}"
            
    except Exception as e:
        check_result["error"] = f"Error running expectation: {str(e)}"
    
    return check_result


def write_results_to_s3(results: Dict[str, Any], s3_path: str) -> None:
    """
    Write DQ results to S3.
    
    Args:
        results: DQ results dictionary
        s3_path: S3 path to write results
    """
    import boto3
    
    s3_client = boto3.client('s3')
    
    # Parse S3 path
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    
    bucket, key = s3_path.split('/', 1)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = results["table_name"]
    filename = f"dq_results_{table_name}_{timestamp}.json"
    
    full_key = f"{key}/{filename}"
    
    # Write results as JSON
    results_json = json.dumps(results, indent=2)
    
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=full_key,
            Body=results_json,
            ContentType='application/json'
        )
        logger.info(f"DQ results written to s3://{bucket}/{full_key}")
    except Exception as e:
        logger.error(f"Failed to write results to S3: {e}")
        raise


def main():
    """Main function to run DQ checks."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Great Expectations data quality checks")
    parser.add_argument("--lake-root", required=True, help="S3 root path of the data lake")
    parser.add_argument("--lake-bucket", required=True, help="S3 bucket for results")
    parser.add_argument("--suite", required=True, help="Path to expectation suite YAML file")
    parser.add_argument("--table", required=True, help="Table name to check")
    parser.add_argument("--layer", default="silver", help="Data layer (bronze, silver, gold)")
    parser.add_argument("--config", help="Configuration file path")
    args = parser.parse_args()
    
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Load configuration if provided
        if args.config:
            config = load_conf(args.config)
            lake_root = config.get("lake", {}).get("root", args.lake_root)
        else:
            lake_root = args.lake_root
        
        # Load expectation suite
        suite = load_expectation_suite(args.suite)
        
        # Create Spark session
        spark = get_spark_session("DataQualityChecks")
        
        # Build table location
        table_location = f"{lake_root}/{args.layer}/{args.table}"
        
        # Run DQ checks
        results = run_expectation_checks(spark, table_location, suite, args.table)
        
        # Write results to S3
        results_path = f"s3://{args.lake_bucket}/gold/quality/{args.layer}"
        write_results_to_s3(results, results_path)
        
        # Print summary
        summary = results["summary"]
        print(f"✅ DQ checks completed for {args.table}")
        print(f"   Total checks: {summary['total_checks']}")
        print(f"   Passed: {summary['passed']}")
        print(f"   Failed: {summary['failed']}")
        print(f"   Critical failures: {summary['critical_failures']}")
        
        # Exit with error code if critical failures
        if summary["critical_failures"] > 0:
            print(f"❌ {summary['critical_failures']} critical failures detected!")
            sys.exit(1)
        else:
            print("✅ All critical checks passed!")
        
    except Exception as e:
        logger.error(f"DQ checks failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()