"""
Data quality utilities for Great Expectations integration.

Provides helper functions for running DQ checks, validating data,
and generating DQ reports.
"""

import logging
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


def run_dq_suite(
    spark: SparkSession,
    df: DataFrame,
    suite_name: str,
    expectations: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Run a data quality suite on a DataFrame.
    
    Args:
        spark: SparkSession
        df: DataFrame to check
        suite_name: Name of the DQ suite
        expectations: List of expectation dictionaries
        
    Returns:
        Dictionary with validation results
    """
    logger.info(f"Running DQ suite: {suite_name}")
    
    results = {
        "suite_name": suite_name,
        "total_expectations": len(expectations),
        "passed": 0,
        "failed": 0,
        "expectations": []
    }
    
    for expectation in expectations:
        exp_type = expectation.get("expectation_type")
        kwargs = expectation.get("kwargs", {})
        
        try:
            # Placeholder for actual DQ checks
            passed = True  # Would implement actual check here
            result = {
                "expectation_type": exp_type,
                "passed": passed,
                "kwargs": kwargs
            }
            results["expectations"].append(result)
            
            if passed:
                results["passed"] += 1
            else:
                results["failed"] += 1
                logger.warning(f"DQ check failed: {exp_type}")
                
        except Exception as e:
            logger.error(f"DQ check error: {exp_type}: {e}")
            results["failed"] += 1
            results["expectations"].append({
                "expectation_type": exp_type,
                "passed": False,
                "error": str(e)
            })
    
    logger.info(f"DQ suite {suite_name}: {results['passed']}/{results['total_expectations']} passed")
    return results


def validate_not_null(df: DataFrame, column: str) -> bool:
    """Validate that a column has no null values."""
    null_count = df.filter(df[column].isNull()).count()
    return null_count == 0


def validate_unique(df: DataFrame, columns: List[str]) -> bool:
    """Validate that columns are unique."""
    total_rows = df.count()
    distinct_rows = df.select(*columns).distinct().count()
    return total_rows == distinct_rows


def generate_dq_report(results: Dict[str, Any]) -> str:
    """
    Generate a human-readable DQ report.
    
    Args:
        results: DQ validation results
        
    Returns:
        Formatted report string
    """
    report = f"\n{'='*60}\n"
    report += f"Data Quality Report: {results['suite_name']}\n"
    report += f"{'='*60}\n"
    report += f"Total Expectations: {results['total_expectations']}\n"
    report += f"Passed: {results['passed']}\n"
    report += f"Failed: {results['failed']}\n"
    report += f"{'='*60}\n\n"
    
    for exp in results['expectations']:
        status = "✅" if exp.get('passed', False) else "❌"
        report += f"{status} {exp.get('expectation_type', 'unknown')}\n"
    
    return report

