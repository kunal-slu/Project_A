"""
Data quality runner that executes YAML policies and returns JSON summary.
"""

import json
import logging
import yaml
from typing import Dict, Any, List
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession

from .rules import create_rule_from_config

logger = logging.getLogger(__name__)


def run_yaml_policy(spark: SparkSession, policy_path: str, df: DataFrame) -> Dict[str, Any]:
    """
    Run data quality policy from YAML file.
    
    Args:
        spark: Spark session
        policy_path: Path to YAML policy file
        df: DataFrame to check
        
    Returns:
        JSON summary of data quality results
    """
    logger.info(f"Running data quality policy: {policy_path}")
    
    # Load policy
    with open(policy_path, 'r') as f:
        policy = yaml.safe_load(f)
    
    table_name = policy["table"]
    rules_config = policy["rules"]
    
    logger.info(f"Running DQ checks for table: {table_name}")
    
    # Execute rules
    results = []
    error_count = 0
    warning_count = 0
    
    for rule_config in rules_config:
        try:
            rule = create_rule_from_config(rule_config)
            result = rule.check(df)
            results.append(result)
            
            if result["severity"] == "error" and not result["passed"]:
                error_count += 1
            elif result["severity"] == "warn" and not result["passed"]:
                warning_count += 1
                
            logger.info(f"Rule '{rule.name}': {'PASSED' if result['passed'] else 'FAILED'}")
            
        except Exception as e:
            logger.error(f"Rule '{rule_config['name']}' failed with error: {e}")
            results.append({
                "rule_name": rule_config["name"],
                "rule_type": rule_config["type"],
                "severity": rule_config["severity"],
                "error": str(e),
                "passed": False
            })
            error_count += 1
    
    # Create summary
    summary = {
        "table": table_name,
        "total_rules": len(rules_config),
        "passed_rules": len([r for r in results if r.get("passed", False)]),
        "failed_rules": len([r for r in results if not r.get("passed", False)]),
        "error_count": error_count,
        "warning_count": warning_count,
        "overall_status": "PASSED" if error_count == 0 else "FAILED",
        "results": results
    }
    
    logger.info(f"DQ Policy Summary: {summary['overall_status']} "
               f"({summary['passed_rules']}/{summary['total_rules']} rules passed)")
    
    return summary


def print_dq_summary(summary: Dict[str, Any]) -> None:
    """Print formatted data quality summary."""
    print("\n" + "="*60)
    print("DATA QUALITY SUMMARY")
    print("="*60)
    print(f"Table: {summary['table']}")
    print(f"Overall Status: {summary['overall_status']}")
    print(f"Rules: {summary['passed_rules']}/{summary['total_rules']} passed")
    print(f"Errors: {summary['error_count']}, Warnings: {summary['warning_count']}")
    print("-"*60)
    
    for result in summary["results"]:
        status = "✓ PASS" if result.get("passed", False) else "✗ FAIL"
        print(f"{status} {result['rule_name']} ({result['rule_type']})")
        
        if not result.get("passed", False):
            if "failure_rate" in result:
                print(f"    Failure Rate: {result['failure_rate']:.2%}")
            if "error" in result:
                print(f"    Error: {result['error']}")
    
    print("="*60)
    
    # Exit with non-zero code if there are errors
    if summary["error_count"] > 0:
        print(f"\n❌ Data quality check FAILED with {summary['error_count']} errors")
        exit(1)
    else:
        print(f"\n✅ Data quality check PASSED")
