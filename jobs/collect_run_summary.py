"""
Collect run summary and metrics from all pipeline stages.

Aggregates metrics, lineage, and DQ results into a single run summary.
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from pyspark.sql import SparkSession

from pyspark_interview_project.utils.spark_session import build_spark
from pyspark_interview_project.utils.config import load_conf

logger = logging.getLogger(__name__)


def collect_run_summary(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str,
    execution_date: str
) -> Dict[str, Any]:
    """
    Collect run summary from all pipeline stages.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        run_id: Pipeline run ID
        execution_date: Execution date (YYYY-MM-DD)
        
    Returns:
        Run summary dictionary
    """
    summary = {
        "run_id": run_id,
        "execution_date": execution_date,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "stages": {},
        "metrics": {
            "total_rows_ingested": 0,
            "total_rows_silver": 0,
            "total_rows_gold": 0,
            "total_rows_published": 0
        },
        "dq_results": {},
        "lineage_events": [],
        "status": "success",
        "errors": []
    }
    
    bronze_path = config.get("data_lake", {}).get("bronze_path", "data/lakehouse_delta/bronze")
    silver_path = config.get("data_lake", {}).get("silver_path", "data/lakehouse_delta/silver")
    gold_path = config.get("data_lake", {}).get("gold_path", "data/lakehouse_delta/gold")
    metrics_path = config.get("data_lake", {}).get("metrics_path", "data/metrics")
    
    # Collect metrics from each layer
    try:
        # Bronze layer
        bronze_tables = ["redshift/behavior", "crm/accounts", "crm/contacts", "snowflake/orders"]
        for table in bronze_tables:
            try:
                df = spark.read.format("delta").load(f"{bronze_path}/{table}")
                count = df.count()
                summary["stages"][f"bronze_{table.replace('/', '_')}"] = {
                    "rows": count,
                    "status": "success"
                }
                summary["metrics"]["total_rows_ingested"] += count
            except Exception as e:
                logger.warning(f"Could not read bronze {table}: {e}")
                summary["stages"][f"bronze_{table.replace('/', '_')}"] = {
                    "rows": 0,
                    "status": "error",
                    "error": str(e)
                }
                summary["errors"].append(f"Bronze {table}: {e}")
        
        # Silver layer
        silver_tables = ["behavior", "crm/accounts", "crm/contacts", "snowflake/orders"]
        for table in silver_tables:
            try:
                df = spark.read.format("delta").load(f"{silver_path}/{table}")
                count = df.count()
                summary["stages"][f"silver_{table.replace('/', '_')}"] = {
                    "rows": count,
                    "status": "success"
                }
                summary["metrics"]["total_rows_silver"] += count
            except Exception as e:
                logger.warning(f"Could not read silver {table}: {e}")
                summary["stages"][f"silver_{table.replace('/', '_')}"] = {
                    "rows": 0,
                    "status": "error",
                    "error": str(e)
                }
                summary["errors"].append(f"Silver {table}: {e}")
        
        # Gold layer
        gold_tables = ["customer_360", "product_perf_daily"]
        for table in gold_tables:
            try:
                df = spark.read.format("delta").load(f"{gold_path}/{table}")
                count = df.count()
                summary["stages"][f"gold_{table}"] = {
                    "rows": count,
                    "status": "success"
                }
                summary["metrics"]["total_rows_gold"] += count
            except Exception as e:
                logger.warning(f"Could not read gold {table}: {e}")
                summary["stages"][f"gold_{table}"] = {
                    "rows": 0,
                    "status": "error",
                    "error": str(e)
                }
                summary["errors"].append(f"Gold {table}: {e}")
        
    except Exception as e:
        logger.error(f"Error collecting run summary: {e}")
        summary["status"] = "error"
        summary["errors"].append(str(e))
    
    # Load DQ results if available
    try:
        dq_results_path = Path(metrics_path) / "dq_results" / f"{execution_date}.json"
        if dq_results_path.exists():
            with open(dq_results_path, "r") as f:
                summary["dq_results"] = json.load(f)
    except Exception as e:
        logger.warning(f"Could not load DQ results: {e}")
    
    # Determine overall status
    if summary["errors"]:
        summary["status"] = "error"
    elif any(stage.get("status") == "error" for stage in summary["stages"].values()):
        summary["status"] = "partial"
    
    return summary


def save_run_summary(summary: Dict[str, Any], output_path: str) -> str:
    """Save run summary to file."""
    output_file = Path(output_path) / f"run_summary_{summary['run_id']}.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w") as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Saved run summary to: {output_file}")
    return str(output_file)


def main():
    parser = argparse.ArgumentParser(description="Collect pipeline run summary")
    parser.add_argument("--config", default="config/local.yaml", help="Config file path")
    parser.add_argument("--run-id", required=True, help="Pipeline run ID")
    parser.add_argument("--execution-date", required=True, help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--output", default="data/metrics/run_summaries", help="Output path for summary")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    config = load_conf(args.config)
    spark = build_spark(app_name="collect_run_summary", config=config)
    
    try:
        summary = collect_run_summary(spark, config, args.run_id, args.execution_date)
        output_file = save_run_summary(summary, args.output)
        
        # Print summary
        print("\n=== Pipeline Run Summary ===")
        print(f"Run ID: {summary['run_id']}")
        print(f"Execution Date: {summary['execution_date']}")
        print(f"Status: {summary['status']}")
        print(f"\nMetrics:")
        print(f"  Total Rows Ingested (Bronze): {summary['metrics']['total_rows_ingested']:,}")
        print(f"  Total Rows Silver: {summary['metrics']['total_rows_silver']:,}")
        print(f"  Total Rows Gold: {summary['metrics']['total_rows_gold']:,}")
        
        if summary["errors"]:
            print(f"\nErrors ({len(summary['errors'])}):")
            for error in summary["errors"]:
                print(f"  - {error}")
        
        print(f"\nSummary saved to: {output_file}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

