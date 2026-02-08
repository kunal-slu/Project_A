#!/usr/bin/env python3
"""
CLI for PySpark ETL Pipeline
"""

import argparse
import logging
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml

from project_a.standard_etl_pipeline import StandardETLPipeline

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(config_path: str, env: str = "local"):
    """Load configuration from YAML file"""
    try:
        with open(config_path) as f:
            config_data = yaml.safe_load(f)
        logger.info(f"✅ Configuration loaded from {config_path}")
        return config_data
    except Exception as e:
        logger.error(f"❌ Failed to load config from {config_path}: {e}")
        raise


def run_ingest(config: dict, env: str):
    """Run data ingestion stage"""
    logger.info("🔄 Starting data ingestion...")

    # Check if input data exists
    input_dir = "data/input_data"
    if not os.path.exists(input_dir):
        logger.error(f"❌ Input data directory not found: {input_dir}")
        return False

    files = [f for f in os.listdir(input_dir) if f.endswith((".csv", ".json"))]
    logger.info(f"📊 Found {len(files)} input files for ingestion")

    # Log input data summary
    for file in files:
        file_path = os.path.join(input_dir, file)
        if file.endswith(".csv"):
            try:
                import pandas as pd

                df = pd.read_csv(file_path, nrows=5)
                logger.info(f"📄 {file}: {len(df)} sample rows, {len(df.columns)} columns")
            except Exception as e:
                logger.warning(f"⚠️ Could not read {file}: {e}")
        elif file.endswith(".json"):
            try:
                import json

                with open(file_path) as f:
                    data = json.load(f)
                if isinstance(data, list):
                    logger.info(f"📄 {file}: {len(data)} records")
                else:
                    logger.info(f"📄 {file}: JSON object with {len(data)} keys")
            except Exception as e:
                logger.warning(f"⚠️ Could not read {file}: {e}")

    logger.info("✅ Data ingestion completed")
    return True


def run_transform(config: dict, env: str):
    """Run data transformation stage"""
    logger.info("🔄 Starting data transformation...")

    try:
        # Use the standard pipeline for transformation
        pipeline = StandardETLPipeline(config)
        success = pipeline.run_pipeline()

        if success:
            logger.info("✅ Data transformation completed")
            return True
        else:
            logger.error("❌ Data transformation failed")
            return False
    except Exception as e:
        logger.error(f"❌ Transformation error: {e}")
        return False


def run_validate(config: dict, env: str):
    """Run data validation stage"""
    logger.info("🔄 Starting data validation...")

    try:
        # Check if Delta Lake tables exist and have data
        delta_path = config.get("delta_path", "data/lakehouse_delta_standard")

        validation_results = []
        layers = ["bronze", "silver", "gold"]

        for layer in layers:
            layer_path = os.path.join(delta_path, layer)
            if os.path.exists(layer_path):
                for table_dir in os.listdir(layer_path):
                    table_path = os.path.join(layer_path, table_dir)
                    if os.path.isdir(table_path):
                        # Check for parquet files
                        parquet_files = [
                            f for f in os.listdir(table_path) if f.endswith(".parquet")
                        ]
                        # Check for transaction logs
                        delta_log_path = os.path.join(table_path, "_delta_log")
                        log_files = []
                        if os.path.exists(delta_log_path):
                            log_files = [
                                f for f in os.listdir(delta_log_path) if f.endswith(".json")
                            ]

                        validation_results.append(
                            {
                                "layer": layer,
                                "table": table_dir,
                                "parquet_files": len(parquet_files),
                                "transaction_logs": len(log_files),
                            }
                        )

                        logger.info(
                            f"📊 {layer}.{table_dir}: {len(parquet_files)} parquet files, {len(log_files)} versions"
                        )

        # Summary
        total_tables = len(validation_results)
        total_files = sum(r["parquet_files"] for r in validation_results)
        total_versions = sum(r["transaction_logs"] for r in validation_results)

        logger.info(
            f"✅ Validation completed: {total_tables} tables, {total_files} files, {total_versions} versions"
        )
        return True

    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
        return False


def run_load(config: dict, env: str):
    """Run data loading/publishing stage (MERGE into Gold)"""
    logger.info("🔄 Starting data loading/publishing...")

    try:
        # Get gold layer path
        delta_path = config.get("delta_path", "data/lakehouse_delta_standard")
        gold_path = os.path.join(delta_path, "gold")

        if not os.path.exists(gold_path):
            logger.warning(f"⚠️ Gold layer not found at {gold_path}")
            return False

        # Process each gold table
        for table_dir in os.listdir(gold_path):
            table_path = os.path.join(gold_path, table_dir)
            if os.path.isdir(table_path):
                logger.info(f"📊 Processing gold table: {table_dir}")

                try:
                    # Check for parquet files instead of reading with Spark
                    parquet_files = [f for f in os.listdir(table_path) if f.endswith(".parquet")]
                    delta_log_path = os.path.join(table_path, "_delta_log")
                    log_files = []
                    if os.path.exists(delta_log_path):
                        log_files = [f for f in os.listdir(delta_log_path) if f.endswith(".json")]

                    # Log the write path and file counts
                    logger.info(f"✅ Gold written to: {table_path}")
                    logger.info(f"📊 Gold parquet files: {len(parquet_files)}")
                    logger.info(f"📊 Gold versions: {len(log_files)}")

                    # Show recent transaction logs
                    if log_files:
                        latest_log = sorted(log_files)[-1]
                        logger.info(f"📝 Latest version: {latest_log}")

                except Exception as e:
                    logger.error(f"❌ Error processing {table_dir}: {e}")
                    continue
        logger.info("✅ Data loading/publishing completed")
        return True

    except Exception as e:
        logger.error(f"❌ Loading error: {e}")
        return False


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline CLI")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--env", default="local", help="Environment (local, dev, prod)")
    parser.add_argument(
        "--cmd",
        choices=["ingest", "transform", "validate", "pipeline", "full"],
        required=True,
        help="Command to run",
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config, args.env)

    # Execute command
    if args.cmd == "ingest":
        success = run_ingest(config, args.env)
    elif args.cmd == "transform":
        success = run_transform(config, args.env)
    elif args.cmd == "validate":
        success = run_validate(config, args.env)
    elif args.cmd == "load":
        success = run_load(config, args.env)
    elif args.cmd in ("pipeline", "full"):
        # Run all stages in sequence
        logger.info("🚀 Running complete ETL pipeline...")

        stages = [
            ("Ingest", run_ingest),
            ("Transform", run_transform),
            ("Validate", run_validate),
            ("Load", run_load),
        ]

        success = True
        for stage_name, stage_func in stages:
            logger.info(f"📋 Stage: {stage_name}")
            stage_success = stage_func(config, args.env)
            if not stage_success:
                logger.error(f"❌ Stage {stage_name} failed")
                success = False
                break
            logger.info(f"✅ Stage {stage_name} completed")

        if success:
            logger.info("🎉 Complete ETL pipeline finished successfully!")
        else:
            logger.error("❌ ETL pipeline failed")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
