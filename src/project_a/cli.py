"""
Project A CLI - Enterprise data platform command line interface
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Project A - Enterprise PySpark + Delta + Airflow Data Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  projecta --config config/aws-prod.yaml --cmd ingest
  projecta --config config/local.yaml --cmd transform
  projecta --config config/aws-prod.yaml --cmd validate
        """,
    )

    parser.add_argument("--config", required=True, help="Configuration file path (YAML)")
    parser.add_argument(
        "--cmd",
        choices=["ingest", "transform", "validate", "pipeline"],
        required=True,
        help="Command to execute",
    )
    parser.add_argument(
        "--env",
        default="local",
        choices=["local", "dev", "staging", "prod"],
        help="Environment to run in",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate config file exists
    if not Path(args.config).exists():
        logger.error(f"Configuration file not found: {args.config}")
        return 1

    logger.info(f"Running {args.cmd} with {args.config} in {args.env} environment")

    try:
        if args.cmd == "ingest":
            return run_ingest(args.config, args.env)
        elif args.cmd == "transform":
            return run_transform(args.config, args.env)
        elif args.cmd == "validate":
            return run_validate(args.config, args.env)
        elif args.cmd == "pipeline":
            return run_pipeline(args.config, args.env)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        return 1

    return 0


def run_ingest(config_path: str, env: str) -> int:
    """Run data ingestion."""
    logger.info("Starting data ingestion...")
    ingest_jobs = [
        "fx_json_to_bronze",
        "snowflake_to_bronze",
        "crm_to_bronze",
        "redshift_to_bronze",
        "kafka_csv_to_bronze",
    ]
    for job_name in ingest_jobs:
        code = _run_job(job_name, config_path, env)
        if code != 0:
            return code
    logger.info("Data ingestion completed successfully")
    return 0


def run_transform(config_path: str, env: str) -> int:
    """Run data transformation."""
    logger.info("Starting data transformation...")
    transform_jobs = ["bronze_to_silver", "silver_to_gold"]
    for job_name in transform_jobs:
        code = _run_job(job_name, config_path, env)
        if code != 0:
            return code
    logger.info("Data transformation completed successfully")
    return 0


def run_validate(config_path: str, env: str) -> int:
    """Run data validation."""
    logger.info("Starting data validation...")
    # Bronze sanity + comprehensive DQ
    sanity_cmd = [
        sys.executable,
        "scripts/check_bronze_data.py",
        "--config",
        config_path,
    ]
    dq_cmd = [
        sys.executable,
        "jobs/dq/run_comprehensive_dq.py",
        "--layer",
        "all",
        "--env",
        env,
        "--config",
        config_path,
    ]
    if subprocess.call(sanity_cmd) != 0:
        return 1
    if subprocess.call(dq_cmd) != 0:
        return 1
    logger.info("Data validation completed successfully")
    return 0


def run_pipeline(config_path: str, env: str) -> int:
    """Run complete ETL pipeline."""
    logger.info("Starting complete ETL pipeline...")

    # Run ingestion
    if run_ingest(config_path, env) != 0:
        return 1

    # Run transformation
    if run_transform(config_path, env) != 0:
        return 1

    # Run validation
    if run_validate(config_path, env) != 0:
        return 1

    logger.info("Complete ETL pipeline completed successfully")
    return 0


def _run_job(job_name: str, config_path: str, env: str) -> int:
    """Run a single job through the canonical pipeline runner."""
    from argparse import Namespace

    from project_a.pipeline.run_pipeline import JOB_MAP

    job_fn = JOB_MAP.get(job_name)
    if not job_fn:
        logger.error("Unknown job: %s", job_name)
        return 1

    args = Namespace(job=job_name, env=env, config=config_path, run_date=None)
    try:
        result = job_fn(args)
    except SystemExit as exc:
        return int(exc.code or 1)
    if isinstance(result, int):
        return result
    return 0


def etl_main():
    """ETL pipeline entry point."""
    return main()


def dq_main():
    """Data quality entry point."""
    return main()


if __name__ == "__main__":
    sys.exit(main())
