"""
Project A – EMR Job Runner

Unified entrypoint that routes jobs by name.
This provides a mature platform interface rather than loose scripts.

Usage:
    python -m project_a.pipeline.run_pipeline \
        --job fx_json_to_bronze \
        --env dev \
        --config s3://bucket/config/dev.yaml \
        --run-date 2025-01-15

EMR/Airflow Usage:
    "entryPoint": "s3://.../packages/project_a-0.1.0-py3-none-any.whl",
    "entryPointArguments": [
      "--job", "fx_json_to_bronze",
      "--env", "dev",
      "--config", "s3://.../config/dev.yaml"
    ]
"""
import argparse
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)

# Import job main functions
try:
    from project_a.jobs import (
        fx_json_to_bronze,
        bronze_to_silver,
        silver_to_gold,
        publish_gold_to_snowflake,
    )
except ImportError as e:
    logger.warning(f"Some job imports failed: {e}. Continuing with available jobs.")
    # Create dummy modules to prevent errors
    class DummyModule:
        def main(self, args):
            logger.error(f"Job module not available: {e}")
            sys.exit(1)
    
    fx_json_to_bronze = DummyModule()
    bronze_to_silver = DummyModule()
    silver_to_gold = DummyModule()
    publish_gold_to_snowflake = DummyModule()


JOB_MAP = {
    "fx_json_to_bronze": fx_json_to_bronze.main,
    "bronze_to_silver": bronze_to_silver.main,
    "silver_to_gold": silver_to_gold.main,
    "publish_gold_to_snowflake": publish_gold_to_snowflake.main,
}


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Project A – EMR job runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run FX JSON to Bronze
  python -m project_a.pipeline.run_pipeline \\
    --job fx_json_to_bronze \\
    --env dev \\
    --config s3://bucket/config/dev.yaml

  # Run Bronze to Silver transformation
  python -m project_a.pipeline.run_pipeline \\
    --job bronze_to_silver \\
    --env dev \\
    --config s3://bucket/config/dev.yaml \\
    --run-date 2025-01-15
        """
    )
    
    parser.add_argument(
        "--job",
        required=True,
        choices=sorted(JOB_MAP.keys()),
        help="Which logical job to run",
    )
    parser.add_argument(
        "--env",
        required=True,
        help="Environment name, e.g. dev/stage/prod",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Config file path (s3://…/dev.yaml, etc.)",
    )
    parser.add_argument(
        "--run-date",
        required=False,
        help="Optional run date override (YYYY-MM-DD)",
    )
    
    return parser.parse_args()


def main():
    """Main entry point - dispatches to appropriate job."""
    args = parse_args()
    job_name = args.job
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("Starting job %s with config=%s env=%s", job_name, args.config, args.env)
    
    job_fn = JOB_MAP.get(job_name)
    if job_fn is None:
        logger.error(f"Unknown job: {job_name}. Available: {list(JOB_MAP.keys())}")
        sys.exit(1)
    
    try:
        # Every job `main` should accept args and read args.env / args.config
        job_fn(args)
        logger.info("Finished job %s", job_name)
    except Exception as e:
        logger.error(f"Job {job_name} failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

