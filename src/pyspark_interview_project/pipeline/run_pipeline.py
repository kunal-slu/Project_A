#!/usr/bin/env python3
"""
Unified Pipeline Entrypoint

Single canonical entrypoint that routes jobs by name.
This provides a mature platform interface rather than loose scripts.

Usage:
    python -m pyspark_interview_project.pipeline.run_pipeline \
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
import sys
import argparse
import logging
from pathlib import Path
from typing import Dict, Callable, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)

# Import job main functions
try:
    # Bronze ingestion jobs
    from jobs.ingest.fx_json_to_bronze import main as fx_json_to_bronze_main
    
    # Transform jobs
    from jobs.transform.bronze_to_silver import main as bronze_to_silver_main
    from jobs.gold.silver_to_gold import main as silver_to_gold_main
    
    # Publish jobs
    from jobs.publish_gold_to_snowflake import main as publish_gold_to_snowflake_main
    
except ImportError as e:
    logger.warning(f"Some job imports failed: {e}. Continuing with available jobs.")
    # Create dummy functions to prevent errors
    def fx_json_to_bronze_main():
        logger.error("fx_json_to_bronze job not available")
        return 1
    def bronze_to_silver_main():
        logger.error("bronze_to_silver job not available")
        return 1
    def silver_to_gold_main():
        logger.error("silver_to_gold job not available")
        return 1
    def publish_gold_to_snowflake_main():
        logger.error("publish_gold_to_snowflake job not available")
        return 1

# Job dispatcher map
JOB_MAP: Dict[str, Callable] = {
    # Bronze ingestion
    "fx_json_to_bronze": fx_json_to_bronze_main,
    
    # Transformations
    "bronze_to_silver": bronze_to_silver_main,
    "silver_to_gold": silver_to_gold_main,
    
    # Publishing
    "publish_gold_to_snowflake": publish_gold_to_snowflake_main,
}


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Unified pipeline entrypoint - routes jobs by name",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run FX JSON to Bronze
  python -m pyspark_interview_project.pipeline.run_pipeline \\
    --job fx_json_to_bronze \\
    --env dev \\
    --config s3://bucket/config/dev.yaml

  # Run Bronze to Silver transformation
  python -m pyspark_interview_project.pipeline.run_pipeline \\
    --job bronze_to_silver \\
    --env dev \\
    --config s3://bucket/config/dev.yaml \\
    --run-date 2025-01-15
        """
    )
    
    parser.add_argument(
        "--job",
        required=True,
        choices=list(JOB_MAP.keys()),
        help=f"Job name to execute. Available: {', '.join(JOB_MAP.keys())}"
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "staging", "prod"],
        help="Environment (dev/staging/prod)"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Config file path (local or S3 URI)"
    )
    parser.add_argument(
        "--run-date",
        required=False,
        help="Processing date (YYYY-MM-DD). Defaults to today."
    )
    
    return parser.parse_args()


def main():
    """Main entry point - dispatches to appropriate job."""
    args = parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info(f"🚀 Starting job: {args.job} (env={args.env}, config={args.config})")
    
    if args.job not in JOB_MAP:
        logger.error(f"❌ Unknown job: {args.job}. Available: {list(JOB_MAP.keys())}")
        sys.exit(1)
    
    job_func = JOB_MAP[args.job]
    
    try:
        # Call job with appropriate arguments
        # Most jobs expect: --env, --config, optionally --run-date
        import sys as job_sys
        
        # Build argument list for job
        job_args = ["--env", args.env]
        if args.config:
            job_args.extend(["--config", args.config])
        if args.run_date:
            job_args.extend(["--run-date", args.run_date])
        
        # Temporarily replace sys.argv for job execution
        original_argv = job_sys.argv
        job_sys.argv = [args.job] + job_args
        
        try:
            exit_code = job_func()
            if exit_code is None:
                exit_code = 0
            logger.info(f"✅ Job {args.job} completed with exit code: {exit_code}")
            sys.exit(exit_code if isinstance(exit_code, int) else 0)
        finally:
            job_sys.argv = original_argv
            
    except Exception as e:
        logger.error(f"❌ Job {args.job} failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
