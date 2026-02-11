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

Backward-compatible aliases:
    - fx_to_bronze -> fx_json_to_bronze
    - kafka_events_to_bronze -> kafka_events_csv_snapshot_to_bronze
"""

import argparse
import logging
import sys
from collections.abc import Callable
from importlib import import_module

logger = logging.getLogger(__name__)

_CANONICAL_JOB_TARGETS: dict[str, str] = {
    # Bronze ingestion jobs
    "fx_json_to_bronze": "project_a.jobs.fx_json_to_bronze:main",
    "snowflake_to_bronze": "project_a.jobs.snowflake_to_bronze:main",
    "crm_to_bronze": "project_a.jobs.crm_to_bronze:main",
    "redshift_to_bronze": "project_a.jobs.redshift_to_bronze:main",
    "kafka_csv_to_bronze": "project_a.jobs.kafka_csv_to_bronze:main",
    "kafka_stream_to_bronze": "project_a.jobs.kafka_stream_to_bronze:main",
    # Transformation jobs
    "bronze_to_silver": "project_a.jobs.bronze_to_silver:main",
    "silver_to_gold": "project_a.jobs.silver_to_gold:main",
    # DQ gate jobs
    "dq_silver_gate": "project_a.jobs.dq_silver_gate:main",
    "dq_gold_gate": "project_a.jobs.dq_gold_gate:main",
    "gold_truth_tests": "project_a.jobs.gold_truth_tests:main",
    # Publish jobs
    "publish_gold_to_redshift": "project_a.jobs.publish_gold_to_redshift:main",
    "publish_gold_to_snowflake": "project_a.jobs.publish_gold_to_snowflake:main",
    # Iceberg maintenance
    "orders_silver_to_iceberg": "project_a.jobs.orders_silver_to_iceberg:main",
}

_ALIASES: dict[str, str] = {
    "kafka_events_csv_snapshot_to_bronze": "kafka_csv_to_bronze",
    "kafka_events_to_bronze": "kafka_csv_to_bronze",
    "fx_to_bronze": "fx_json_to_bronze",
}


def _load_job_callable(target: str) -> Callable:
    module_name, attr_name = target.split(":", 1)
    module = import_module(module_name)
    job_fn = getattr(module, attr_name, None)
    if not callable(job_fn):
        raise AttributeError(f"{target} is not a callable job entrypoint")
    return job_fn


def _build_job_dispatcher(target: str) -> Callable:
    def _dispatch(args):
        return _load_job_callable(target)(args)

    return _dispatch


JOB_MAP: dict[str, Callable] = {
    name: _build_job_dispatcher(target) for name, target in _CANONICAL_JOB_TARGETS.items()
}
for alias, canonical in _ALIASES.items():
    JOB_MAP[alias] = JOB_MAP[canonical]


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
        """,
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
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
