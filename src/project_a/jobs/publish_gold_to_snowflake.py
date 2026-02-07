"""
Publish Gold to Snowflake Job

Publish Gold tables to Snowflake using MERGE pattern (staging + MERGE).
"""

import logging
import sys
import tempfile
import uuid
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


import time

from project_a.utils.config import load_config_resolved
from project_a.utils.logging import get_trace_id, setup_json_logging
from project_a.utils.spark_session import build_spark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(args):
    """
    Publish Gold table to Snowflake using staging + MERGE pattern.

    Args:
        args: argparse.Namespace with --env, --config, --table, --mode
    """
    # Setup structured logging
    trace_id = get_trace_id()
    setup_json_logging(level="INFO", include_trace_id=True)
    logger.info(f"Job started (trace_id={trace_id}, env={args.env})")

    # Load config
    if args.config:
        config_path = args.config
        if config_path.startswith("s3://"):
            from pyspark.sql import SparkSession

            spark_temp = (
                SparkSession.builder.appName("config_loader")
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate()
            )
            try:
                config_lines = spark_temp.sparkContext.textFile(config_path).collect()
                config_content = "\n".join(config_lines)
                tmp_file = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".yaml", delete=False, encoding="utf-8"
                )
                tmp_file.write(config_content)
                tmp_file.close()
                config_path = tmp_file.name
            finally:
                spark_temp.stop()
        else:
            config_path = str(Path(config_path))
    else:
        config_path = Path(f"config/{args.env}.yaml")
        if not config_path.exists():
            config_path = Path("config/prod.yaml")
        if not config_path.exists():
            config_path = Path("config/local.yaml")
        config_path = str(config_path)

    config = load_config_resolved(config_path)

    if not config.get("environment"):
        config["environment"] = "emr"

    run_id = str(uuid.uuid4())
    start_time = time.time()
    spark = build_spark(config)

    try:
        # Import the actual publish logic
        # The publish logic is in jobs/publish_gold_to_snowflake.py
        import sys

        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
        from jobs.publish_gold_to_snowflake import publish_gold_to_snowflake

        table_name = getattr(args, "table", "fact_orders")
        mode = getattr(args, "mode", "merge")

        publish_gold_to_snowflake(spark, config, table_name=table_name, mode=mode)

        logger.info(f"✅ Published {table_name} to Snowflake (mode={mode})")

    except Exception as e:
        logger.error(f"❌ Publish failed: {e}", exc_info=True, extra={"trace_id": trace_id})
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    # Keep this for local debugging
    import argparse

    parser = argparse.ArgumentParser(description="Publish Gold to Snowflake")
    parser.add_argument("--env", default="dev", help="Environment (dev/prod)")
    parser.add_argument("--config", help="Config file path (local or S3)")
    parser.add_argument("--table", default="fact_orders", help="Gold table name")
    parser.add_argument(
        "--mode", default="merge", choices=["merge", "overwrite"], help="Publish mode"
    )
    main(parser.parse_args())
