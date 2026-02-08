"""Kafka Orders Streaming Job.

Runs Spark Structured Streaming to ingest Kafka order events into Bronze.
"""

import argparse
import logging

from project_a.config_loader import load_config_resolved
from project_a.utils.spark_session import build_spark
from project_a.pyspark_interview_project.extract.kafka_orders_stream import stream_orders_from_kafka

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka orders stream job")
    parser.add_argument("--config", default="local/config/local.yaml")
    parser.add_argument("--env", default="local")
    args = parser.parse_args()

    config = load_config_resolved(args.config)
    spark = build_spark(app_name="kafka_orders_stream", config=config)

    try:
        stream_orders_from_kafka(spark, config)
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
