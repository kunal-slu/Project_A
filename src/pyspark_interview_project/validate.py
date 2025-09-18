"""
validate.py
Post-write DataFrame validations.
"""

import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class ValidateOutput:
    def validate_parquet(self, spark: SparkSession, path: str) -> None:
        logger.info(f"Validating Parquet at {path}")
        files = [f for f in os.listdir(path) if f.endswith(".parquet")]
        if not files:
            raise FileNotFoundError("No parquet files found")
        df = spark.read.parquet(path)
        count = df.count()
        if count == 0:
            raise ValueError("Parquet output has zero rows")
        logger.info(f"Parquet validation passed: {count} rows")

    def validate_avro(self, spark: SparkSession, path: str) -> None:
        logger.info(f"Validating Avro at {path}")
        # Avro sink writes directories containing part files
        if not os.path.isdir(path):
            raise FileNotFoundError("Avro directory not found")
        df = spark.read.format("avro").load(path)
        if df.count() == 0:
            raise ValueError("Avro output has zero rows")
        logger.info("Avro validation passed")

    def validate_json(self, spark: SparkSession, path: str) -> None:
        logger.info(f"Validating JSON at {path}")
        if not os.path.isdir(path):
            raise FileNotFoundError("JSON directory not found")
        df = spark.read.json(path)
        if df.count() == 0:
            raise ValueError("JSON output has zero rows")
        logger.info("JSON validation passed")
