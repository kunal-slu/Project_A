"""
Error lane handler for quarantining bad rows.

Implements the P0 pattern for safe data ingestion by isolating
schema-violating or malformed records without failing entire jobs.
"""

import logging
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class ErrorLaneHandler:
    """
    Handler for quarantining bad rows to error lanes.

    Error lanes follow this pattern:
    s3://bucket/_errors/<layer>/<table>/dt=YYYY-MM-DD/run_id=<uuid>/*.json
    """

    def __init__(self, lake_root: str):
        """
        Initialize error lane handler.

        Args:
            lake_root: Root data lake path (e.g., "s3://bucket" or "data/lakehouse_delta")
        """
        self.lake_root = lake_root.rstrip('/')
        self.errors_prefix = f"{self.lake_root}/_errors"

    def get_error_lane_path(
        self,
        layer: str,
        table: str,
        run_id: str,
        partition_date: str = None
    ) -> str:
        """
        Generate error lane path for quarantined rows.

        Args:
            layer: Data layer (bronze, silver, gold)
            table: Table name
            run_id: Unique run identifier
            partition_date: Date partition (YYYY-MM-DD), defaults to today

        Returns:
            Error lane path
        """
        from datetime import datetime

        if partition_date is None:
            partition_date = datetime.now().strftime("%Y-%m-%d")

        path = f"{self.errors_prefix}/{layer}/{table}/dt={partition_date}/run_id={run_id}"

        logger.debug(f"Error lane path: {path}")
        return path

    def quarantine_bad_rows(
        self,
        df: DataFrame,
        validation_errors: DataFrame,
        layer: str,
        table: str,
        run_id: str
    ) -> tuple[DataFrame, dict[str, Any]]:
        """
        Quarantine bad rows to error lane.

        Args:
            df: Input DataFrame
            validation_errors: DataFrame with validation error details
            layer: Data layer (bronze, silver, gold)
            table: Table name
            run_id: Unique run identifier

        Returns:
            Tuple of (clean DataFrame, quarantine stats)
        """
        if df.isEmpty():
            logger.info("No data to quarantine")
            return df, {"total_rows": 0, "quarantined": 0, "clean_rows": 0}

        # Get error lane path
        error_path = self.get_error_lane_path(layer, table, run_id)

        # Split into clean and bad rows
        # Bad rows have validation errors
        if validation_errors.count() > 0:
            # Extract error details
            bad_row_ids = validation_errors.select("row_id").distinct()

            # Join to get bad rows
            quarantined_df = df.join(bad_row_ids, on="row_id", how="inner")
            clean_df = df.join(bad_row_ids, on="row_id", how="left_anti")

            # Add error metadata
            quarantined_df = quarantined_df \
                .withColumn("_error_timestamp", F.current_timestamp()) \
                .withColumn("_error_lane_path", F.lit(error_path))

            # Write to error lane
            logger.warning(f"⚠️  Quarantining {quarantined_df.count()} bad rows to {error_path}")

            quarantined_df \
                .write \
                .format("json") \
                .mode("append") \
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'") \
                .save(error_path)

            logger.info("✅ Bad rows written to error lane")
        else:
            clean_df = df
            quarantined_df = df.filter(F.lit(False))  # Empty DataFrame

        # Stats
        stats = {
            "total_rows": df.count(),
            "quarantined": quarantined_df.count() if not quarantined_df.isEmpty() else 0,
            "clean_rows": clean_df.count(),
            "error_lane_path": error_path
        }

        logger.info(f"✅ Quarantine complete: {stats['clean_rows']}/{stats['total_rows']} rows passed")

        return clean_df, stats

    def quarantine_schema_violations(
        self,
        df: DataFrame,
        layer: str,
        table: str,
        run_id: str,
        required_cols: list,
        date_col: str = None
    ) -> tuple[DataFrame, dict[str, Any]]:
        """
        Quarantine rows with null values in required columns.

        Args:
            df: Input DataFrame
            layer: Data layer
            table: Table name
            run_id: Unique run identifier
            required_cols: List of required column names
            date_col: Optional date column for partitioning

        Returns:
            Tuple of (clean DataFrame, quarantine stats)
        """
        # Build filter for bad rows (null in any required column)
        bad_rows_mask = None
        for col_name in required_cols:
            if col_name in df.columns:
                col_mask = F.col(col_name).isNull()
                if bad_rows_mask is None:
                    bad_rows_mask = col_mask
                else:
                    bad_rows_mask = bad_rows_mask | col_mask

        # Split clean vs bad
        if bad_rows_mask is not None:
            clean_df = df.filter(~bad_rows_mask)
            quarantined_df = df.filter(bad_rows_mask)
        else:
            clean_df = df
            quarantined_df = df.filter(F.lit(False))

        # Write quarantined rows
        quarantine_count = quarantined_df.count()
        if quarantine_count > 0:
            error_path = self.get_error_lane_path(layer, table, run_id)

            logger.warning(f"⚠️  Quarantining {quarantine_count} schema violations")

            quarantined_df \
                .withColumn("_error_timestamp", F.current_timestamp()) \
                .withColumn("_error_type", F.lit("schema_violation")) \
                .write \
                .format("json") \
                .mode("append") \
                .save(error_path)

        stats = {
            "total_rows": df.count(),
            "quarantined": quarantine_count,
            "clean_rows": clean_df.count(),
            "error_type": "schema_violation"
        }

        return clean_df, stats


def add_row_id(df: DataFrame) -> DataFrame:
    """
    Add a unique row ID to DataFrame for error tracking.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with row_id column
    """

    from pyspark.sql import functions as F

    return df.withColumn(
        "row_id",
        F.monotonically_increasing_id()
    )


def create_error_lane_metadata(
    layer: str,
    table: str,
    run_id: str,
    error_type: str,
    error_details: dict[str, Any]
) -> dict[str, Any]:
    """
    Create error lane metadata document.

    Args:
        layer: Data layer
        table: Table name
        run_id: Run identifier
        error_type: Type of error
        error_details: Error details

    Returns:
        Error metadata dictionary
    """
    return {
        "layer": layer,
        "table": table,
        "run_id": run_id,
        "error_type": error_type,
        "error_timestamp": datetime.utcnow().isoformat() + "Z",
        "error_details": error_details
    }

