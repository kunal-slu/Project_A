"""
Base transformer class for code reuse across all transformations.

Provides common functionality:
- Schema validation
- Data cleaning patterns
- DQ validation
- Metadata addition
- Error handling
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, trim

# DQ utilities - optional import
try:
    from project_a.utils.dq_utils import validate_not_null, validate_unique
except ImportError:
    # Fallback stubs if dq_utils not available
    def validate_not_null(*args, **kwargs):
        pass

    def validate_unique(*args, **kwargs):
        pass


logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """Base class for all transformers."""

    def __init__(self, source_name: str, target_layer: str, config: dict[str, Any]):
        """
        Initialize transformer.

        Args:
            source_name: Source identifier (e.g., 'behavior', 'orders')
            target_layer: Target layer (e.g., 'silver', 'gold')
            config: Configuration dictionary
        """
        self.source_name = source_name
        self.target_layer = target_layer
        self.config = config

    def _trim_string_columns(self, df: DataFrame) -> DataFrame:
        """Trim whitespace from all string columns."""
        string_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == "string"]
        for col_name in string_cols:
            df = df.withColumn(col_name, trim(col(col_name)))
        return df

    def _handle_nulls(self, df: DataFrame, fill_value: str = "") -> DataFrame:
        """Fill nulls in string columns."""
        string_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == "string"]
        return df.na.fill(fill_value, subset=string_cols)

    def _add_metadata(self, df: DataFrame, run_id: str | None = None) -> DataFrame:
        """Add processing metadata columns."""
        from pyspark.sql.functions import lit

        df = df.withColumn("_processing_ts", current_timestamp())
        if run_id:
            df = df.withColumn(
                "_run_id", col("_run_id") if "_run_id" in df.columns else lit(run_id)
            )
        return df

    def _validate_dq(self, df: DataFrame, key_columns: list[str]) -> DataFrame:
        """Apply DQ validations."""
        for key_col in key_columns:
            if key_col in df.columns:
                df = validate_not_null(df, key_col)
                df = validate_unique(df, key_col)
        return df

    @abstractmethod
    def transform(self, spark: SparkSession, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform data. Must be implemented by subclasses.

        Args:
            spark: SparkSession
            df: Input DataFrame
            **kwargs: Additional arguments

        Returns:
            Transformed DataFrame
        """
        pass

    def transform_with_cleanup(self, spark: SparkSession, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform with standard cleanup steps.

        Args:
            spark: SparkSession
            df: Input DataFrame
            **kwargs: Additional arguments

        Returns:
            Transformed DataFrame
        """
        # Standard cleanup
        df = self._trim_string_columns(df)
        df = self._handle_nulls(df)

        # Custom transformation
        df = self.transform(spark, df, **kwargs)

        # Add metadata
        run_id = kwargs.get("run_id")
        df = self._add_metadata(df, run_id)

        return df


class BronzeToSilverTransformer(BaseTransformer):
    """Base transformer for Bronze → Silver transformations."""

    def __init__(self, source_name: str, config: dict[str, Any]):
        super().__init__(source_name, "silver", config)

    def transform(self, spark: SparkSession, df: DataFrame, **kwargs) -> DataFrame:
        """Transform Bronze to Silver - implement in subclasses."""
        raise NotImplementedError("Subclasses must implement transform()")


class SilverToGoldTransformer(BaseTransformer):
    """Base transformer for Silver → Gold transformations."""

    def __init__(self, source_name: str, config: dict[str, Any]):
        super().__init__(source_name, "gold", config)

    def transform(self, spark: SparkSession, df: DataFrame, **kwargs) -> DataFrame:
        """Transform Silver to Gold - implement in subclasses."""
        raise NotImplementedError("Subclasses must implement transform()")
