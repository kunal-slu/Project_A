"""
Base extractor class for code reuse across all extractors.

Provides common functionality:
- Metadata column addition
- CSV/Delta/API reading patterns
- Watermark management
- Error handling
- Metrics emission
"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from project_a.monitoring.metrics_collector import emit_duration, emit_rowcount
from project_a.utils.state_store import get_state_store

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Base class for all extractors."""

    def __init__(self, source_name: str, table_name: str, config: dict[str, Any]):
        """
        Initialize extractor.

        Args:
            source_name: Source identifier (e.g., 'salesforce', 'snowflake')
            table_name: Table name (e.g., 'accounts', 'orders')
            config: Configuration dictionary
        """
        self.source_name = source_name
        self.table_name = table_name
        self.config = config
        self.state_store = get_state_store(config)
        self.environment = config.get("environment", "local")

    def _read_local_csv(self, spark: SparkSession, path: str) -> DataFrame:
        """Read CSV file for local development."""
        logger.info(f"Reading local CSV: {path}")
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add standard metadata columns to DataFrame."""
        from pyspark.sql.functions import lit

        return (
            df.withColumn("record_source", lit(self.source_name))
            .withColumn("record_table", lit(self.table_name))
            .withColumn("ingest_timestamp", current_timestamp())
        )

    def _get_watermark(self) -> datetime | None:
        """Get watermark for incremental loading."""
        watermark_str = self.state_store.get_watermark(f"{self.source_name}_{self.table_name}")
        if watermark_str:
            return datetime.fromisoformat(watermark_str.replace("Z", "+00:00"))
        return None

    def _set_watermark(self, value: datetime):
        """Set watermark after successful extraction."""
        self.state_store.set_watermark(f"{self.source_name}_{self.table_name}", value.isoformat())

    @abstractmethod
    def extract(self, spark: SparkSession, **kwargs) -> DataFrame:
        """
        Extract data from source. Must be implemented by subclasses.

        Args:
            spark: SparkSession
            **kwargs: Additional arguments

        Returns:
            DataFrame with extracted data
        """
        pass

    def extract_with_metrics(self, spark: SparkSession, **kwargs) -> DataFrame:
        """
        Extract data and emit metrics.

        Args:
            spark: SparkSession
            **kwargs: Additional arguments

        Returns:
            DataFrame with extracted data
        """
        start_time = time.time()

        try:
            df = self.extract(spark, **kwargs)

            # Add metadata
            df = self._add_metadata(df)

            # Emit metrics
            record_count = df.count()
            duration_ms = (time.time() - start_time) * 1000

            emit_rowcount(
                "records_extracted",
                record_count,
                {"source": self.source_name, "table": self.table_name},
                self.config,
            )

            emit_duration(
                "extraction_duration", duration_ms, {"source": self.source_name}, self.config
            )

            logger.info(
                f"✅ Extracted {record_count:,} records from {self.source_name}.{self.table_name}"
            )

            return df

        except Exception as e:
            logger.error(f"❌ Failed to extract {self.source_name}.{self.table_name}: {e}")
            raise


class SalesforceExtractor(BaseExtractor):
    """Extractor for Salesforce objects (consolidates all Salesforce extractors)."""

    def __init__(self, table_name: str, config: dict[str, Any]):
        super().__init__("salesforce", table_name, config)
        self.local_csv_path = f"aws/data/salesforce/salesforce_{table_name}_ready.csv"

    def extract(self, spark: SparkSession, **kwargs) -> DataFrame:
        """Extract Salesforce data."""
        if self.environment == "local":
            return self._read_local_csv(spark, self.local_csv_path)
        else:
            # Production: Use Salesforce API
            # For now, return empty DataFrame with expected schema
            return self._create_empty_dataframe(spark)

    def _create_empty_dataframe(self, spark: SparkSession) -> DataFrame:
        """Create empty DataFrame with Salesforce schema."""
        from pyspark.sql.types import StructType

        # This would load schema from config/schema_definitions/
        schema = StructType([])  # Placeholder - load from config
        return spark.createDataFrame([], schema)


class CRMExtractor(BaseExtractor):
    """Extractor for CRM data (accounts, contacts, opportunities)."""

    def __init__(self, table_name: str, config: dict[str, Any]):
        super().__init__("crm", table_name, config)
        self.local_csv_path = f"aws/data/crm/{table_name}.csv"

    def extract(self, spark: SparkSession, **kwargs) -> DataFrame:
        """Extract CRM data."""
        if self.environment == "local":
            return self._read_local_csv(spark, self.local_csv_path)
        else:
            # Production: Use CRM API (Salesforce/HubSpot)
            return self._create_empty_dataframe(spark)

    def _create_empty_dataframe(self, spark: SparkSession) -> DataFrame:
        """Create empty DataFrame with CRM schema."""
        from pyspark.sql.types import StructType

        schema = StructType([])  # Placeholder
        return spark.createDataFrame([], schema)


class HubSpotExtractor(BaseExtractor):
    """Extractor for HubSpot CRM data with incremental support."""

    def __init__(self, table_name: str, config: dict[str, Any]):
        super().__init__("hubspot", table_name, config)
        self.local_csv_path = f"aws/data/crm/{table_name}.csv"

    def extract(
        self, spark: SparkSession, since_ts: datetime | None = None, **kwargs
    ) -> DataFrame:
        """Extract HubSpot data with incremental support."""
        if since_ts is None:
            since_ts = self._get_watermark()

        if self.environment == "local":
            df = self._read_local_csv(spark, self.local_csv_path)
            if since_ts and "last_modified_date" in df.columns:
                from pyspark.sql.functions import lit

                df = df.filter(col("last_modified_date") >= lit(since_ts))
        else:
            # Production: Use HubSpot API
            df = self._extract_from_api(spark, since_ts)

        # Update watermark
        if df.count() > 0:
            from project_a.utils.watermark_utils import get_latest_timestamp_from_df

            latest_ts = get_latest_timestamp_from_df(df, "last_modified_date")
            if latest_ts:
                self._set_watermark(latest_ts)

        return df

    def _extract_from_api(self, spark: SparkSession, since_ts: datetime | None) -> DataFrame:
        """Extract from HubSpot API."""
        from pyspark.sql.types import StructType

        schema = StructType([])
        return spark.createDataFrame([], schema)


class SnowflakeExtractor(BaseExtractor):
    """Extractor for Snowflake data with incremental support."""

    def __init__(self, table_name: str, config: dict[str, Any]):
        super().__init__("snowflake", table_name, config)

    def extract(
        self, spark: SparkSession, since_ts: datetime | None = None, **kwargs
    ) -> DataFrame:
        """Extract Snowflake data with watermark support."""
        # Get watermark if not provided
        if since_ts is None:
            since_ts = self._get_watermark()

        if self.environment == "local":
            sample_path = self.config.get("paths", {}).get(f"snowflake_{self.table_name}")
            df = self._read_local_csv(spark, sample_path)
            if since_ts and "order_date" in df.columns:
                from pyspark.sql.functions import lit

                df = df.filter(col("order_date") >= lit(since_ts))
        else:
            # Production: Use Snowflake JDBC
            snowflake_config = self.config.get("data_sources", {}).get("snowflake", {})
            query = self._build_query(since_ts)
            df = (
                spark.read.format("snowflake")
                .options(**snowflake_config)
                .option("query", query)
                .load()
            )

        # Update watermark
        if df.count() > 0:
            from project_a.utils.watermark_utils import get_latest_timestamp_from_df

            latest_ts = get_latest_timestamp_from_df(df, "order_date")
            if latest_ts:
                self._set_watermark(latest_ts)

        return df

    def _build_query(self, since_ts: datetime | None) -> str:
        """Build Snowflake query with optional watermark filter."""
        table_name_upper = self.table_name.upper()
        if since_ts:
            return f"SELECT * FROM {table_name_upper} WHERE LAST_MODIFIED_TS > '{since_ts.isoformat()}'"
        return f"SELECT * FROM {table_name_upper}"


class RedshiftExtractor(BaseExtractor):
    """Extractor for Redshift data with incremental support."""

    def __init__(self, table_name: str, config: dict[str, Any]):
        super().__init__("redshift", table_name, config)

    def extract(
        self, spark: SparkSession, since_ts: datetime | None = None, **kwargs
    ) -> DataFrame:
        """Extract Redshift data with watermark support."""
        if since_ts is None:
            since_ts = self._get_watermark()

        if self.environment == "local":
            sample_path = self.config.get("paths", {}).get(f"redshift_{self.table_name}")
            df = self._read_local_csv(spark, sample_path)
            if since_ts and "event_timestamp" in df.columns:
                from pyspark.sql.functions import lit

                df = df.filter(col("event_timestamp") >= lit(since_ts))
        else:
            # Production: Use Redshift JDBC
            redshift_config = self.config.get("data_sources", {}).get("redshift", {})
            query = self._build_query(since_ts)
            df = spark.read.format("jdbc").options(**redshift_config).option("query", query).load()

        # Update watermark
        if df.count() > 0:
            from project_a.utils.watermark_utils import get_latest_timestamp_from_df

            latest_ts = get_latest_timestamp_from_df(df, "event_timestamp")
            if latest_ts:
                self._set_watermark(latest_ts)

        return df

    def _build_query(self, since_ts: datetime | None) -> str:
        """Build Redshift query with optional watermark filter."""
        if since_ts:
            return f"SELECT * FROM {self.table_name} WHERE updated_at > '{since_ts.isoformat()}'"
        return f"SELECT * FROM {self.table_name}"


class FXRatesExtractor(BaseExtractor):
    """Extractor for FX rates with REST API support."""

    def __init__(self, config: dict[str, Any]):
        super().__init__("fx_rates", "rates", config)
        self.api_url = config.get("data_sources", {}).get("fx_rates", {}).get("api_url")
        self.api_key = config.get("data_sources", {}).get("fx_rates", {}).get("api_key")

    def extract(self, spark: SparkSession, date: str = None, **kwargs) -> DataFrame:
        """Extract FX rates from REST API or CSV."""
        if date is None:
            from datetime import datetime

            date = datetime.now().strftime("%Y-%m-%d")

        if self.environment == "local":
            csv_path = self.config.get("paths", {}).get(
                "fx_rates", "data/fx_rates_historical_730_days.csv"
            )
            df = self._read_local_csv(spark, csv_path)
            if "date" in df.columns:
                from pyspark.sql.functions import lit

                df = df.filter(col("date") == lit(date))
        else:
            # Production: Call REST API
            df = self._extract_from_rest_api(spark, date)

        return df

    def _extract_from_rest_api(self, spark: SparkSession, date: str) -> DataFrame:
        """
        Extract from REST API.

        Note: This implementation avoids the third-party ``requests`` library
        so that EMR Serverless jobs do not require it at runtime. If you need
        richer HTTP behaviour, consider running this extractor outside Spark
        and landing data to Bronze instead.
        """
        import json
        import urllib.error
        import urllib.parse
        import urllib.request

        if not self.api_url:
            raise ValueError("FX rates API URL not configured")

        params = {"date": date}
        query = urllib.parse.urlencode(params)
        url = f"{self.api_url}?{query}"

        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        req = urllib.request.Request(url, headers=headers, method="GET")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
                data = json.loads(body)
            df = spark.createDataFrame(data, samplingRatio=0.1)
            logger.info("Extracted FX rates from API for %s", date)
            return df
        except urllib.error.URLError as exc:
            logger.error("Failed to extract FX rates from API: %s", exc)
            raise


# Factory function for easy access
def get_extractor(source_name: str, table_name: str, config: dict[str, Any]) -> BaseExtractor:
    """Factory function to get appropriate extractor."""
    if source_name == "salesforce":
        return SalesforceExtractor(table_name, config)
    elif source_name == "crm":
        return CRMExtractor(table_name, config)
    elif source_name == "snowflake":
        return SnowflakeExtractor(table_name, config)
    elif source_name == "redshift":
        return RedshiftExtractor(table_name, config)
    elif source_name == "fx_rates":
        return FXRatesExtractor(config)
    elif source_name == "hubspot":
        return HubSpotExtractor(table_name, config)
    else:
        raise ValueError(f"Unknown source: {source_name}")


# Standalone incremental extraction function for legacy compatibility
def extract_incremental(
    spark: SparkSession,
    source_name: str,
    read_fn,
    read_options: dict[str, Any] | None = None,
) -> DataFrame:
    """
    Extract incrementally from a source using updated_at watermark.

    This is a legacy standalone function. For new code, use BaseExtractor classes
    which have built-in incremental support via watermarks.

    Args:
        spark: Spark session
        source_name: Logical source name (used for state path)
        read_fn: Callable that returns a DataFrame when invoked as read_fn(spark, **read_options)
        read_options: Optional kwargs for read_fn

    Returns:
        Filtered DataFrame containing only rows with updated_at > watermark
    """

    from pyspark.sql.functions import max as spark_max

    read_options = read_options or {}
    logger.info(f"Starting incremental extract for {source_name}")

    # Get watermark from state store
    state_store = get_state_store(config={})  # Will use default config
    watermark_str = state_store.get_watermark(source_name)
    watermark = None
    if watermark_str:
        watermark = datetime.fromisoformat(watermark_str.replace("Z", "+00:00"))

    # Load raw df via provided function
    raw_df: DataFrame = read_fn(spark, **read_options)
    if "updated_at" not in raw_df.columns:
        raise ValueError(
            "Input DataFrame must contain 'updated_at' column for incremental processing"
        )

    if watermark:
        from pyspark.sql.types import TimestampType

        df = raw_df.filter(
            col("updated_at").cast(TimestampType()) > lit(watermark).cast(TimestampType())
        )
        logger.info(f"Filtered to records with updated_at > {watermark}")
    else:
        df = raw_df
        logger.info("No watermark found, processing all records")

    # Compute new watermark
    if not df.isEmpty():
        max_result = df.agg(
            spark_max(col("updated_at").cast("timestamp")).alias("max_ts")
        ).collect()[0]["max_ts"]
        if max_result:
            state_store.set_watermark(source_name, max_result.isoformat())
            logger.info(f"Updated watermark for {source_name} -> {max_result}")
    else:
        logger.info("No new records to update watermark")

    return df
