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
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, col

from pyspark_interview_project.utils.state_store import get_state_store
from pyspark_interview_project.monitoring.metrics_collector import emit_rowcount, emit_duration
from pyspark_interview_project.monitoring.lineage_decorator import lineage_job
import time

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Base class for all extractors."""
    
    def __init__(self, source_name: str, table_name: str, config: Dict[str, Any]):
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
        self.environment = config.get('environment', 'local')
    
    def _read_local_csv(self, spark: SparkSession, path: str) -> DataFrame:
        """Read CSV file for local development."""
        logger.info(f"Reading local CSV: {path}")
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add standard metadata columns to DataFrame."""
        from pyspark.sql.functions import lit
        return df \
            .withColumn("record_source", lit(self.source_name)) \
            .withColumn("record_table", lit(self.table_name)) \
            .withColumn("ingest_timestamp", current_timestamp())
    
    def _get_watermark(self) -> Optional[datetime]:
        """Get watermark for incremental loading."""
        watermark_str = self.state_store.get_watermark(f"{self.source_name}_{self.table_name}")
        if watermark_str:
            return datetime.fromisoformat(watermark_str.replace('Z', '+00:00'))
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
            
            emit_rowcount("records_extracted", record_count, {
                "source": self.source_name,
                "table": self.table_name
            }, self.config)
            
            emit_duration("extraction_duration", duration_ms, {
                "source": self.source_name
            }, self.config)
            
            logger.info(f"✅ Extracted {record_count:,} records from {self.source_name}.{self.table_name}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to extract {self.source_name}.{self.table_name}: {e}")
            raise


class SalesforceExtractor(BaseExtractor):
    """Extractor for Salesforce objects (consolidates all Salesforce extractors)."""
    
    def __init__(self, table_name: str, config: Dict[str, Any]):
        super().__init__("salesforce", table_name, config)
        self.local_csv_path = f"aws/data/salesforce/salesforce_{table_name}_ready.csv"
    
    def extract(self, spark: SparkSession, **kwargs) -> DataFrame:
        """Extract Salesforce data."""
        if self.environment == 'local':
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
    
    def __init__(self, table_name: str, config: Dict[str, Any]):
        super().__init__("crm", table_name, config)
        self.local_csv_path = f"aws/data/crm/{table_name}.csv"
    
    def extract(self, spark: SparkSession, **kwargs) -> DataFrame:
        """Extract CRM data."""
        if self.environment == 'local':
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
    
    def __init__(self, table_name: str, config: Dict[str, Any]):
        super().__init__("hubspot", table_name, config)
        self.local_csv_path = f"aws/data/crm/{table_name}.csv"
    
    def extract(self, spark: SparkSession, since_ts: Optional[datetime] = None, **kwargs) -> DataFrame:
        """Extract HubSpot data with incremental support."""
        if since_ts is None:
            since_ts = self._get_watermark()
        
        if self.environment == 'local':
            df = self._read_local_csv(spark, self.local_csv_path)
            if since_ts and "last_modified_date" in df.columns:
                from pyspark.sql.functions import lit
                df = df.filter(col("last_modified_date") >= lit(since_ts))
        else:
            # Production: Use HubSpot API
            df = self._extract_from_api(spark, since_ts)
        
        # Update watermark
        if df.count() > 0:
            from pyspark_interview_project.utils.watermark_utils import get_latest_timestamp_from_df
            latest_ts = get_latest_timestamp_from_df(df, "last_modified_date")
            if latest_ts:
                self._set_watermark(latest_ts)
        
        return df
    
    def _extract_from_api(self, spark: SparkSession, since_ts: Optional[datetime]) -> DataFrame:
        """Extract from HubSpot API."""
        from pyspark.sql.types import StructType
        schema = StructType([])
        return spark.createDataFrame([], schema)


class SnowflakeExtractor(BaseExtractor):
    """Extractor for Snowflake data with incremental support."""
    
    def __init__(self, table_name: str, config: Dict[str, Any]):
        super().__init__("snowflake", table_name, config)
    
    def extract(self, spark: SparkSession, since_ts: Optional[datetime] = None, **kwargs) -> DataFrame:
        """Extract Snowflake data with watermark support."""
        # Get watermark if not provided
        if since_ts is None:
            since_ts = self._get_watermark()
        
        if self.environment == 'local':
            sample_path = self.config.get('paths', {}).get(f'snowflake_{self.table_name}')
            df = self._read_local_csv(spark, sample_path)
            if since_ts and "order_date" in df.columns:
                from pyspark.sql.functions import lit
                df = df.filter(col("order_date") >= lit(since_ts))
        else:
            # Production: Use Snowflake JDBC
            snowflake_config = self.config.get('data_sources', {}).get('snowflake', {})
            query = self._build_query(since_ts)
            df = spark.read \
                .format("snowflake") \
                .options(**snowflake_config) \
                .option("query", query) \
                .load()
        
        # Update watermark
        if df.count() > 0:
            from pyspark_interview_project.utils.watermark_utils import get_latest_timestamp_from_df
            latest_ts = get_latest_timestamp_from_df(df, "order_date")
            if latest_ts:
                self._set_watermark(latest_ts)
        
        return df
    
    def _build_query(self, since_ts: Optional[datetime]) -> str:
        """Build Snowflake query with optional watermark filter."""
        table_name_upper = self.table_name.upper()
        if since_ts:
            return f"SELECT * FROM {table_name_upper} WHERE LAST_MODIFIED_TS > '{since_ts.isoformat()}'"
        return f"SELECT * FROM {table_name_upper}"


class RedshiftExtractor(BaseExtractor):
    """Extractor for Redshift data with incremental support."""
    
    def __init__(self, table_name: str, config: Dict[str, Any]):
        super().__init__("redshift", table_name, config)
    
    def extract(self, spark: SparkSession, since_ts: Optional[datetime] = None, **kwargs) -> DataFrame:
        """Extract Redshift data with watermark support."""
        if since_ts is None:
            since_ts = self._get_watermark()
        
        if self.environment == 'local':
            sample_path = self.config.get('paths', {}).get(f'redshift_{self.table_name}')
            df = self._read_local_csv(spark, sample_path)
            if since_ts and "event_timestamp" in df.columns:
                from pyspark.sql.functions import lit
                df = df.filter(col("event_timestamp") >= lit(since_ts))
        else:
            # Production: Use Redshift JDBC
            redshift_config = self.config.get('data_sources', {}).get('redshift', {})
            query = self._build_query(since_ts)
            df = spark.read \
                .format("jdbc") \
                .options(**redshift_config) \
                .option("query", query) \
                .load()
        
        # Update watermark
        if df.count() > 0:
            from pyspark_interview_project.utils.watermark_utils import get_latest_timestamp_from_df
            latest_ts = get_latest_timestamp_from_df(df, "event_timestamp")
            if latest_ts:
                self._set_watermark(latest_ts)
        
        return df
    
    def _build_query(self, since_ts: Optional[datetime]) -> str:
        """Build Redshift query with optional watermark filter."""
        if since_ts:
            return f"SELECT * FROM {self.table_name} WHERE updated_at > '{since_ts.isoformat()}'"
        return f"SELECT * FROM {self.table_name}"


class FXRatesExtractor(BaseExtractor):
    """Extractor for FX rates with REST API support."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("fx_rates", "rates", config)
        self.api_url = config.get('data_sources', {}).get('fx_rates', {}).get('api_url')
        self.api_key = config.get('data_sources', {}).get('fx_rates', {}).get('api_key')
    
    def extract(self, spark: SparkSession, date: str = None, **kwargs) -> DataFrame:
        """Extract FX rates from REST API or CSV."""
        if date is None:
            from datetime import datetime
            date = datetime.now().strftime("%Y-%m-%d")
        
        if self.environment == 'local':
            csv_path = self.config.get('paths', {}).get('fx_rates', "data/fx_rates_historical_730_days.csv")
            df = self._read_local_csv(spark, csv_path)
            if "date" in df.columns:
                from pyspark.sql.functions import lit
                df = df.filter(col("date") == lit(date))
        else:
            # Production: Call REST API
            df = self._extract_from_rest_api(spark, date)
        
        return df
    
    def _extract_from_rest_api(self, spark: SparkSession, date: str) -> DataFrame:
        """Extract from REST API."""
        import requests
        if not self.api_url:
            raise ValueError("FX rates API URL not configured")
        
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        params = {"date": date}
        
        try:
            response = requests.get(self.api_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            df = spark.createDataFrame(data, samplingRatio=0.1)
            logger.info(f"Extracted FX rates from API for {date}")
            return df
        except Exception as e:
            logger.error(f"Failed to extract FX rates from API: {e}")
            raise


# Factory function for easy access
def get_extractor(source_name: str, table_name: str, config: Dict[str, Any]) -> BaseExtractor:
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
    read_options: Optional[Dict[str, Any]] = None,
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
    from datetime import timezone
    
    read_options = read_options or {}
    logger.info(f"Starting incremental extract for {source_name}")
    
    # Get watermark from state store
    state_store = get_state_store(config={})  # Will use default config
    watermark_str = state_store.get_watermark(source_name)
    watermark = None
    if watermark_str:
        watermark = datetime.fromisoformat(watermark_str.replace('Z', '+00:00'))
    
    # Load raw df via provided function
    raw_df: DataFrame = read_fn(spark, **read_options)
    if "updated_at" not in raw_df.columns:
        raise ValueError("Input DataFrame must contain 'updated_at' column for incremental processing")
    
    if watermark:
        from pyspark.sql.types import TimestampType
        df = raw_df.filter(col("updated_at").cast(TimestampType()) > lit(watermark).cast(TimestampType()))
        logger.info(f"Filtered to records with updated_at > {watermark}")
    else:
        df = raw_df
        logger.info("No watermark found, processing all records")
    
    # Compute new watermark
    if not df.isEmpty():
        max_result = df.agg(spark_max(col("updated_at").cast("timestamp")).alias("max_ts")).collect()[0]["max_ts"]
        if max_result:
            state_store.set_watermark(source_name, max_result.isoformat())
            logger.info(f"Updated watermark for {source_name} -> {max_result}")
    else:
        logger.info("No new records to update watermark")
    
    return df

