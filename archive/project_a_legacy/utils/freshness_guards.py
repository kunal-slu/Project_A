"""
Source freshness guards to avoid full reloads and time drift.
"""

import logging
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max

logger = logging.getLogger(__name__)


class FreshnessGuard:
    """Base class for source freshness validation."""

    def __init__(self, max_age_days: int = 2):
        self.max_age_days = max_age_days

    def check_freshness(self, df: DataFrame, date_column: str) -> bool:
        """
        Check if data is fresh enough.

        Args:
            df: DataFrame to check
            date_column: Name of the date column

        Returns:
            True if data is fresh, False otherwise
        """
        try:
            latest_date = df.agg(spark_max(col(date_column))).first()[0]
            if latest_date is None:
                logger.warning(f"No data found in {date_column}")
                return False

            # Convert to date if it's a datetime
            if isinstance(latest_date, datetime):
                latest_date = latest_date.date()

            cutoff_date = date.today() - timedelta(days=self.max_age_days)

            if latest_date < cutoff_date:
                logger.error(f"Data is stale: latest {date_column} = {latest_date}, cutoff = {cutoff_date}")
                return False

            logger.info(f"Data is fresh: latest {date_column} = {latest_date}")
            return True

        except Exception as e:
            logger.error(f"Error checking freshness: {e}")
            return False


class HubSpotFreshnessGuard(FreshnessGuard):
    """HubSpot-specific freshness guard."""

    def __init__(self, max_age_hours: int = 24):
        super().__init__(max_age_days=1)  # 1 day for HubSpot
        self.max_age_hours = max_age_hours

    def check_freshness(self, df: DataFrame, timestamp_column: str = "last_modified") -> bool:
        """Check HubSpot data freshness."""
        try:
            latest_timestamp = df.agg(spark_max(col(timestamp_column))).first()[0]
            if latest_timestamp is None:
                logger.warning("No HubSpot data found")
                return False

            # Convert to datetime if needed
            if isinstance(latest_timestamp, str):
                latest_timestamp = datetime.fromisoformat(latest_timestamp.replace('Z', '+00:00'))

            cutoff_time = datetime.now() - timedelta(hours=self.max_age_hours)

            if latest_timestamp < cutoff_time:
                logger.error(f"HubSpot data is stale: latest = {latest_timestamp}, cutoff = {cutoff_time}")
                return False

            logger.info(f"HubSpot data is fresh: latest = {latest_timestamp}")
            return True

        except Exception as e:
            logger.error(f"Error checking HubSpot freshness: {e}")
            return False


class SnowflakeFreshnessGuard(FreshnessGuard):
    """Snowflake-specific freshness guard with Airflow macro support."""

    def __init__(self, max_age_days: int = 2):
        super().__init__(max_age_days)

    def check_freshness(self, df: DataFrame, date_column: str = "event_ts") -> bool:
        """Check Snowflake data freshness."""
        # For Snowflake, we expect daily data
        return super().check_freshness(df, date_column)


class KafkaFreshnessGuard(FreshnessGuard):
    """Kafka-specific freshness guard using checkpoints."""

    def __init__(self, max_age_minutes: int = 60):
        super().__init__(max_age_days=0)  # Kafka is real-time
        self.max_age_minutes = max_age_minutes

    def check_freshness(self, df: DataFrame, timestamp_column: str = "event_timestamp") -> bool:
        """Check Kafka data freshness."""
        try:
            latest_timestamp = df.agg(spark_max(col(timestamp_column))).first()[0]
            if latest_timestamp is None:
                logger.warning("No Kafka data found")
                return False

            cutoff_time = datetime.now() - timedelta(minutes=self.max_age_minutes)

            if latest_timestamp < cutoff_time:
                logger.error(f"Kafka data is stale: latest = {latest_timestamp}, cutoff = {cutoff_time}")
                return False

            logger.info(f"Kafka data is fresh: latest = {latest_timestamp}")
            return True

        except Exception as e:
            logger.error(f"Error checking Kafka freshness: {e}")
            return False


class FXFreshnessGuard(FreshnessGuard):
    """FX rates freshness guard."""

    def __init__(self, max_age_days: int = 2):
        super().__init__(max_age_days)

    def check_freshness(self, df: DataFrame, date_column: str = "fx_date") -> bool:
        """Check FX rates freshness."""
        # FX rates should be updated daily
        return super().check_freshness(df, date_column)


def apply_freshness_window(df: DataFrame, source_type: str,
                          date_column: str, last_success_ts: datetime | None = None) -> DataFrame:
    """
    Apply freshness window filtering to DataFrame.

    Args:
        df: Input DataFrame
        source_type: Type of source (hubspot, snowflake, kafka, fx_rates)
        date_column: Name of the date/timestamp column
        last_success_ts: Last successful processing timestamp

    Returns:
        Filtered DataFrame
    """
    if last_success_ts is None:
        # Use default freshness window
        if source_type == "hubspot":
            cutoff = datetime.now() - timedelta(hours=24)
        elif source_type == "snowflake":
            cutoff = datetime.now() - timedelta(days=1)
        elif source_type == "kafka":
            cutoff = datetime.now() - timedelta(minutes=60)
        elif source_type == "fx_rates":
            cutoff = datetime.now() - timedelta(days=2)
        else:
            cutoff = datetime.now() - timedelta(days=1)
    else:
        cutoff = last_success_ts

    # Filter data based on cutoff
    filtered_df = df.filter(col(date_column) >= cutoff)

    logger.info(f"Applied freshness window to {source_type}: {cutoff}")
    return filtered_df
