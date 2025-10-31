"""
Custom Airflow sensors for pipeline monitoring.
"""

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import boto3
import logging

logger = logging.getLogger(__name__)


class S3PrefixSensor(BaseSensorOperator):
    """
    Sensor that waits for objects to appear in an S3 prefix.
    """
    
    @apply_defaults
    def __init__(
        self,
        bucket: str,
        prefix: str,
        aws_conn_id: str = "aws_default",
        check_fn=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id
        self.check_fn = check_fn or self._default_check
    
    def poke(self, context):
        """Check if condition is met."""
        s3_client = boto3.client("s3")
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix,
                MaxKeys=1
            )
            
            if "Contents" in response and len(response["Contents"]) > 0:
                return self.check_fn(response["Contents"][0])
            
            return False
        except Exception as e:
            logger.warning(f"S3PrefixSensor error: {e}")
            return False
    
    def _default_check(self, obj):
        """Default check function."""
        return True


class DataQualitySensor(BaseSensorOperator):
    """
    Sensor that waits for data quality checks to complete successfully.
    """
    
    @apply_defaults
    def __init__(
        self,
        dq_run_id: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.dq_run_id = dq_run_id
    
    def poke(self, context):
        """Check if DQ checks passed."""
        # This would query Great Expectations store or database
        # For now, return True as placeholder
        logger.info(f"Checking DQ status for run: {self.dq_run_id}")
        return True

