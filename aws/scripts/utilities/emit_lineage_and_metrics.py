#!/usr/bin/env python3
"""
Emit lineage and metrics for ETL pipeline operations.

This job provides comprehensive observability by:
- Creating unique run IDs for each pipeline execution
- Logging detailed metrics (row counts, processing time, etc.)
- Emitting lineage information to OpenLineage/CloudWatch
- Tracking data flow from source to target
- Providing audit trail for compliance and debugging
"""

import sys
import logging
import json
import uuid
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession

# Add project root to path
sys.path.append('/opt/airflow/dags/src')

from project_a.utils.spark_session import build_spark
from project_a.utils.config import load_config

logger = logging.getLogger(__name__)


@dataclass
class RunMetadata:
    """Metadata for a pipeline run."""
    run_id: str
    job_name: str
    start_time: str
    end_time: str
    status: str
    input_tables: List[str]
    output_tables: List[str]
    row_counts: Dict[str, int]
    processing_time_seconds: float
    environment: str
    version: str


@dataclass
class LineageEvent:
    """Lineage event for data flow tracking."""
    event_id: str
    run_id: str
    job_name: str
    timestamp: str
    source_tables: List[str]
    target_tables: List[str]
    transformations: List[str]
    data_quality_score: float
    environment: str


class LineageEmitter:
    """Handles lineage and metrics emission for ETL operations."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.lineage_config = config.get('lineage', {})
        self.namespace = self.lineage_config.get('namespace', 'company-etl')
        self.environment = config.get('environment', 'aws-prod')
        
    def create_run_metadata(self, job_name: str, input_tables: List[str], 
                          output_tables: List[str], start_time: datetime) -> RunMetadata:
        """Create run metadata for a pipeline execution."""
        
        run_id = f"{job_name}_{int(start_time.timestamp())}_{str(uuid.uuid4())[:8]}"
        
        return RunMetadata(
            run_id=run_id,
            job_name=job_name,
            start_time=start_time.isoformat(),
            end_time="",  # Will be set when job completes
            status="running",
            input_tables=input_tables,
            output_tables=output_tables,
            row_counts={},
            processing_time_seconds=0.0,
            environment=self.environment,
            version=self.config.get('version', '1.0.0')
        )
    
    def emit_lineage_event(self, run_metadata: RunMetadata, 
                          transformations: List[str], 
                          data_quality_score: float) -> LineageEvent:
        """Create and emit lineage event."""
        
        lineage_event = LineageEvent(
            event_id=str(uuid.uuid4()),
            run_id=run_metadata.run_id,
            job_name=run_metadata.job_name,
            timestamp=datetime.now(timezone.utc).isoformat(),
            source_tables=run_metadata.input_tables,
            target_tables=run_metadata.output_tables,
            transformations=transformations,
            data_quality_score=data_quality_score,
            environment=self.environment
        )
        
        # Emit to CloudWatch
        self._emit_to_cloudwatch(lineage_event)
        
        # Emit to OpenLineage (if configured)
        if self.lineage_config.get('openlineage_enabled', False):
            self._emit_to_openlineage(lineage_event)
        
        return lineage_event
    
    def _emit_to_cloudwatch(self, lineage_event: LineageEvent) -> None:
        """Emit lineage event to CloudWatch."""
        
        try:
            import boto3
            
            cloudwatch = boto3.client('cloudwatch', region_name=self.config['aws']['region'])
            namespace = f"{self.namespace}/lineage"
            
            # Emit custom metrics
            cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=[
                    {
                        'MetricName': 'DataQualityScore',
                        'Value': lineage_event.data_quality_score,
                        'Unit': 'None',
                        'Dimensions': [
                            {'Name': 'JobName', 'Value': lineage_event.job_name},
                            {'Name': 'Environment', 'Value': lineage_event.environment}
                        ]
                    },
                    {
                        'MetricName': 'PipelineExecution',
                        'Value': 1,
                        'Unit': 'Count',
                        'Dimensions': [
                            {'Name': 'JobName', 'Value': lineage_event.job_name},
                            {'Name': 'Environment', 'Value': lineage_event.environment}
                        ]
                    }
                ]
            )
            
            # Log detailed lineage information
            logger.info(f"Lineage event emitted to CloudWatch: {lineage_event.event_id}")
            
        except Exception as e:
            logger.warning(f"Failed to emit to CloudWatch: {str(e)}")
    
    def _emit_to_openlineage(self, lineage_event: LineageEvent) -> None:
        """Emit lineage event to OpenLineage."""
        
        try:
            # This would integrate with OpenLineage client
            # For now, we'll log the event structure
            logger.info(f"OpenLineage event: {asdict(lineage_event)}")
            
        except Exception as e:
            logger.warning(f"Failed to emit to OpenLineage: {str(e)}")
    
    def log_table_metrics(self, run_metadata: RunMetadata, 
                         table_name: str, row_count: int, 
                         s3_path: str) -> None:
        """Log metrics for a specific table."""
        
        run_metadata.row_counts[table_name] = row_count
        
        logger.info(f"Table metrics - {table_name}: {row_count} rows, path: {s3_path}")
        
        # Emit table-level metrics to CloudWatch
        try:
            import boto3
            
            cloudwatch = boto3.client('cloudwatch', region_name=self.config['aws']['region'])
            namespace = f"{self.namespace}/tables"
            
            cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=[
                    {
                        'MetricName': 'RowCount',
                        'Value': row_count,
                        'Unit': 'Count',
                        'Dimensions': [
                            {'Name': 'TableName', 'Value': table_name},
                            {'Name': 'Environment', 'Value': self.environment}
                        ]
                    }
                ]
            )
            
        except Exception as e:
            logger.warning(f"Failed to emit table metrics: {str(e)}")
    
    def finalize_run_metadata(self, run_metadata: RunMetadata, 
                            status: str, end_time: datetime) -> None:
        """Finalize run metadata with completion information."""
        
        run_metadata.end_time = end_time.isoformat()
        run_metadata.status = status
        run_metadata.processing_time_seconds = (end_time - datetime.fromisoformat(run_metadata.start_time)).total_seconds()
        
        # Emit final run metrics
        try:
            import boto3
            
            cloudwatch = boto3.client('cloudwatch', region_name=self.config['aws']['region'])
            namespace = f"{self.namespace}/runs"
            
            cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=[
                    {
                        'MetricName': 'ProcessingTime',
                        'Value': run_metadata.processing_time_seconds,
                        'Unit': 'Seconds',
                        'Dimensions': [
                            {'Name': 'JobName', 'Value': run_metadata.job_name},
                            {'Name': 'Status', 'Value': status},
                            {'Name': 'Environment', 'Value': self.environment}
                        ]
                    },
                    {
                        'MetricName': 'TotalRowsProcessed',
                        'Value': sum(run_metadata.row_counts.values()),
                        'Unit': 'Count',
                        'Dimensions': [
                            {'Name': 'JobName', 'Value': run_metadata.job_name},
                            {'Name': 'Environment', 'Value': self.environment}
                        ]
                    }
                ]
            )
            
        except Exception as e:
            logger.warning(f"Failed to emit final run metrics: {str(e)}")
        
        logger.info(f"Run {run_metadata.run_id} completed: {status}, "
                   f"processed {sum(run_metadata.row_counts.values())} rows in "
                   f"{run_metadata.processing_time_seconds:.2f} seconds")


def emit_pipeline_lineage(spark: SparkSession, config: Dict[str, Any],
                         job_name: str, input_tables: List[str], 
                         output_tables: List[str], transformations: List[str],
                         data_quality_score: float = 1.0) -> str:
    """Emit lineage and metrics for a complete pipeline run."""
    
    logger.info(f"Starting lineage emission for job: {job_name}")
    
    try:
        # Initialize lineage emitter
        emitter = LineageEmitter(spark, config)
        
        # Create run metadata
        start_time = datetime.now(timezone.utc)
        run_metadata = emitter.create_run_metadata(
            job_name, input_tables, output_tables, start_time
        )
        
        # Emit lineage event
        lineage_event = emitter.emit_lineage_event(
            run_metadata, transformations, data_quality_score
        )
        
        logger.info(f"Lineage emission completed for run: {run_metadata.run_id}")
        return run_metadata.run_id
        
    except Exception as e:
        logger.error(f"Failed to emit lineage for job {job_name}: {str(e)}")
        raise


def log_table_operation(spark: SparkSession, config: Dict[str, Any],
                       run_id: str, table_name: str, row_count: int, 
                       s3_path: str) -> None:
    """Log metrics for a specific table operation."""
    
    try:
        emitter = LineageEmitter(spark, config)
        
        # Create a minimal run metadata for logging
        run_metadata = RunMetadata(
            run_id=run_id,
            job_name="table_operation",
            start_time=datetime.now(timezone.utc).isoformat(),
            end_time="",
            status="running",
            input_tables=[],
            output_tables=[table_name],
            row_counts={},
            processing_time_seconds=0.0,
            environment=config.get('environment', 'aws-prod'),
            version=config.get('version', '1.0.0')
        )
        
        emitter.log_table_metrics(run_metadata, table_name, row_count, s3_path)
        
    except Exception as e:
        logger.error(f"Failed to log table operation: {str(e)}")


def main():
    """Main entry point for lineage emission job."""
    
    # Initialize Spark
    spark = build_spark("LineageEmissionJob")
    
    # Load configuration
    config = load_config("aws/config/config-prod.yaml")
    
    try:
        # Example usage - emit lineage for a complete pipeline
        job_name = "bronze_to_silver_transform"
        input_tables = ["bronze_orders", "bronze_customers"]
        output_tables = ["silver_orders", "silver_customers"]
        transformations = ["deduplication", "standardization", "validation"]
        
        run_id = emit_pipeline_lineage(
            spark, config, job_name, input_tables, 
            output_tables, transformations, data_quality_score=0.95
        )
        
        logger.info(f"Lineage emission job completed successfully: {run_id}")
        
    except Exception as e:
        logger.error(f"Lineage emission job failed: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
