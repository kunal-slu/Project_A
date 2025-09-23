"""
EMR Serverless helper functions for Airflow.
Provides utilities for starting, monitoring, and managing EMR Serverless jobs.
"""

import boto3
import time
import logging
from typing import Dict, Any, Optional
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class EMRServerlessHelper:
    """Helper class for EMR Serverless operations."""
    
    def __init__(self, aws_conn_id: str = 'aws_default'):
        """
        Initialize EMR Serverless helper.
        
        Args:
            aws_conn_id: Airflow AWS connection ID
        """
        self.aws_conn_id = aws_conn_id
        self.conn = BaseHook.get_connection(aws_conn_id)
        
        # Initialize boto3 clients
        self.emr_client = boto3.client(
            'emr-serverless',
            region_name=self.conn.extra_dejson.get('region_name', 'us-east-1'),
            aws_access_key_id=self.conn.login,
            aws_secret_access_key=self.conn.password
        )
        
        self.logs_client = boto3.client(
            'logs',
            region_name=self.conn.extra_dejson.get('region_name', 'us-east-1'),
            aws_access_key_id=self.conn.login,
            aws_secret_access_key=self.conn.password
        )
    
    def start_job_run(
        self,
        application_id: str,
        execution_role_arn: str,
        job_driver: Dict[str, Any],
        configuration_overrides: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Start an EMR Serverless job run.
        
        Args:
            application_id: EMR Serverless application ID
            execution_role_arn: IAM role ARN for job execution
            job_driver: Job driver configuration
            configuration_overrides: Optional configuration overrides
            name: Optional job run name
            tags: Optional tags for the job run
            
        Returns:
            Job run ID
        """
        try:
            response = self.emr_client.start_job_run(
                applicationId=application_id,
                executionRoleArn=execution_role_arn,
                jobDriver=job_driver,
                configurationOverrides=configuration_overrides or {},
                name=name,
                tags=tags or {}
            )
            
            job_run_id = response['jobRunId']
            logger.info(f"Started EMR Serverless job run: {job_run_id}")
            return job_run_id
            
        except Exception as e:
            logger.error(f"Failed to start EMR Serverless job: {e}")
            raise
    
    def get_job_run(self, application_id: str, job_run_id: str) -> Dict[str, Any]:
        """
        Get job run details.
        
        Args:
            application_id: EMR Serverless application ID
            job_run_id: Job run ID
            
        Returns:
            Job run details
        """
        try:
            response = self.emr_client.get_job_run(
                applicationId=application_id,
                jobRunId=job_run_id
            )
            return response['jobRun']
            
        except Exception as e:
            logger.error(f"Failed to get job run details: {e}")
            raise
    
    def wait_for_job_completion(
        self,
        application_id: str,
        job_run_id: str,
        max_wait_time: int = 3600,
        poll_interval: int = 30
    ) -> Dict[str, Any]:
        """
        Wait for job completion.
        
        Args:
            application_id: EMR Serverless application ID
            job_run_id: Job run ID
            max_wait_time: Maximum wait time in seconds
            poll_interval: Polling interval in seconds
            
        Returns:
            Final job run details
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            job_run = self.get_job_run(application_id, job_run_id)
            state = job_run['state']
            
            logger.info(f"Job run {job_run_id} state: {state}")
            
            if state in ['SUCCESS', 'FAILED', 'CANCELLED']:
                return job_run
            
            time.sleep(poll_interval)
        
        raise TimeoutError(f"Job run {job_run_id} did not complete within {max_wait_time} seconds")
    
    def get_job_logs(
        self,
        application_id: str,
        job_run_id: str,
        log_type: str = 'spark-driver'
    ) -> str:
        """
        Get job run logs.
        
        Args:
            application_id: EMR Serverless application ID
            job_run_id: Job run ID
            log_type: Type of logs to retrieve
            
        Returns:
            Log content
        """
        try:
            # Get log stream name from job run
            job_run = self.get_job_run(application_id, job_run_id)
            
            # Extract log group and stream from job run
            log_group = f"/aws/emr-serverless/{application_id}"
            log_stream = f"jobs/{job_run_id}/{log_type}"
            
            # Get logs from CloudWatch
            response = self.logs_client.get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream,
                startFromHead=True
            )
            
            # Format logs
            log_content = "\n".join([
                f"{event['timestamp']}: {event['message']}"
                for event in response['events']
            ])
            
            return log_content
            
        except Exception as e:
            logger.error(f"Failed to get job logs: {e}")
            return f"Error retrieving logs: {e}"
    
    def cancel_job_run(self, application_id: str, job_run_id: str) -> None:
        """
        Cancel a job run.
        
        Args:
            application_id: EMR Serverless application ID
            job_run_id: Job run ID
        """
        try:
            self.emr_client.cancel_job_run(
                applicationId=application_id,
                jobRunId=job_run_id
            )
            logger.info(f"Cancelled job run: {job_run_id}")
            
        except Exception as e:
            logger.error(f"Failed to cancel job run: {e}")
            raise


def start_emr_job_run(
    application_id: str,
    execution_role_arn: str,
    entry_point: str,
    entry_args: list,
    spark_params: str = "",
    name: Optional[str] = None,
    aws_conn_id: str = 'aws_default'
) -> str:
    """
    Start an EMR Serverless job run with simplified parameters.
    
    Args:
        application_id: EMR Serverless application ID
        execution_role_arn: IAM role ARN for job execution
        entry_point: S3 path to the entry point script
        entry_args: List of arguments for the entry point
        spark_params: Spark configuration parameters
        name: Optional job run name
        aws_conn_id: Airflow AWS connection ID
        
    Returns:
        Job run ID
    """
    helper = EMRServerlessHelper(aws_conn_id)
    
    job_driver = {
        "sparkSubmit": {
            "entryPoint": entry_point,
            "entryPointArguments": entry_args,
            "sparkSubmitParameters": spark_params
        }
    }
    
    return helper.start_job_run(
        application_id=application_id,
        execution_role_arn=execution_role_arn,
        job_driver=job_driver,
        name=name
    )


def wait_for_emr_job_completion(
    application_id: str,
    job_run_id: str,
    max_wait_time: int = 3600,
    aws_conn_id: str = 'aws_default'
) -> Dict[str, Any]:
    """
    Wait for EMR Serverless job completion.
    
    Args:
        application_id: EMR Serverless application ID
        job_run_id: Job run ID
        max_wait_time: Maximum wait time in seconds
        aws_conn_id: Airflow AWS connection ID
        
    Returns:
        Final job run details
    """
    helper = EMRServerlessHelper(aws_conn_id)
    return helper.wait_for_job_completion(
        application_id=application_id,
        job_run_id=job_run_id,
        max_wait_time=max_wait_time
    )
