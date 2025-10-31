"""
Custom Airflow operator for EMR Serverless job execution.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3
import time
import logging

logger = logging.getLogger(__name__)


class EMRServerlessOperator(BaseOperator):
    """
    Airflow operator to submit and monitor EMR Serverless jobs.
    """
    
    @apply_defaults
    def __init__(
        self,
        application_id: str,
        job_role_arn: str,
        entry_point: str,
        entry_point_args: list = None,
        spark_submit_parameters: dict = None,
        job_name: str = None,
        wait_for_completion: bool = True,
        aws_conn_id: str = "aws_default",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.application_id = application_id
        self.job_role_arn = job_role_arn
        self.entry_point = entry_point
        self.entry_point_args = entry_point_args or []
        self.spark_submit_parameters = spark_submit_parameters or {}
        self.job_name = job_name or self.task_id
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id
    
    def execute(self, context):
        """Execute the EMR Serverless job."""
        emr_client = boto3.client("emr-serverless", region_name=self.get_region())
        
        # Build job driver
        job_driver = {
            "sparkSubmit": {
                "entryPoint": self.entry_point,
                "entryPointArguments": self.entry_point_args,
                "sparkSubmitParameters": self._build_spark_submit_params()
            }
        }
        
        # Submit job
        logger.info(f"Submitting EMR Serverless job: {self.job_name}")
        response = emr_client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.job_role_arn,
            name=self.job_name,
            jobDriver=job_driver
        )
        
        job_run_id = response["jobRunId"]
        logger.info(f"Job submitted: {job_run_id}")
        
        if self.wait_for_completion:
            self._wait_for_completion(emr_client, job_run_id)
        
        return job_run_id
    
    def _build_spark_submit_params(self) -> str:
        """Build spark-submit parameters string."""
        params = []
        for key, value in self.spark_submit_parameters.items():
            if value is None:
                params.append(f"--conf {key}")
            else:
                params.append(f"--conf {key}={value}")
        return " ".join(params)
    
    def _wait_for_completion(self, emr_client, job_run_id: str):
        """Wait for job to complete."""
        logger.info(f"Waiting for job {job_run_id} to complete...")
        
        while True:
            response = emr_client.get_job_run(
                applicationId=self.application_id,
                jobRunId=job_run_id
            )
            
            state = response["jobRun"]["state"]
            logger.info(f"Job {job_run_id} state: {state}")
            
            if state in ["SUCCESS", "FAILED", "CANCELLED"]:
                if state != "SUCCESS":
                    error_msg = response["jobRun"].get("stateDetails", "Unknown error")
                    raise Exception(f"EMR Serverless job failed: {error_msg}")
                break
            
            time.sleep(30)  # Poll every 30 seconds
        
        logger.info(f"Job {job_run_id} completed successfully")
    
    def get_region(self) -> str:
        """Get AWS region from connection or default."""
        # This would typically come from Airflow connection
        # For now, return default
        return "us-east-1"

