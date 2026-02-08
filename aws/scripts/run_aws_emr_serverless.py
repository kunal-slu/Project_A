#!/usr/bin/env python3
"""
AWS EMR Serverless job submission script.
"""

import argparse
import logging
import os
import sys
import time
import zipfile
from pathlib import Path

import boto3

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from project_a.utils.config import load_conf
from project_a.utils.logging import get_trace_id, log_with_trace, setup_json_logging


def create_artifact_package(source_dir: str, output_path: str) -> str:
    """
    Create zip package of source code.

    Args:
        source_dir: Source directory to package
        output_path: Output zip file path

    Returns:
        Path to created zip file
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Creating artifact package: {output_path}")

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_dir):
            # Skip __pycache__ and .git directories
            dirs[:] = [d for d in dirs if d not in ["__pycache__", ".git", ".pytest_cache"]]

            for file in files:
                if file.endswith((".py", ".yaml", ".yml", ".json")):
                    file_path = os.path.join(root, file)
                    arc_path = os.path.relpath(file_path, source_dir)
                    zipf.write(file_path, arc_path)

    logger.info(f"Artifact package created: {output_path}")
    return output_path


def upload_to_s3(local_path: str, s3_path: str, aws_region: str) -> None:
    """
    Upload file to S3.

    Args:
        local_path: Local file path
        s3_path: S3 path (s3://bucket/key)
        aws_region: AWS region
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Uploading {local_path} to {s3_path}")

    s3_client = boto3.client("s3", region_name=aws_region)

    # Parse S3 path
    s3_path = s3_path.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)

    s3_client.upload_file(local_path, bucket, key)
    logger.info(f"Upload completed: s3://{bucket}/{key}")


def submit_emr_serverless_job(
    application_id: str,
    job_name: str,
    entry_point: str,
    spark_submit_params: list,
    s3_artifacts_path: str,
    aws_region: str,
) -> str:
    """
    Submit job to EMR Serverless.

    Args:
        application_id: EMR Serverless application ID
        job_name: Job name
        entry_point: Entry point script
        spark_submit_params: Spark submit parameters
        s3_artifacts_path: S3 path to artifacts
        aws_region: AWS region

    Returns:
        Job run ID
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Submitting EMR Serverless job: {job_name}")

    emr_client = boto3.client("emr-serverless", region_name=aws_region)

    response = emr_client.start_job_run(
        applicationId=application_id,
        executionRoleArn=f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/EMRServerlessExecutionRole",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_point,
                "entryPointArguments": [],
                "sparkSubmitParameters": " ".join(spark_submit_params),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"{s3_artifacts_path}/logs/"}
            }
        },
    )

    job_run_id = response["jobRunId"]
    logger.info(f"Job submitted successfully. Job Run ID: {job_run_id}")

    return job_run_id


def wait_for_job_completion(job_run_id: str, application_id: str, aws_region: str) -> str:
    """
    Wait for job completion and return final state.

    Args:
        job_run_id: Job run ID
        application_id: EMR Serverless application ID
        aws_region: AWS region

    Returns:
        Final job state
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Waiting for job completion: {job_run_id}")

    emr_client = boto3.client("emr-serverless", region_name=aws_region)

    while True:
        response = emr_client.get_job_run(applicationId=application_id, jobRunId=job_run_id)

        state = response["jobRun"]["state"]
        logger.info(f"Job state: {state}")

        if state in ["SUCCESS", "FAILED", "CANCELLED"]:
            break

        time.sleep(30)  # Wait 30 seconds before checking again

    return state


def main():
    """Main AWS EMR Serverless submission."""
    parser = argparse.ArgumentParser(description="Submit ETL job to AWS EMR Serverless")
    parser.add_argument("--conf", required=True, help="Configuration file path")
    args = parser.parse_args()

    # Setup logging
    trace_id = get_trace_id()
    logger = setup_json_logging(include_trace_id=True)
    log_with_trace(logger, "info", "Starting AWS EMR Serverless submission", trace_id)

    try:
        # Load configuration
        config = load_conf(args.conf)
        log_with_trace(logger, "info", f"Configuration loaded: {args.conf}", trace_id)

        # Get AWS configuration
        aws_region = config["glue"]["region"]
        emr_app_id = config["emr"]["application_id"]
        s3_bucket = os.environ.get("AWS_S3_BUCKET")

        if not s3_bucket:
            raise ValueError("AWS_S3_BUCKET environment variable is required")

        log_with_trace(logger, "info", f"AWS Region: {aws_region}, EMR App: {emr_app_id}", trace_id)

        # Create artifact package
        source_dir = str(Path(__file__).parent.parent / "src")
        artifact_path = "/tmp/etl-artifacts.zip"
        create_artifact_package(source_dir, artifact_path)

        # Upload artifacts to S3
        s3_artifacts_path = f"s3://{s3_bucket}/artifacts"
        upload_to_s3(artifact_path, f"{s3_artifacts_path}/etl-artifacts.zip", aws_region)

        # Prepare Spark submit parameters
        spark_submit_params = [
            "--conf",
            "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "--conf",
            "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "--conf",
            f"spark.sql.shuffle.partitions={config['runtime']['shuffle_partitions']}",
            "--py-files",
            f"{s3_artifacts_path}/etl-artifacts.zip",
            "--files",
            f"{s3_artifacts_path}/etl-artifacts.zip",
        ]

        # Submit job
        job_name = f"etl-pipeline-{int(time.time())}"
        entry_point = "scripts/run_local_etl.py"

        job_run_id = submit_emr_serverless_job(
            emr_app_id, job_name, entry_point, spark_submit_params, s3_artifacts_path, aws_region
        )

        # Wait for completion
        final_state = wait_for_job_completion(job_run_id, emr_app_id, aws_region)

        # Print results
        print("\n" + "=" * 60)
        print("AWS EMR SERVERLESS JOB RESULTS")
        print("=" * 60)
        print(f"Job Run ID: {job_run_id}")
        print(f"Final State: {final_state}")
        print(f"Application ID: {emr_app_id}")
        print(f"Region: {aws_region}")
        print("=" * 60)

        if final_state == "SUCCESS":
            log_with_trace(logger, "info", "EMR Serverless job completed successfully", trace_id)
            print("\n✅ Job completed successfully!")
        else:
            log_with_trace(logger, "error", f"EMR Serverless job failed: {final_state}", trace_id)
            print(f"\n❌ Job failed with state: {final_state}")
            sys.exit(1)

    except Exception as e:
        log_with_trace(
            logger,
            "error",
            f"AWS EMR Serverless submission failed: {str(e)}",
            trace_id,
            error=str(e),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
