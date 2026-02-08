"""
Production ETL DAG.

Compatibility DAG entrypoint expected by AWS contract tests.
"""

from aws.dags.daily_batch_pipeline_dag import dag

__all__ = ["dag"]
