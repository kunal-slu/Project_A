from datetime import timedelta
import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Standard DAG defaults for production
default_args = dict(
    owner="data-eng", 
    retries=3, 
    retry_delay=timedelta(minutes=5)
)

# Add --proc_date parameter for partitioned processing
proc_date = "{{ ds }}"  # Airflow execution date in YYYY-MM-DD format

with DAG(
    dag_id="returns_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["etl", "bronze-silver-gold"],
) as dag:

    env = f"APP_ENV=prod DATA_ROOT=/mnt/data/data_extracted/data/lakehouse PROC_DATE={proc_date}"

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f'{env} python -m src.pipeline.bronze_to_silver --proc_date={proc_date}',
        execution_timeout=timedelta(minutes=30),
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f'{env} python -m src.pipeline.silver_to_gold --proc_date={proc_date}',
        execution_timeout=timedelta(minutes=30),
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    finalize = BashOperator(
        task_id="finalize",
        bash_command=f'{env} python -m src.pipeline.run_pipeline --ingest-metrics-json --with-dr --proc_date={proc_date}',
        execution_timeout=timedelta(hours=1),
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # Add data quality validation
    validate_returns = BashOperator(
        task_id="validate_returns",
        bash_command=f'{env} python -m pyspark_interview_project.data_quality_suite --config config/config-prod.yaml --proc_date={proc_date}',
        execution_timeout=timedelta(minutes=15),
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    bronze_to_silver >> silver_to_gold >> finalize >> validate_returns
