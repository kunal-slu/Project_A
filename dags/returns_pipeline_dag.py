from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Assumes your container has an entrypoint: python -m src.pipeline.run_pipeline
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="returns_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["lakehouse", "returns"],
) as dag:

    env = "APP_ENV=prod DATA_ROOT=/mnt/data/data_extracted/data/lakehouse"

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f'{env} python -m src.pipeline.bronze_to_silver',
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f'{env} python -m src.pipeline.silver_to_gold',
    )

    finalize = BashOperator(
        task_id="finalize",
        bash_command=f'{env} python -m src.pipeline.run_pipeline --ingest-metrics-json --with-dr',
    )

    bronze_to_silver >> silver_to_gold >> finalize
