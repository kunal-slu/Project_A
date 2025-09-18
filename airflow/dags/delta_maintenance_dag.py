import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/project")
DELTA_PATH = os.getenv("DELTA_OUTPUT", "data/output_data/final_delta")
GOLD_FACT = os.getenv("GOLD_FACT", "data/lakehouse/gold/fact_orders")
VENV_ACTIVATE = os.getenv("VENV_ACTIVATE", ".venv/bin/activate")

# Standard DAG defaults for production
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email": [os.getenv('ALERT_EMAIL', 'data-team@company.com')],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(hours=2),
}

dag = DAG(
    dag_id="delta_maintenance",
    default_args=default_args,
    description="Delta maintenance: OPTIMIZE/ZORDER/VACUUM - Production",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    tags=["delta", "maintenance", "production"],
    doc_md="""
    # Delta Maintenance Pipeline

    This DAG performs daily Delta Lake maintenance operations:
    - OPTIMIZE: Compacts small files into larger ones
    - ZORDER: Optimizes query performance on frequently filtered columns
    - VACUUM: Removes old files based on retention policy

    ## Dependencies
    - Requires PROJECT_HOME environment variable
    - Requires Python virtual environment with Delta Lake dependencies
    - Requires valid Delta table paths

    ## SLA
    - Maintenance must complete within 2 hours
    - Failure notifications sent to data-team@company.com
    """,
)

# Prefer calling our maintenance Python script to handle file-based Delta tables reliably
maintenance_cmd = (
    "cd {{ params.project_home }} && "
    "source {{ params.venv }} && "
    "env -u SPARK_HOME python scripts/delta_maintenance.py --config {{ params.config_file }} | cat"
)

run_maintenance = BashOperator(
    task_id="run_delta_maintenance",
    bash_command=maintenance_cmd,
    params={
        "project_home": PROJECT_HOME,
        "venv": VENV_ACTIVATE,
        "config_file": os.getenv("MAINTENANCE_CONFIG", "config/config-prod.yaml"),
    },
    dag=dag,
    execution_timeout=timedelta(hours=1),
    retries=2,
    retry_delay=timedelta(minutes=5),
)

# Add monitoring task to track maintenance results
update_maintenance_metrics = BashOperator(
    task_id="update_maintenance_metrics",
    bash_command=(
        "cd {{ params.project_home }} && "
        "source {{ params.venv }} && "
        "python -m pyspark_interview_project.monitoring --maintenance-metrics --config {{ params.config_file }} | cat"
    ),
    params={
        "project_home": PROJECT_HOME,
        "venv": VENV_ACTIVATE,
        "config_file": os.getenv("MAINTENANCE_CONFIG", "config/config-prod.yaml"),
    },
    dag=dag,
    execution_timeout=timedelta(minutes=15),
    retries=1,
    retry_delay=timedelta(minutes=2),
)

run_maintenance >> update_maintenance_metrics


