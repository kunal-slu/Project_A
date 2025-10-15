"""
Airflow DAG for orchestrating the PySpark Delta ETL pipeline.
This will run your main pipeline script daily at 2am.
If your Airflow, project, or venv is in a different path, update the relevant lines.
"""

import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Parameterize paths for portability; can be set via environment variables
PROJECT_HOME = os.getenv("PROJECT_HOME", "/opt/project")
PYSPARK_SCRIPT = os.getenv("SPARK_APP", "src/pyspark_interview_project/pipeline.py")
CONFIG_FILE = os.getenv("CONFIG_FILE", "config/config-dev.yaml")
VENV_ACTIVATE = os.getenv("VENV_ACTIVATE", ".venv/bin/activate")    # Use .venv not venv

# Standard DAG defaults for production
default_args = dict(
    owner="data-eng", 
    retries=3, 
    retry_delay=timedelta(minutes=5)
)

with DAG(
    dag_id="pyspark_delta_etl",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["etl", "bronze-silver-gold"],
) as dag:

    # Add --proc_date parameter for partitioned processing
    proc_date = "{{ ds }}"  # Airflow execution date in YYYY-MM-DD format

    # Trigger external data ingestion first
    trigger_external_ingestion = TriggerDagRunOperator(
        task_id="trigger_external_ingestion",
        trigger_dag_id="external_data_ingestion",
        wait_for_completion=True,
        poke_interval=60,
        timeout=timedelta(minutes=30)
    )

    use_spark_submit = os.getenv("USE_SPARK_SUBMIT", "false").lower() == "true"

    if use_spark_submit:
        try:
            from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
            run_pyspark_etl = SparkSubmitOperator(
                task_id="run_pyspark_delta_pipeline",
                application=f"{PROJECT_HOME}/{PYSPARK_SCRIPT}",
                name="pde_etl",
                application_args=[CONFIG_FILE, f"--proc_date={proc_date}"],
                verbose=False,
                dag=dag,
                execution_timeout=timedelta(hours=2),
                retries=2,
                retry_delay=timedelta(minutes=5),
            )
        except Exception:
            # Fallback to BashOperator if provider missing
            run_pyspark_etl = BashOperator(
                task_id='run_pyspark_delta_pipeline',
                bash_command=(
                    "cd {{ params.project_home }} && "
                    "source {{ params.venv_activate }} && "
                    "env -u SPARK_HOME python -m pyspark_interview_project {{ params.config_file }} --proc_date={{ params.proc_date }} | cat"
                ),
                params={
                    "project_home": PROJECT_HOME,
                    "venv_activate": VENV_ACTIVATE,
                    "config_file": CONFIG_FILE,
                    "proc_date": proc_date,
                },
                dag=dag,
                execution_timeout=timedelta(hours=2),
                retries=2,
                retry_delay=timedelta(minutes=5),
            )
    else:
        # Use BashOperator for direct Python execution
        run_pyspark_etl = BashOperator(
            task_id='run_pyspark_delta_pipeline',
            bash_command=(
                "cd {{ params.project_home }} && "
                "source {{ params.venv_activate }} && "
                "python {{ params.pyspark_script }} {{ params.config_file }} --proc_date={{ params.proc_date }} | cat"
            ),
            params={
                "project_home": PROJECT_HOME,
                "venv_activate": VENV_ACTIVATE,
                "pyspark_script": PYSPARK_SCRIPT,
                "config_file": CONFIG_FILE,
                "proc_date": proc_date,
            },
            dag=dag,
            execution_timeout=timedelta(hours=2),
            retries=2,
            retry_delay=timedelta(minutes=5),
        )

    # Add data quality validation task
    validate_data_quality = BashOperator(
        task_id='validate_data_quality',
        bash_command=(
            "cd {{ params.project_home }} && "
            "source {{ params.venv_activate }} && "
            "python -m pyspark_interview_project.data_quality_suite --config {{ params.config_file }} --proc_date={{ params.proc_date }} | cat"
        ),
        params={
            "project_home": PROJECT_HOME,
            "venv_activate": VENV_ACTIVATE,
            "config_file": CONFIG_FILE,
            "proc_date": proc_date,
        },
        dag=dag,
        execution_timeout=timedelta(minutes=30),
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    # Add monitoring update task
    update_monitoring = BashOperator(
        task_id='update_monitoring',
        bash_command=(
            "cd {{ params.project_home }} && "
            "source {{ params.venv_activate }} && "
            "python -m pyspark_interview_project.monitoring --config {{ params.config_file }} --proc_date={{ params.proc_date }} | cat"
        ),
        params={
            "project_home": PROJECT_HOME,
            "venv_activate": VENV_ACTIVATE,
            "config_file": CONFIG_FILE,
            "proc_date": proc_date,
        },
        dag=dag,
        execution_timeout=timedelta(minutes=15),
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    # Define task dependencies
    trigger_external_ingestion >> run_pyspark_etl >> validate_data_quality >> update_monitoring
