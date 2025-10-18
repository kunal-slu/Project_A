"""
Production Delta Lake ETL Pipeline DAG
Fixed to use proper repo paths and BashOperator
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime
import os

# Use env var if set; otherwise fall back to the current working dir
REPO_ROOT = os.environ.get("PROJECT_ROOT", os.getcwd())
CFG_PATH = os.environ.get("PROJECT_CONFIG", f"{REPO_ROOT}/config/local.yaml")

with DAG(
    dag_id="delta_lake_etl_pipeline_dag",
    description="Production Delta Lake ETL Pipeline",
    schedule_interval=None,  # set cron later; None for manual trigger while testing
    start_date=datetime(2025, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["production", "etl", "delta-lake"],
) as dag:

    start_pipeline = BashOperator(
        task_id="start_pipeline",
        bash_command='echo "🚀 Starting Production Delta Lake ETL Pipeline..."'
    )

    run_etl_pipeline = BashOperator(
        task_id="run_etl_pipeline",
        bash_command=(
            "set -euo pipefail; "
            "cd {{ params.repo_root }}; "
            "export PYTHONPATH={{ params.repo_root }}/src:${PYTHONPATH:-}; "
            # if you use a venv, activate it here:
            # "source .venv/bin/activate; "
            "python -m pyspark_interview_project.cli "
            "--config {{ params.cfg_path }} --env local --cmd full"
        ),
        params={"repo_root": REPO_ROOT, "cfg_path": CFG_PATH},
    )

    validate_output = BashOperator(
        task_id="validate_output",
        bash_command=(
            "cd {{ params.repo_root }}; "
            "echo '🔍 Validating Delta Lake output...'; "
            "ls -la data/lakehouse_delta_standard || true; "
            "find data/lakehouse_delta_standard -name _delta_log -type d | wc -l | xargs echo 'Delta tables:'; "
            # optional: show a quick count
            "python - <<'PY'\n"
            "import os\n"
            "delta_path = 'data/lakehouse_delta_standard'\n"
            "if os.path.exists(delta_path):\n"
            "  layers = ['bronze', 'silver', 'gold']\n"
            "  total_files = 0\n"
            "  total_versions = 0\n"
            "  for layer in layers:\n"
            "    layer_path = os.path.join(delta_path, layer)\n"
            "    if os.path.exists(layer_path):\n"
            "      for table_dir in os.listdir(layer_path):\n"
            "        table_path = os.path.join(layer_path, table_dir)\n"
            "        if os.path.isdir(table_path):\n"
            "          parquet_files = [f for f in os.listdir(table_path) if f.endswith('.parquet')]\n"
            "          delta_log_path = os.path.join(table_path, '_delta_log')\n"
            "          log_files = []\n"
            "          if os.path.exists(delta_log_path):\n"
            "            log_files = [f for f in os.listdir(delta_log_path) if f.endswith('.json')]\n"
            "          total_files += len(parquet_files)\n"
            "          total_versions += len(log_files)\n"
            "          print(f'{layer}.{table_dir}: {len(parquet_files)} files, {len(log_files)} versions')\n"
            "  print(f'Total: {total_files} files, {total_versions} versions')\n"
            "else:\n"
            "  print('Delta Lake data not found')\n"
            "PY\n"
        ),
        params={"repo_root": REPO_ROOT},
    )

    end_pipeline = BashOperator(
        task_id="end_pipeline",
        bash_command='echo "✅ Production Delta Lake ETL Pipeline completed successfully!"'
    )

    start_pipeline >> run_etl_pipeline >> validate_output >> end_pipeline
