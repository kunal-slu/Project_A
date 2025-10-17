#!/bin/bash
# Easy DAG Runner Script

echo "🌪️ AIRFLOW DAG RUNNER"
echo "====================="
echo ""
echo "Available DAGs:"
echo "1. demo_pipeline_dag - Complete ETL Pipeline (7 tasks)"
echo "2. simple_test_dag - Simple Test DAG (3 tasks)"
echo "3. list - Show available DAGs"
echo ""

if [ "$1" = "demo" ]; then
    echo "🚀 Running Demo Pipeline DAG..."
    export AIRFLOW__CORE__XCOM_BACKEND=airflow.models.xcom.BaseXCom
    python3 simple_dag_runner.py --dag demo_pipeline_dag
elif [ "$1" = "test" ]; then
    echo "🚀 Running Simple Test DAG..."
    export AIRFLOW__CORE__XCOM_BACKEND=airflow.models.xcom.BaseXCom
    python3 simple_dag_runner.py --dag simple_test_dag
elif [ "$1" = "list" ]; then
    echo "📋 Available DAGs:"
    export AIRFLOW__CORE__XCOM_BACKEND=airflow.models.xcom.BaseXCom
    python3 simple_dag_runner.py --list
else
    echo "Usage:"
    echo "  ./run_dags.sh demo    # Run demo pipeline DAG"
    echo "  ./run_dags.sh test    # Run simple test DAG"
    echo "  ./run_dags.sh list    # List available DAGs"
fi
