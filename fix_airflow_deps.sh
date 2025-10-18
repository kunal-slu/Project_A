#!/bin/bash
# Fix Airflow dependency conflicts

echo "🔧 Fixing Airflow dependency conflicts..."

# Upgrade typing-extensions to fix Sentinel import issue
pip3 install --upgrade "typing-extensions>=4.14.1"

# Fix other conflicting packages
pip3 install --upgrade "attrs>=24.3.0"
pip3 install --upgrade "docutils>=0.18.1"

echo "✅ Dependencies fixed!"
echo ""
echo "Now you can run:"
echo "export PROJECT_ROOT=\"\$(pwd)\""
echo "export AIRFLOW_HOME=\"\$PROJECT_ROOT/.airflow_local\""
echo "export AIRFLOW__CORE__DAGS_FOLDER=\"\$PROJECT_ROOT/airflow/dags\""
echo "export AIRFLOW__CORE__LOAD_EXAMPLES=False"
echo ""
echo "airflow db migrate"
echo "airflow users create --username admin --firstname a --lastname b --role Admin --email a@b.c --password admin"
echo ""
echo "airflow webserver -D"
echo "airflow scheduler -D"
