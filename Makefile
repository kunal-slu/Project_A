# AWS Production ETL Pipeline - Makefile
# Provides convenient targets for development and operations

.PHONY: help test lint sync-dags deploy clean format check-deps

# Default target
help:
	@echo "🚀 AWS Production ETL Pipeline - Available Targets"
	@echo "=================================================="
	@echo ""
	@echo "📋 Development:"
	@echo "  test         - Run all tests"
	@echo "  lint         - Run linting and formatting checks"
	@echo "  format       - Format code with black and isort"
	@echo "  check-deps   - Check Python dependencies"
	@echo ""
	@echo "🚀 Deployment:"
	@echo "  deploy       - Deploy to AWS production"
	@echo "  sync-dags    - Sync DAGs to MWAA"
	@echo "  infra-plan   - Plan Terraform infrastructure"
	@echo "  infra-apply  - Apply Terraform infrastructure"
	@echo ""
	@echo "🧹 Maintenance:"
	@echo "  clean        - Clean build artifacts and caches"
	@echo "  optimize     - Optimize Delta tables"
	@echo "  dq-check     - Run data quality checks"
	@echo ""
	@echo "📊 Monitoring:"
	@echo "  status       - Check pipeline status"
	@echo "  logs         - View recent logs"
	@echo "  metrics      - Show performance metrics"

# Development targets
test:
	@echo "🧪 Running tests..."
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term

lint:
	@echo "🔍 Running linting checks..."
	black --check src/ tests/ --line-length=120
	isort --check-only src/ tests/ --profile black
	flake8 src/ tests/ --max-line-length=120 --ignore=E203,W503,F401
	mypy src/ --ignore-missing-imports --no-strict-optional

format:
	@echo "✨ Formatting code..."
	black src/ tests/ --line-length=120
	isort src/ tests/ --profile black

check-deps:
	@echo "📦 Checking dependencies..."
	pip check
	pip-audit

# Deployment targets
deploy:
	@echo "🚀 Deploying to AWS production..."
	./aws/scripts/aws_production_deploy.sh

sync-dags:
	@echo "🔄 Syncing DAGs to MWAA..."
	aws s3 sync aws/dags/ s3://$${MWAA_DAGS_BUCKET}/ --delete

infra-plan:
	@echo "📋 Planning Terraform infrastructure..."
	cd infra/terraform && terraform plan -var-file="variables.tfvars"

infra-apply:
	@echo "🏗️ Applying Terraform infrastructure..."
	cd infra/terraform && terraform apply -var-file="variables.tfvars"

# Maintenance targets
clean:
	@echo "🧹 Cleaning build artifacts..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	rm -rf htmlcov/ 2>/dev/null || true
	rm -rf .pytest_cache/ 2>/dev/null || true
	rm -rf dist/ 2>/dev/null || true
	rm -rf build/ 2>/dev/null || true

optimize:
	@echo "⚡ Optimizing Delta tables..."
	python aws/scripts/optimize_delta_tables.py

dq-check:
	@echo "🔍 Running data quality checks..."
	python aws/scripts/run_ge_checks.py

# Monitoring targets
status:
	@echo "📊 Checking pipeline status..."
	@echo "EMR Application ID: $${EMR_APP_ID:-Not set}"
	@echo "S3 Lake Bucket: $${S3_LAKE_BUCKET:-Not set}"
	@echo "Glue Silver DB: $${GLUE_DB_SILVER:-Not set}"
	@echo "Glue Gold DB: $${GLUE_DB_GOLD:-Not set}"
	@echo ""
	@echo "Recent EMR jobs:"
	aws emr-serverless list-job-runs --application-id $${EMR_APP_ID} --max-items 5 2>/dev/null || echo "EMR_APP_ID not set"

logs:
	@echo "📋 Recent logs:"
	aws logs describe-log-groups --log-group-name-prefix /aws/emr-serverless --max-items 5

metrics:
	@echo "📈 Performance metrics:"
	@echo "Data volumes:"
	aws s3 ls s3://$${S3_LAKE_BUCKET}/bronze/ --recursive --summarize | tail -1
	aws s3 ls s3://$${S3_LAKE_BUCKET}/silver/ --recursive --summarize | tail -1
	aws s3 ls s3://$${S3_LAKE_BUCKET}/gold/ --recursive --summarize | tail -1

# Setup targets
setup-local:
	@echo "🛠️ Setting up local development environment..."
	pip install -r requirements.txt
	cp .env.example .env
	@echo "✅ Setup complete. Edit .env with your configuration."

setup-aws:
	@echo "☁️ Setting up AWS environment..."
	source aws/scripts/source_terraform_outputs.sh
	@echo "✅ AWS environment configured."

# Validation targets
validate-dags:
	@echo "🌪️ Validating Airflow DAGs..."
	pytest tests/test_dag_import.py -v

validate-config:
	@echo "⚙️ Validating configuration files..."
	python -c "import yaml; [yaml.safe_load(open(f)) for f in ['config/aws.yaml', 'config/default.yaml', 'config/local.yaml']]"
	@echo "✅ All configuration files are valid YAML"

validate-data:
	@echo "📊 Validating data files..."
	python -c "import pandas as pd; [pd.read_csv(f, nrows=5) for f in ['aws/data_fixed/hubspot_contacts_25000.csv', 'aws/data_fixed/snowflake_customers_50000.csv']]"
	@echo "✅ Data files are valid"

# Smoke test
smoke-test: validate-config validate-data validate-dags test
	@echo "🚀 Smoke test completed successfully!"
	@echo "✅ Configuration files valid"
	@echo "✅ Data files accessible"
	@echo "✅ DAGs import correctly"
	@echo "✅ Tests pass"
	@echo ""
	@echo "🎯 Pipeline is ready for deployment!"

# Golden path (complete deployment)
golden-path: clean format lint test deploy sync-dags dq-check
	@echo "🏆 Golden path completed successfully!"
	@echo "✅ Code formatted and linted"
	@echo "✅ Tests passed"
	@echo "✅ Deployed to AWS"
	@echo "✅ DAGs synchronized"
	@echo "✅ Data quality checks passed"
	@echo ""
	@echo "🎉 Production pipeline is live!"