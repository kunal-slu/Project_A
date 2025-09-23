.PHONY: venv run-local run-bronze run-silver run-gold test up down aws clean help infra-init infra-plan infra-apply infra-destroy dq-run dq-docs dist optimize-vacuum lf-setup kafka-produce

# Default target
help:
	@echo "Available targets:"
	@echo "  venv         - Create virtual environment and install dependencies"
	@echo "  run-local    - Run local ETL pipeline"
	@echo "  run-bronze   - Run bronze layer processing"
	@echo "  run-silver   - Run silver layer processing"
	@echo "  run-gold     - Run gold layer processing"
	@echo "  test         - Run tests"
	@echo "  up           - Start Docker services (MinIO + Spark)"
	@echo "  down         - Stop Docker services"
	@echo "  aws          - Run AWS EMR Serverless job"
	@echo "  infra-init   - Initialize Terraform"
	@echo "  infra-plan   - Plan Terraform changes"
	@echo "  infra-apply  - Apply Terraform changes"
	@echo "  infra-destroy- Destroy Terraform infrastructure"
	@echo "  dq-run       - Run data quality checks"
	@echo "  dq-docs      - Generate data quality docs"
	@echo "  dist         - Build Python wheel distribution"
	@echo "  optimize-vacuum - Optimize and vacuum Delta tables"
	@echo "  lf-setup     - Setup Lake Formation tags and policies"
	@echo "  kafka-produce - Produce sample events to Kafka"
	@echo "  clean        - Clean up generated files and directories"

venv:
	python -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

run-local:
	. .venv/bin/activate && python scripts/run_local_etl.py --conf conf/application-local.yaml

run-bronze:
	. .venv/bin/activate && PYTHONPATH=src python -m pyspark_interview_project.pipeline.bronze_to_silver --config conf/application-local.yaml

run-silver:
	. .venv/bin/activate && PYTHONPATH=src python -m pyspark_interview_project.pipeline.silver_to_gold --config conf/application-local.yaml

run-gold:
	. .venv/bin/activate && PYTHONPATH=src python -m pyspark_interview_project.pipeline.silver_to_gold --config conf/application-local.yaml

test:
	. .venv/bin/activate && pytest -q

up:
	cd docker && docker compose up -d --build

down:
	cd docker && docker compose down -v

aws:
	python scripts/run_aws_emr_serverless.py --conf conf/application-aws.yaml

# Infrastructure targets
infra-init:
	@echo "Initializing Terraform..."
	@cd infra/terraform && terraform init

infra-plan:
	@echo "Planning Terraform changes..."
	@cd infra/terraform && terraform plan

infra-apply:
	@echo "Applying Terraform changes..."
	@cd infra/terraform && terraform apply

infra-destroy:
	@echo "Destroying Terraform infrastructure..."
	@cd infra/terraform && terraform destroy

# Data quality targets
dq-run:
	@echo "Running data quality checks..."
	@. .venv/bin/activate && python aws/scripts/run_ge_checks.py --lake-root s3://pyspark-de-project-dev-data-lake --lake-bucket pyspark-de-project-dev-data-lake --suite dq/suites/silver_orders.yml --table orders --layer silver

dq-docs:
	@echo "Generating data quality docs..."
	@. .venv/bin/activate && python aws/scripts/run_ge_checks.py --lake-root s3://pyspark-de-project-dev-data-lake --lake-bucket pyspark-de-project-dev-data-lake --suite dq/suites/silver_orders.yml --table orders --layer silver

# Distribution target
dist:
	@echo "Building Python wheel distribution..."
	@. .venv/bin/activate && python -m build
	@echo "Wheel built in dist/ directory"

# Delta optimization target
optimize-vacuum:
	@echo "Optimizing and vacuuming Delta tables..."
	@. .venv/bin/activate && python scripts/delta_optimize_vacuum.py --lake-root s3://pyspark-de-project-dev-data-lake --layers silver gold

# Lake Formation setup target
lf-setup:
	@echo "Setting up Lake Formation tags and policies..."
	@. .venv/bin/activate && python aws/scripts/lf_tags_seed.py --database pyspark_de_project_silver

# Kafka producer target
kafka-produce:
	@echo "Producing sample events to Kafka..."
	@. .venv/bin/activate && python scripts/kafka_producer.py --bootstrap-servers ${KAFKA_BOOTSTRAP} --api-key ${KAFKA_API_KEY} --api-secret ${KAFKA_API_SECRET} --topic orders_events --num-orders 50

clean:
	rm -rf data/lake .pytest_cache .venv __pycache__ .mypy_cache dist build
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true