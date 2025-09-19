.PHONY: venv run-local test up down aws clean help

# Default target
help:
	@echo "Available targets:"
	@echo "  venv      - Create virtual environment and install dependencies"
	@echo "  run-local - Run local ETL pipeline"
	@echo "  test      - Run tests"
	@echo "  up        - Start Docker services (MinIO + Spark)"
	@echo "  down      - Stop Docker services"
	@echo "  aws       - Run AWS EMR Serverless job"
	@echo "  clean     - Clean up generated files and directories"

venv:
	python -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

run-local:
	. .venv/bin/activate && python scripts/run_local_etl.py --conf conf/application-local.yaml

test:
	. .venv/bin/activate && pytest -q

up:
	cd docker && docker compose up -d --build

down:
	cd docker && docker compose down -v

aws:
	python scripts/run_aws_emr_serverless.py --conf conf/application-aws.yaml

clean:
	rm -rf data/lake .pytest_cache .venv __pycache__ .mypy_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true