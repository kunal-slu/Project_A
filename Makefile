# Makefile - Developer convenience targets

.PHONY: help venv install dev test run-local dq-check airflow-check terraform-init terraform-validate terraform-plan catalog-register

help:
	@echo "Available targets:"
	@echo "  install            Install base dependencies"
	@echo "  dev                Install dev dependencies"
	@echo "  test               Run tests"
	@echo "  run-local          Run local end-to-end pipeline"
	@echo "  dq-check           Run data quality checks"
	@echo "  airflow-check      Validate Airflow DAG imports"
	@echo "  terraform-init     Terraform init in aws/terraform"
	@echo "  terraform-validate Terraform validate in aws/terraform"
	@echo "  terraform-plan     Terraform plan in aws/terraform"
	@echo "  catalog-register   Register Glue tables from Silver"

venv:
	python3 -m venv venv
	. venv/bin/activate && pip install -U pip

install:
	pip install -r requirements.txt

dev:
	pip install -r requirements-dev.txt || true

TEST_FLAGS ?= -q

test:
	pytest $(TEST_FLAGS)

export PYTHONPATH := $(PWD)/src:$(PYTHONPATH)

run-local:
	python scripts/local/run_pipeline.py --config config/dev.yaml --phase all

DQ_CONFIG ?= config/dev.yaml

dq-check:
	python aws/jobs/transform/dq_check_bronze.py || true
	python aws/jobs/transform/dq_check_silver.py || true

airflow-check:
	python - <<'PY'
import pkgutil
import sys
from pathlib import Path
base = Path('dags')
failed = False
for p in base.rglob('*.py'):
    if any(x in str(p) for x in ('development/archive', '__pycache__')):
        continue
    try:
        mod_path = 'dags.' + p.with_suffix('').as_posix().replace('/', '.')
        __import__(mod_path)
        print(f"OK  {p}")
    except Exception as e:
        print(f"FAIL {p}: {e}")
        failed = True
sys.exit(1 if failed else 0)
PY

terraform-init:
	cd aws/terraform && terraform init

terraform-validate:
	cd aws/terraform && terraform validate

terraform-plan:
	cd aws/terraform && terraform plan

GLUE_DB ?= silver
GLUE_ROOT ?= s3://bucket/silver

catalog-register:
	python aws/scripts/utilities/register_glue_tables.py --db $(GLUE_DB) --root $(GLUE_ROOT)

.PHONY: fmt lint type test unit it docs wheel run-simple run-local dq-check airflow-check terraform-init terraform-validate terraform-plan catalog-register

fmt:        ## Format code
	black src/ tests/ aws/jobs/ dags/
	isort src/ tests/ aws/jobs/ dags/
	ruff format src/ tests/ aws/jobs/ dags/

lint:       ## Lint code
	ruff check src/ tests/ aws/jobs/ dags/
	yamllint config/ aws/emr_configs/ || true
	python3 << 'PYEOF'
import json
import yaml
import jsonschema

with open('config/config.schema.json') as f:
    schema = json.load(f)

for config_file in ['config/prod.yaml', 'config/dev.yaml']:
    try:
        with open(config_file) as f:
            config = yaml.safe_load(f)
        jsonschema.validate(config, schema)
        print(f"✓ {config_file} is valid")
    except Exception as e:
        print(f"✗ {config_file} failed: {e}")
        exit(1)
PYEOF

type:       ## Type-check with mypy
	mypy src/ --ignore-missing-imports || true

test: unit  ## All tests

unit:       ## Unit tests
	pytest -q -m "not integration" --cov=src --cov-report=term-missing

it:         ## Integration tests
	pytest -q -m "integration" --cov=src --cov-report=term-missing

docs:       ## Generate documentation
	@echo "Documentation is in docs/ directory"

wheel:      ## Build wheel package
	python -m build --wheel
	@echo "Wheel built in dist/"

run-simple: ## Local CSV→Delta smoke test
	python scripts/local/run_pipeline.py --config config/local.yaml --phase bronze_silver

run-local:  ## Full local run
	python scripts/local/run_pipeline.py --config config/local.yaml --phase all

dq-check:   ## Run data quality checks
	python aws/jobs/transform/dq_check_bronze.py --config config/dev.yaml || true
	python aws/jobs/transform/dq_check_silver.py --config config/dev.yaml || true

airflow-check:  ## Validate Airflow DAG imports
	python - <<'PY'
import sys
from pathlib import Path
base = Path('dags')
failed = False
for p in base.rglob('*.py'):
    if '__pycache__' in str(p):
        continue
    try:
        mod_path = 'dags.' + p.with_suffix('').as_posix().replace('/', '.')
        __import__(mod_path)
        print(f"OK  {p}")
    except Exception as e:
        print(f"FAIL {p}: {e}")
        failed = True
sys.exit(1 if failed else 0)
PY

terraform-init:
	cd aws/terraform && terraform init

terraform-validate:
	cd aws/terraform && terraform validate

terraform-plan:
	cd aws/terraform && terraform plan

GLUE_DB ?= silver
GLUE_ROOT ?= s3://bucket/silver

catalog-register:
	python aws/scripts/utilities/register_glue_tables.py --db $(GLUE_DB) --root $(GLUE_ROOT)

help:       ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'