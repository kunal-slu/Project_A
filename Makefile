.PHONY: help lint lint-fix lint-local lint-aws test test-local test-aws test-integration test-contracts test-dags test-runtime-contracts dbt-test package deploy-dev clean

PYTHON ?= .venv/bin/python
RUFF ?= .venv/bin/ruff
PYTEST ?= $(PYTHON) -m pytest
MYPY ?= .venv/bin/mypy

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

lint: lint-local lint-aws ## Run local + AWS linting checks
	@echo "✅ Linting complete"

lint-local: ## Run lint checks for local/runtime code
	@echo "🔍 Running linting checks..."
	$(RUFF) check src jobs tests
	$(RUFF) format --check src jobs tests
	@echo "✅ Local lint complete"

lint-aws: ## Run lint checks for AWS/cloud code
	@echo "☁️ Running AWS linting checks..."
	$(RUFF) check aws
	$(RUFF) format --check aws
	@echo "✅ AWS lint complete"

lint-fix: ## Auto-fix linting issues
	@echo "🔧 Fixing linting issues..."
	ruff check --fix src jobs aws/dags
	ruff format src jobs aws/dags
	@echo "✅ Linting fixes applied"

test: ## Run all unit tests
	@echo "🧪 Running local unit tests..."
	$(PYTEST) -q tests/
	@echo "✅ Local unit tests complete"

test-local: ## Run local unit tests only
	@echo "🧪 Running unit tests..."
	$(PYTEST) -q tests/ --ignore=tests/integration
	@echo "✅ Local unit tests complete"

test-aws: ## Run AWS/cloud tests only
	@echo "☁️ Running AWS tests..."
	$(PYTEST) -q aws/tests/
	@echo "✅ AWS tests complete"

test-integration: ## Run integration tests
	@echo "🧪 Running integration tests..."
	@if [ -d tests/integration ]; then \
		$(PYTEST) -q tests/integration/; \
	else \
		echo "ℹ️  No tests/integration directory found; skipping integration tests"; \
	fi
	@echo "✅ Integration tests complete"

test-contracts: ## Run contract validation tests
	@echo "📋 Running contract tests..."
	@if [ -f tests/test_contracts_all_sources.py ]; then \
		$(PYTEST) -q tests/test_contracts_all_sources.py; \
	else \
		echo "ℹ️  tests/test_contracts_all_sources.py not found; skipping contract aggregate test"; \
	fi
	@echo "✅ Contract tests complete"

test-runtime-contracts: ## Run runtime fail-fast contract tests
	@echo "📋 Running runtime contract tests..."
	$(PYTEST) -q tests/test_runtime_contracts.py
	@echo "✅ Runtime contract tests complete"

dbt-test: ## Run dbt contract/data tests (fails pipeline on test failures)
	@echo "🧪 Running dbt tests..."
	@command -v dbt >/dev/null 2>&1 || (echo "❌ dbt is not installed"; exit 1)
	dbt test --project-dir dbt
	@echo "✅ dbt tests complete"

test-dags: ## Test Airflow DAG imports
	@echo "🪶 Testing Airflow DAGs..."
	$(PYTEST) -q tests/test_dag_imports.py
	@echo "✅ DAG tests complete"

test-all: test-local test-aws test-integration test-contracts test-dags ## Run full test matrix

package: ## Build Python wheel package
	@echo "📦 Building package..."
	$(PYTHON) -m build
	@echo "✅ Package built: dist/project_a-*.whl"

terraform-fmt: ## Format Terraform files
	@echo "🔧 Formatting Terraform..."
	cd aws/terraform && terraform fmt -recursive
	@echo "✅ Terraform formatted"

terraform-validate: ## Validate Terraform configuration
	@echo "✅ Validating Terraform..."
	cd aws/terraform && terraform init && terraform validate
	@echo "✅ Terraform validation complete"

terraform-plan: ## Show Terraform plan
	@echo "📋 Generating Terraform plan..."
	cd aws/terraform && terraform plan -var-file=env/dev.tfvars
	@echo "✅ Terraform plan complete"

deploy-dev: terraform-validate ## Deploy to dev environment
	@echo "🚀 Deploying to dev..."
	cd aws/terraform && terraform apply -var-file=env/dev.tfvars
	@echo "✅ Deployment complete"

clean: ## Clean build artifacts
	@echo "🧹 Cleaning..."
	rm -rf dist/ build/ *.egg-info/
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "✅ Clean complete"

all: lint test-all package ## Run lint, tests, and build package
