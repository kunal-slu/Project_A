.PHONY: help lint lint-fix test test-integration test-contracts test-dags package deploy-dev clean

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

lint: ## Run all linting checks
	@echo "🔍 Running linting checks..."
	ruff check src jobs aws/dags
	ruff format --check src jobs aws/dags
	mypy src/pyspark_interview_project || true
	@echo "✅ Linting complete"

lint-fix: ## Auto-fix linting issues
	@echo "🔧 Fixing linting issues..."
	ruff check --fix src jobs aws/dags
	ruff format src jobs aws/dags
	@echo "✅ Linting fixes applied"

test: ## Run all unit tests
	@echo "🧪 Running unit tests..."
	pytest -q tests/ --ignore=tests/integration
	@echo "✅ Unit tests complete"

test-integration: ## Run integration tests
	@echo "🧪 Running integration tests..."
	pytest -q tests/integration/
	@echo "✅ Integration tests complete"

test-contracts: ## Run contract validation tests
	@echo "📋 Running contract tests..."
	pytest -q tests/test_contracts_all_sources.py
	@echo "✅ Contract tests complete"

test-dags: ## Test Airflow DAG imports
	@echo "🪶 Testing Airflow DAGs..."
	pytest -q tests/test_dags_import.py
	@echo "✅ DAG tests complete"

test-all: test test-integration test-contracts test-dags ## Run all tests

package: ## Build Python wheel package
	@echo "📦 Building package..."
	python -m build
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
