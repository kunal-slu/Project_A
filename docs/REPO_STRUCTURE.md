# Repository Structure

## Top-Level Layout

- `src/project_a/`: Core Python package (shared logic, utilities, contracts).
- `jobs/`: Local/runtime job entry points and orchestration.
- `aws/`: AWS-specific code (DAGs, cloud jobs, Terraform, AWS tests).
- `local/`: Local execution scripts and helpers.
- `config/`: Shared/local configuration and schema contracts.
- `tests/`: Local/core test suite.
- `docs/`: Documentation, guides, runbooks, reports.
- `scripts/`: Operational helper scripts.
- `artifacts/`: Generated analysis artifacts.

## Execution Boundaries

- Local execution:
  - Jobs: `jobs/`
  - Tests: `tests/`
  - Config: `config/` and `local/config/`
- AWS execution:
  - Jobs and DAGs: `aws/jobs/`, `aws/dags/`
  - Tests: `aws/tests/`
  - Config: `aws/config/`

## Notes

- Root-level markdown/txt report artifacts were moved to `docs/reports/`.
- Runtime output files such as `PIPELINE_RESULTS.*` remain at repo root to preserve existing scripts.
