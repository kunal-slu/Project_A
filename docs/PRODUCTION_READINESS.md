## Production Readiness Checklist

- Packaging
  - Package code under `src/` and publish wheel to artifacts
  - Thin notebooks under `notebooks/` call package only

- Orchestration
  - Databricks Job JSON in `jobs/` with job clusters and libraries
  - `ci_cd/deploy_jobs.sh` to create/update jobs via API
  - Cluster policy in `policies/databricks_cluster_policy.json` for guardrails

- CI/CD
  - `ci_cd/build_wheel.sh` to build wheels
  - GitHub Actions *azure* workflow triggers job per environment

- Config & Secrets
  - `config_loader.py` resolves `${ENV:VAR}` and `${SECRET:scope:key}`
  - Use Databricks secret scopes backed by Key Vault
  - Example secrets: `adls-client-id`, `adls-tenant-id`, `adls-client-secret`

- Data Access
  - Use `abfss://` paths with SPN-based OAuth; no mounts required
  - If mounting, restrict via cluster policies and use MSI/SCIM
  - Required Spark conf on clusters/jobs for ADLS OAuth

- Logging & Monitoring
  - Structured logs to Log Analytics or App Insights
  - Azure Monitor alert rules for failures, DQ violations, SLAs

- Governance
  - Unity Catalog for RBAC and table registration
  - Purview for lineage; monthly audits


