# Interview Scope (Canonical vs Legacy)

This repo contains **canonical production code** and **legacy compatibility** modules.  
For interviews and demos, focus on the canonical pipeline only.

## ✅ Canonical Pipeline (Interview‑Ready)

- `src/project_a/`  
- `jobs/` (non‑legacy jobs invoked by `project_a.pipeline.run_pipeline`)  
- `airflow/` (local Airflow orchestration)  
- `docs/interview/` (interview narrative and step‑by‑step guide)

## ⚠️ Legacy / Compatibility (Archived)

Legacy code has been **archived** to keep the repo interview‑clean:

- `archive/pyspark_interview_project/`  
- `archive/project_a_legacy/`  
- `archive/jobs_legacy/`

These folders are kept only for reference and are **not** used by the canonical pipeline.

## Why This Matters

Keeping the story focused prevents tool confusion during interviews.  
If asked, mention that legacy modules are kept for compatibility, but the production path uses `project_a` + Iceberg + Airflow.
