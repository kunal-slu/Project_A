# Legacy Module – Deprecated

**Status: DEPRECATED.** This package is kept for reference only. No code outside `legacy/` should import from it.

- The active pipeline uses **`jobs/run_pipeline.py`** with **`project_a.core`** and repo-root **`jobs/transform/`** and **`jobs/ingest/`**.
- Config: use **`project_a.utils.config`** (`load_config`, `load_config_resolved`).
- Lineage: use **`project_a.lineage.tracking`**.
- Delta utilities: use **`project_a.delta_utils`**.

Planned: archive or remove this folder after confirming no external or CI usage.
