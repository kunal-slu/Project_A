# Scope and Limitations

This document clarifies project scope, simulation boundaries, and known weak points.

## What Is Implemented

- Bronze to Silver and Silver to Gold transform paths for local execution.
- Contract and schema validation utilities.
- AWS deployment artifacts (DAGs, Terraform, cloud job scripts).
- Local and AWS-focused test suites.

## What Is Simulated

- Some cloud integrations rely on mocked/sample datasets in local runs.
- Certain DAG/unit tests validate import/contract presence rather than full cloud execution.
- Legacy modules are preserved for compatibility, not as the primary runtime path.

## Known Limitations

- Formatting gate (`ruff format --check`) is not fully green across the entire repository.
- Some optional test lanes are conditional/skip when paths are absent.
- Legacy duplication increases navigation complexity.
- Full production-scale performance behavior is not proven in local execution mode.

## What Breaks First at Scale

- Schema drift and contract mismatch between producers and consumers.
- Silent row loss from bad upstream records if strict fail-fast mode is not enabled.
- Operational complexity from legacy/compatibility paths being invoked unintentionally.

## Why Current Risk Is Acceptable

- Core transforms now support strict fail-fast validation for primary keys and row-drop thresholds.
- Central schema registry reduces definition drift.
- Assertion-level tests verify deterministic transform behavior and end-to-end row/aggregation outcomes.

## Reviewer Context

- This project combines production-style structure with educational/simulation components.
- The active runtime path is intentionally small; legacy modules are retained to preserve backward compatibility.
