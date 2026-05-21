---
component: adr-run-research-dataset-reporting-contract
subsystem: reporting
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - reporting
  - research-dataset
  - comparison
  - export
code_paths:
  - portal/backend/service/reports/run_research_dataset.py
  - portal/backend/service/reports/contract.py
  - portal/backend/service/reports/export_bundle.py
  - portal/backend/service/reports/schemas.py
  - portal/backend/controller/reports.py
  - portal/frontend/src/components/reports
  - docs/architecture/reporting/REPORTING_BOUNDARY.md
---
# ADR 0010: Use RunResearchDataset V1 As The Reporting Contract

## Status

Accepted, backfilled on 2026-05-13.

## Context

Reports, compare views, downloads, frontend modals, and external analysis all
need run-level data. If each consumer shapes its own payload from raw tables or
BotLens snapshots, readiness and comparison rules drift.

Reporting is downstream of runtime truth. It should expose analysis-ready data
and diagnostics without mutating execution semantics or reconstructing hidden
indicator state.

## Decision

`RunResearchDataset v1` is the canonical reporting data product. Sectioned API
routes, summary payloads, comparison, and export bundles derive from it or from
the same reporting contract services.

Readiness is explicit and sectioned: dataset, results, comparison, export, data
quality, execution quality, golden candidate status, repeatability, blocking
reasons, and material fingerprint.

## Consequences

- Frontend report views and exports use typed report contracts instead of
  frontend-shaped legacy payloads.
- Comparison returns blocked results with reasons until compatibility and
  readiness checks pass.
- Golden-run certification is stricter than normal comparison readiness.
- Missing decision context, candle data, lifecycle truth, projection health, or
  wallet evidence becomes an explicit unavailable section or diagnostic.

## References

- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [Reporting contract redesign](../reporting/REPORTING_CONTRACT_REDESIGN.md)
- [Reporting datasets concept](../../concepts/reporting-datasets.md)

