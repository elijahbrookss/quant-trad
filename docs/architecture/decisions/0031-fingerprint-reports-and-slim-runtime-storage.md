---
component: adr-fingerprint-reports-and-slim-runtime-storage
subsystem: reporting
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - reporting
  - persistence
  - observability
  - schema
code_paths:
  - portal/backend/db/models.py
  - portal/backend/db/session.py
  - portal/backend/service/storage/repos/report_materializations.py
  - portal/backend/service/storage/repos/runs.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/reports/materialization.py
  - portal/backend/service/reports/report_data.py
  - portal/backend/service/reports/run_research_dataset.py
  - portal/backend/service/async_jobs/repository.py
  - src/engines/bot_runtime/runtime/components/step_trace_rollup.py
  - scripts/db/manual_migration_report_provenance_and_materialization_fingerprint_v1.sql
---
# ADR 0031: Fingerprint Reports And Slim Runtime Storage

## Status

Accepted on 2026-06-01.

## Context

Recent run forensics showed that several derived/debug surfaces were still
heavier than their contract required:

- report materialization cache validity used contract/schema versions but not
  a durable input fingerprint,
- `portal_bot_runs` still carried legacy decision-ledger storage and lacked
  explicit lean provenance columns,
- step rollups had become a repeated debug metric sink instead of a compact
  phase-duration profiler,
- QuantLab async job rows could behave like reusable result cache rows.

These surfaces are useful, but none of them should become a second source of
runtime truth.

## Decision

Report materializations are valid only for the recorded input fingerprint. The
fingerprint includes run metadata, run update boundary, runtime-event count and
high-water mark, trade count, config/provenance hashes, and summary hash. Ready
artifacts with missing or changed fingerprints are stale.

`portal_bot_runs` keeps run identity, lifecycle status, bounded config snapshot,
summary, and explicit provenance/hash columns. It no longer stores
`decision_ledger`; report decisions come from BotLens-domain decision events in
the runtime event ledger.

`portal_bot_run_step_rollups_v1` is duration-only profiler storage. Queue
pressure, persistence lag, payload sizes, worker health, and internal debug
counters belong to bounded observability rollups.

QuantLab async jobs may carry a fresh worker result long enough for the waiting
API request to return, but succeeded jobs are not reusable result-cache truth.
Old finished results are pruned to bounded summaries.

## Consequences

- Report caches cannot silently serve stale artifacts after durable run inputs
  change.
- Run rows stay leaner and use typed provenance columns for hot/report identity.
- Decision reporting has one source: domain decision events in the runtime
  event ledger.
- Step-rollup write volume and table growth drop substantially.
- Queue/projector pressure remains visible in observability without multiplying
  per-step profiler rows.
- Existing databases need the matching schema/data migration before deploying
  the code.

## References

- [Reporting Boundary](../reporting/REPORTING_BOUNDARY.md)
- [Persistence Boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Observability Boundary](../observability/OBSERVABILITY_BOUNDARY.md)
