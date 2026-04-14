---
component: botlens-observability-persistence
subsystem: storage
layer: service
doc_type: architecture
status: active
tags:
  - storage
  - runtime
  - observability
  - botlens
  - grafana
code_paths:
  - portal/backend/service/observability.py
  - portal/backend/service/observability_exporter.py
  - portal/backend/db/models.py
  - portal/backend/db/session.py
  - portal/backend/service/storage/repos/observability.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/bots/botlens_symbol_service.py
  - portal/backend/main.py
  - portal/backend/service/bots/container_runtime.py
  - scripts/db/manual_migration_botlens_observability_persistence_v1.sql
---
# BotLens Observability Persistence Architecture

## Purpose

BotLens backend observability now has a durable DB-backed sink.

The in-memory observability substrate is still the single emission API for backend code, but it is no longer treated as the final system of record.

## Runtime Shape

The durable flow is:

`BackendObserver -> InMemoryObservabilitySink -> ObservabilityExporter -> observability_metrics.botlens_backend_metric_samples_v1 / observability_events.botlens_backend_events_v1`

Responsibilities:

- `observability.py` remains the single emission seam for metrics and structured events.
- `observability_exporter.py` owns bounded drain, retry, and batch persistence.
- `storage/repos/observability.py` owns normalized DB writes for observability rows.
- Grafana reads the durable Postgres tables directly.

This keeps hot-path instrumentation cheap while making observability durable enough for dashboards and incident review.

## Sink vs Durable Store

The in-memory sink is now explicitly:

- the process-local emission substrate,
- the contract test surface,
- and the short-lived staging buffer for the exporter.

It is not:

- the durable source of truth for dashboards,
- a replay ledger,
- or a substitute for Postgres-backed operational history.

The exporter drains the sink in batches on a timer and on demand. If the bounded export queue overflows, the exporter persists a synthetic overflow event so dropped export records are visible.

## Durable Query Surfaces

### `observability_metrics.botlens_backend_metric_samples_v1`

Append-only metric samples with typed operational dimensions:

- `observed_at`
- `component`
- `metric_name`
- `metric_kind`
- `value`
- `bot_id`
- `run_id`
- `instrument_id`
- `series_key`
- `queue_name`
- `pipeline_stage`
- `message_kind`
- `storage_target`
- `failure_mode`
- `labels`

### `observability_events.botlens_backend_events_v1`

Append-only structured backend events with typed operational dimensions:

- `observed_at`
- `component`
- `event_name`
- `level`
- `bot_id`
- `run_id`
- `instrument_id`
- `series_key`
- `queue_name`
- `pipeline_stage`
- `message_kind`
- `storage_target`
- `failure_mode`
- `phase`
- `status`
- `run_seq`
- `bridge_session_id`
- `bridge_seq`
- `message`
- `details`

These are stable dashboard-facing tables. Version suffixes allow future evolution without silently breaking Grafana queries.

## Schema Ownership

Target ownership is explicit even where physical table moves remain manual:

- `public`: bot/domain truth such as `portal_bots` and `portal_bot_runs`
- `runtime_state`: replay-critical read models and lifecycle truth
- `observability_events`: structured operational anomalies/transitions
- `observability_metrics`: metric samples and dashboard-friendly measurements

Current code does not reinterpret runtime replay tables as observability storage.

## Runtime Ledger Cleanup

`portal_bot_run_events` remains the runtime/replay ledger.

This pass cleans the application model by:

- keeping BotLens replay facts in the runtime ledger,
- stopping the old `typed_delta_metrics` drift,
- projecting common routing fields like `series_key`, `bridge_session_id`, `bridge_seq`, and `run_seq` out of payloads for app consumers,
- and pushing true backend observability into dedicated observability tables instead of back into runtime payload JSON.

`portal_bot_run_steps` is still a profiling/step-trace path and is explicitly classified as temporary legacy observability storage rather than runtime truth.

## Migration Boundary

The owner still performs manual DDL/cutover work.

`scripts/db/manual_migration_botlens_observability_persistence_v1.sql` is the authoritative DDL artifact for:

- schema creation,
- durable observability tables,
- compatibility/runtime-state views,
- and functional indexes for typed JSON extraction during the migration window.

The detailed manual sequencing and cutover checklist lives in `BOTLENS_OBSERVABILITY_MIGRATION_CHECKLIST.md`.
