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
  - portal/backend/service/bots/botlens_canonical_facts.py
  - portal/backend/service/bots/botlens_event_replay.py
  - portal/backend/service/bots/botlens_symbol_service.py
  - portal/backend/service/bots/botlens_chart_service.py
  - portal/backend/service/bots/botlens_forensics_service.py
  - portal/backend/service/bots/botlens_retrieval_queries.py
  - portal/backend/main.py
  - portal/backend/service/bots/container_runtime.py
  - src/engines/bot_runtime/runtime/components/canonical_facts.py
  - scripts/db/manual_migration_botlens_observability_persistence_v1.sql
  - scripts/db/manual_migration_botlens_runtime_event_storage_efficiency_v2.sql
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

- keeping BotLens truth in the runtime ledger under `botlens_domain.*` only,
- moving canonical BotLens fact capture for committed producer-owned fact batches
  to the producer/runtime side before websocket fanout, including compact domain
  rows for candles, overlays, series stats, signals, decisions, trades,
  diagnostics, and runtime health,
- treating websocket intake as a derived/projector seam rather than a truth-capture seam for those canonical families,
- stopping the old `typed_delta_metrics` drift,
- replacing the old `SELECT event_id -> INSERT` hot-path contract with `seq` guard prechecks plus `INSERT .. ON CONFLICT DO NOTHING` on `event_id`,
- fieldizing hot runtime-event dimensions on `portal_bot_run_events` (`event_name`, `series_key`, `correlation_id`, `root_id`, `bar_time`, `instrument_id`, `symbol`, `timeframe`, `signal_id`, `decision_id`, `trade_id`, `reason_code`),
- retiring payload extraction as a hot-read compatibility bridge for those dimensions so typed columns are the only canonical query surface,
- and pushing true backend observability into dedicated observability tables instead of back into runtime payload JSON.

`runtime.*` and `series_bar.*` rows are not part of durable BotLens truth in this phase. Report-system rebuilding that previously depended on those shapes is intentionally deferred.

`portal_bot_run_steps` is still a profiling/step-trace path and is explicitly classified as temporary legacy observability storage rather than runtime truth.

BotLens live read-path rules now also require:

- chart retrieval to stay range-based and query durable `botlens_domain.candle_observed` truth rather than projector memory or transport snapshots,
- fleet/bot request reads to consume cached projector/read-model state only instead of replaying the ledger on demand,
- forensic reads to page through `portal_bot_run_events` with a stable `(seq, row_id)` cursor instead of assuming `seq` uniqueness,
- forensic filters to be applied to the filtered result stream before pagination slicing so filtered pages, cursor advancement, and exhaustion semantics are defined by returned matches rather than scanned rows,
- signal forensics to read `SIGNAL_EMITTED` / `DECISION_EMITTED` domain rows directly,
- no reconstruction fallback back to durable `runtime.*` rows for the in-scope BotLens chart/forensic contracts,
- no aliasing of `signal_id` to `decision_id` or `decision_id` to `signal_id` while reconstructing persisted BotLens signal forensics,
- and no projection of fields that the domain contract does not actually supply.

BotLens domain-row invariants now also require:

- canonical `series_key` on `CANDLE_OBSERVED`, `SIGNAL_EMITTED`, `DECISION_EMITTED`, `TRADE_OPENED`, `TRADE_UPDATED`, and `TRADE_CLOSED`, with in-scope BotLens reads rejecting persisted rows that omit it,
- `DECISION_EMITTED.context.decision_state` to be the closed enum `accepted | rejected`,
- rejected decisions to include both `reason_code` and `message` across construction, persisted-row decode, projection, and detail reads,
- `HEALTH_STATUS_REPORTED.context.trigger_event` as the persisted derived name for the inner runtime/lifecycle cause, with no legacy `context.event` tolerance on BotLens reads,
- `SIGNAL_EMITTED.context.signal_id` and `DECISION_EMITTED.context.decision_id` to remain distinct contract identities, with equality rejected at construction and persistence boundaries,
- trade-close truth to arrive only through explicit closing `TRADE_CLOSED` facts carrying `closed_at`, with no projection fallback that infers close state from a status string,
- completed run projections to fail if `open_trades` still contains entries, making missing close facts visible instead of silently correcting state,
- malformed candle OHLC payloads to fail at the domain boundary with explicit stable field-level validation errors for missing, non-numeric, NaN, and infinite values,
- `CANDLE_OBSERVED.context.candle` durable rows to carry OHLCV truth only rather than repeated wrapper analytics,
- `SERIES_STATS_REPORTED.context.stats` durable rows to carry compact top-level summary metrics only rather than repeated bulky stats maps,
- `HEALTH_STATUS_REPORTED.context.warnings` to persist bounded derived warning rows without nested `context` blobs, alongside `warning_types` and `highest_warning_severity`,
- and `OVERLAY_STATE_CHANGED.context.overlay_delta` to persist bounded renderable overlay payloads (`detail_level=bounded_render`, `payload`, `overlay_revision`, `payload_summary`, point/op counts) so ledger recovery can redraw overlays when bridge delivery lags.

This means payload-size instrumentation on `portal_bot_run_events` now measures the bounded durable BotLens contract after normalization, not the pre-normalization bridge payload. Before/after comparisons still use the same metrics (`payload_bytes`, `bytes_per_row`, `payload_size_bucket`, `db_write_ms`), but the write path now records the bounded domain shape as the hot payload.

## Runtime-event Hot Table Contract

`portal_bot_run_events` is now treated as a typed hot table backed by the durable payload body.

Write contract:

- `event_id` remains the authoritative idempotency key,
- the hot write path no longer prechecks `event_id` with a standalone `SELECT`,
- writers still precheck `seq` only to preserve the existing logical replay guard,
- inserts use `ON CONFLICT ON CONSTRAINT uq_portal_bot_run_events_event_id DO NOTHING`,
- duplicate observability is derived from the conflict outcome and still reports `already_persisted_same_event_id`,
- and the narrow `event_id` lookup remains only on rare paths where seq-collision handling must distinguish a true collision from an already-persisted duplicate.

Read contract:

- selected-symbol, chart/history, replay, and forensic reads use the typed columns as the primary predicates,
- filtered BotLens reads do not fall back to payload JSON when a typed hot column is `NULL`,
- normal ledger reads do not eagerly deserialize BotLens payloads into current typed models unless the caller explicitly asks for that strict boundary,
- current-schema canonicalization of historical BotLens payloads belongs to replay/diagnostics boundaries rather than standard request-path reads,
- chart retrieval now forwards typed `bar_time` windows into the runtime-event query path instead of scanning full symbol history before filtering,
- signal forensics seed from typed `signal_id`, then narrow related scans through typed `root_id` / `correlation_id`,
- and typed columns are the index surface.

Current immediate index posture:

- added now: ordered read index on `(bot_id, run_id, seq, id)` because every paged runtime-event traversal already uses that shape,
- added now: symbol-scoped replay/read index on `(bot_id, run_id, series_key, seq, id)` for selected-symbol rebuilds and series-scoped history traversals,
- added now: partial candle-window index on `(bot_id, run_id, series_key, bar_time, seq, id) WHERE event_name = 'CANDLE_OBSERVED' AND series_key IS NOT NULL AND bar_time IS NOT NULL` for chart/history window reads,
- added now: event-name ordered index on `(bot_id, run_id, event_name, seq, id)` for lifecycle/domain-family reads that already filter by typed `event_name`,
- added now: partial correlation index on `(bot_id, run_id, correlation_id, seq, id) WHERE correlation_id IS NOT NULL` for causal-chain lookups,
- added now: partial root index on `(bot_id, run_id, root_id, seq, id) WHERE root_id IS NOT NULL` for event-family/root traversal,
- added now: partial bar-time index on `(bot_id, run_id, bar_time, seq, id) WHERE bar_time IS NOT NULL` for typed time-window reads that are not series-scoped,
- removed now: the legacy payload expression index on `payload ->> 'series_key'` because `series_key` is no longer queried through JSON.

The required runtime-event index names are owned by `REQUIRED_BOT_RUN_EVENT_INDEXES` in `portal/backend/db/models.py`.
Startup schema readiness checks do not mutate the table; they warn with
`portal_db_required_indexes_missing` and point operators to
`scripts/db/manual_migration_botlens_runtime_event_storage_efficiency_v2.sql`.
Missing required indexes mean the live database is not index-ready even if the
ORM model and manual migration are correct.

Write batching posture:

- canonical path now: runtime workers append compact BotLens domain rows before transport, using the committed producer-side `run_seq` as the replay boundary,
- derived path now: backend intake coalesces same-context derived BotLens rows up to `128` rows or `10ms` before issuing one DB batch,
- derived batching still preserves persistence-before-projection ordering for each routed intake payload that remains on the backend side,
- and storage observability remains unchanged (`db_write_ms`, `db_write_round_trip_ms`, duplicate accounting, and upstream `telemetry_queue_wait_ms`).

Active-run recovery posture:

- the projector registry tails committed series-scoped BotLens domain rows after
  a run reaches live, using `(seq, row_id)` as the durable cursor,
- the tailer feeds the same symbol projectors as bridge intake and relies on
  stable `event_id` dedupe to avoid double application,
- and this recovery feed is allowed to backpressure on the backend symbol
  mailbox instead of dropping committed progress silently.

Deferred cleanup:

- `observability_events.*` and `observability_metrics.*` candidate columns such as `delta_type`, `instrument_id`, `run_seq`, `bridge_session_id`, `message`, `phase`, and `status` stay in place because current exporter/runtime code still populates them,
- `event_time` stays because it is the cross-family ledger timestamp used by existing lifecycle/consistency reads,
- and `event_type` stays because it routes the durable event family while `event_name` is the business/domain name inside that family.

## Migration Boundary

The owner still performs manual DDL/cutover work.

`scripts/db/manual_migration_botlens_observability_persistence_v1.sql` plus `scripts/db/manual_migration_botlens_runtime_event_storage_efficiency_v2.sql` are the manual DDL artifacts for:

- schema creation,
- durable observability tables,
- typed-column additions on `portal_bot_run_events`,
- runtime-state views over the surviving ledger and lifecycle tables,
- the ordered run-scan index plus the new symbol/candle hot indexes,
- and retirement of the legacy payload `series_key` expression index.

Apply `manual_migration_botlens_runtime_event_storage_efficiency_v2.sql` before
closure of a live BotLens deployment. The backend will not create or alter
missing hot indexes at runtime.

Old-row support boundary:

- unfiltered ledger traversal can still deserialize the durable payload body itself,
- but hot filtered reads now assume the typed hot columns were written at ingest time,
- so historical rows that were never backfilled into those typed columns are out of contract for series-keyed/chart/forensic hot paths,
- and the intended remediation is one-time backfill or acceptance that those old rows are no longer served by the optimized paths.

The detailed manual sequencing and cutover checklist lives in `BOTLENS_OBSERVABILITY_MIGRATION_CHECKLIST.md`.
