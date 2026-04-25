---
component: botlens-backend-observability-contract
subsystem: portal-runtime
layer: contract
doc_type: architecture
status: active
tags:
  - runtime
  - botlens
  - observability
  - telemetry
  - websocket
  - queues
code_paths:
  - src/core/candle_continuity.py
  - portal/backend/service/observability.py
  - portal/backend/service/observability_exporter.py
  - portal/backend/service/bots/container_runtime_telemetry.py
  - portal/backend/service/bots/botlens_runtime_state.py
  - portal/backend/service/bots/botlens_bootstrap_service.py
  - portal/backend/service/bots/botlens_symbol_service.py
  - portal/backend/service/bots/botlens_transport.py
  - portal/backend/service/bots/botlens_candle_continuity.py
  - portal/backend/controller/bots.py
  - portal/backend/service/bots/telemetry_stream.py
  - portal/backend/service/bots/botlens_intake_router.py
  - portal/backend/service/bots/botlens_mailbox.py
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/botlens_projector_registry.py
  - portal/backend/service/bots/botlens_run_stream.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/storage/repos/lifecycle.py
  - portal/backend/service/storage/repos/observability.py
---
# BotLens Backend Observability Contract

## Boundary

This contract covers the backend-only BotLens telemetry pipeline:

`ContainerRuntime telemetry -> TelemetryEmitter -> backend ingest websocket -> IntakeRouter -> mailboxes/slots -> SymbolProjector / RunProjector -> fanout channel -> fanout delivery loop -> BotLensRunStream -> viewer socket delivery`

The terminal seam is backend viewer socket delivery.

Out of scope:

- browser/client reducer timing,
- frontend diagnostics after backend send succeeds,
- browser logs,
- UI instrumentation.

## Contract Shape

Backend observability is emitted through one reusable substrate in `portal/backend/service/observability.py`.

The substrate provides:

- canonical context helpers,
- metric label filtering,
- first-class metric objects (`metric_name`, `metric_type`, `value`, `unit`, `timestamp`, `source`, `tags`),
- counter / histogram / gauge wrappers,
- interval-gated queue gauge helpers,
- payload size helpers,
- timed block utilities,
- structured event emission,
- normalization for event names and failure modes,
- separate in-memory metric and event sinks for tests and bounded exporter drain.

Metrics and structured events are separate on purpose:

- metrics describe flow, pressure, latency, and payload size,
- structured events describe transitions, anomalies, recoveries, and failures,
- backend logs only mirror warning/error events by default,
- INFO-level structured events stay in the observability sink unless a caller opts into logging them.

## Canonical Context

Stable operational context is attached whenever available:

- `bot_id`
- `run_id`
- `instrument_id`
- `series_key`
- `symbol`
- `timeframe`
- `component`
- `worker_id`

Allowed metric labels:

- `queue_name`
- `pipeline_stage`
- `message_kind`
- `delta_type`
- `storage_target`
- `failure_mode`
- `gap_type`

Rejected as metric labels:

- `viewer_id`
- `viewer_session_id`
- `event_id`
- `request_id`
- `trade_id`
- raw exception strings
- raw payload bodies

Those values may still appear in structured events when operationally useful.

## Signal Classes

### Counters / Histograms

These emit per occurrence:

- enqueue / send / drop / retry counts,
- queue wait time,
- transport / projector / fanout / snapshot / DB write latency,
- payload bytes,
- batch sizes and row counts,
- terminal lifecycle detection.

### Interval Gauges

These emit on an interval instead of on every mutation:

- queue depth,
- queue utilization,
- oldest pending age,
- bootstrap pending occupancy,
- active viewers,
- pending snapshot viewers,
- buffered snapshot delta count.

### Structured Events

These capture operational meaning:

- lifecycle transitions,
- runtime state transitions,
- degraded condition started / cleared,
- churn detection,
- supersede and drop-oldest behavior,
- queue overflow and backpressure state,
- transport loss/restoration,
- initial-state load success/failure,
- viewer send failures and snapshot-load failures,
- DB write failures, retries, slow writes, no-op skips, and seq collisions.

## Instrumented Seams

### Runtime -> emitter

- `telemetry_emitted_total`
- `telemetry_enqueue_attempt_total`
- `telemetry_enqueue_success_total`
- `telemetry_enqueue_drop_total`
- `telemetry_queue_depth`
- `telemetry_queue_utilization`
- `telemetry_queue_oldest_age_ms`
- `telemetry_queue_wait_ms`
- `telemetry_payload_bytes`
- `telemetry_duplicate_suppressed_total`
- events: `telemetry_backpressure_entered`, `telemetry_enqueue_timeout`, `telemetry_transport_recovered`

### Emitter -> backend ingest

- `telemetry_transport_send_total`
- `telemetry_transport_send_fail_total`
- `telemetry_transport_send_ms`
- `telemetry_transport_retries_total`
- `telemetry_transport_payload_bytes`
- events: `telemetry_transport_send_failed`, `telemetry_transport_retry_scheduled`, `telemetry_transport_connection_lost`, `telemetry_transport_connection_restored`

### Ingest -> router

- `ingest_messages_total`
- `ingest_messages_invalid_total`
- `ingest_messages_unknown_kind_total`
- `ingest_decode_ms`
- `ingest_route_ms`
- events: `intake_invalid_envelope`, `intake_missing_required_field`, `intake_unknown_kind`

### Router -> queues / slots

- symbol mailbox: `symbol_fact_*`
- bootstrap slot: `bootstrap_*`
- run mailbox lifecycle queue: `run_lifecycle_*`
- symbol -> run notification queue: `run_notification_*`
- raw symbol fact batches must not be enqueued onto the run lifecycle queue
- run lifecycle overflow is an invariant break and must fail loud instead of
  dropping oldest lifecycle truth
- events: rejected post-live bootstrap attempts must be surfaced as structured bootstrap-rejected events rather than silently dropped
- startup bootstrap admission uses one shared runtime-state inference rule: runtime health when present, lifecycle phase as fallback, and projection seq only as the last startup hint

### Projectors

- symbol projector: `symbol_projector_*`
- symbol projector emits compact run notifications after symbol projection and symbol-state persistence succeed
- run projector: `run_projector_*`
- run projector consumes lifecycle batches from `run_lifecycle_queue` and symbol-summary notifications from `run_notification_queue`
- runtime emitters must coalesce repeated health-condition churn before it
  enters transport; repeated warning counts alone must not produce a fresh
  health event every candle
- run health warnings are projected as canonical active conditions keyed by
  `warning_id`, with occurrence count and first/last-seen timestamps rather
  than raw repeated warning snapshots
- terminal lifecycle delivery must bypass normal telemetry-facts queueing so
  `completed` / `stopped` / `failed` / `crashed` truth is not queued behind
  symbol traffic
- lifecycle transition events normalized onto `run_phase_changed`, `run_terminal_detected`, and `run_evicted`
- run health projection now carries canonical runtime observability fields:
  - `runtime_state`
  - `progress_state`
  - `last_useful_progress_at`
  - `degraded`
  - `churn`
  - `pressure`
  - `recent_transitions`
  - `terminal`
- bootstrap scope reset is startup-only; post-live bootstrap batches must be rejected before projector state is reset
- bootstrap and large-fact dedupe are both scoped by `bridge_session_id`, so a new bridge session never loses its boundary because payload facts happened to match the previous session
- rebuild failures are explicit unavailable states, not valid empty projections:
  - run projector failure sets `health.status=projection_error`, readiness false, and a bounded projection fault,
  - symbol projector failure sets `snapshot_ready=false` and a `projection_error` diagnostic,
  - selected-symbol reads surface projection unavailability instead of returning an empty symbol snapshot.

### Candle Continuity Summaries

Continuity tracing stays inside the same BotLens observability substrate.

It is intentionally summary-only and seam-oriented:

- source/runtime fact intake:
  - `botlens_source_bootstrap`
  - `botlens_source_facts`
- ingest admission into `portal_bot_run_events`:
  - `botlens_ingest_admission`
- selected-symbol snapshot shaping:
  - `botlens_run_bootstrap_snapshot`
  - `botlens_selected_symbol_snapshot`
- final per-run/per-series audit:
  - `botlens_run_final`

Metrics:

- `candle_continuity_candle_count`
- `candle_continuity_gap_count`
- `candle_continuity_defect_gap_count`
- `candle_continuity_gap_count_by_type` with bounded `gap_type`
- `candle_continuity_missing_candle_estimate`
- `candle_continuity_max_gap_multiple`
- `candle_continuity_ratio`

Structured event:

- `candle_continuity_summary`

Required summary fields:

- `boundary_name`
- `candle_count`
- `first_ts`
- `last_ts`
- `expected_interval_seconds`
- `detected_gap_count`
- `defect_gap_count`
- `missing_candle_estimate`
- `largest_gap_seconds`
- `max_gap_seconds`
- `max_gap_multiple`
- `continuity_ratio` when available
- `duplicate_count`
- `out_of_order_count`
- `missing_ohlcv_count`
- `gap_count_by_type`
- `final_status`

Rules:

- continuity summaries are keyed by the normal BotLens operational dimensions (`run_id`, `series_key`, `pipeline_stage`, `message_kind`, optional `storage_target`),
- `source_reason` stays as structured context/detail, not a new top-level metric taxonomy,
- every gap is classified as `expected_session_gap`, `provider_missing_data`, `ingestion_failure`, or `unknown_gap`,
- classification is conservative; without explicit session/provider/ingestion evidence, the gap remains `unknown_gap`,
- expected session gaps are sparse truth and do not contribute to `defect_gap_count`,
- provider, ingestion, and unknown gaps remain defects/investigation items,
- the intake router keeps a lightweight run/series accumulator and emits one final `botlens_run_final` summary when terminal lifecycle truth arrives,
- the goal is boundary comparison on the next run, not per-candle tracing or dashboard sprawl.

### Fanout / delivery / run stream

- fanout queue: `fanout_*`
- delivery loop: `fanout_delivery_*`
- viewer socket delivery: `viewer_send_total`, `viewer_send_fail_total`, `viewer_send_ms`, `viewer_broadcast_*`, `viewer_payload_bytes`
- snapshot hydration: `viewer_snapshot_load_ms`, `viewer_snapshot_total_ms`, `snapshot_replay_count`
- replay ring state: `replay_ring_occupancy`, `replay_ring_utilization`, `replay_ring_high_water_mark`, `replay_message_count`, `reset_required_total`
- replay ring gauges must be aggregated as max/high-water or latest/current occupancy, never summed across runs/viewers
- `reset_required_total` is a counter and may be summed by reason; replay gap size/count metrics may be summed only when the panel title says it is counting replayed messages
- `viewer_payload_bytes` must use bounded `message_kind` values such as bootstrap, replay/delta type, heartbeat, selected-symbol snapshot, connected, or reset-required; unknown means a new transport type needs classification.

### Fleet / bot read contract

- Bot/fleet request paths are read-only over already-available projector state.
- `GET /api/bots`, `GET /api/bots/{id}`, and `GET /api/bots/stream` must not trigger ledger replay or projector bootstrap as a fallback.
- When no run snapshot is available, those surfaces return lifecycle/container truth plus explicit telemetry unavailability:
  - `available=false`
  - `reason=snapshot_unavailable` when a run exists but no snapshot is loaded
  - `reason=no_active_run` when no runtime is attached
- Snapshot-derived fields (`seq`, snapshot timestamps, warning counts, symbol/trade counts, engine worker counts) remain nullable/unavailable until projector state exists.

### Persistence

- `db_write_total`
- `db_write_attempt_total`
- `db_write_attempted_rows_total`
- `db_write_fail_total`
- `db_write_ms`
- `db_write_rows`
- `db_write_rows_total`
- `db_write_payload_bytes`
- `db_write_bytes_per_row`
- `db_write_payload_build_ms`
- `db_write_round_trip_ms`
- `db_write_duplicate_rows_total`
- `db_write_retry_total`
- `db_duplicate_skip_total`
- `db_stale_update_total`
- `persistence_wait_ms`
- `db_initial_load_ms`
- events: `db_write_observed`, `db_write_failed`, `db_write_retried`, `db_write_slow`, `db_seq_collision`, `db_initial_state_load_completed`, `db_initial_state_load_failed`

Persistence-attempt rows now carry one formal explainability contract at the write boundary.
The stable batch fields are:

- identity: `run_id`, `bot_id`, `series_key`, `worker_id`, `storage_target`, `message_kind`, optional `event_name`
- source: `source_emitter`, `source_reason`
- conflict semantics: `conflict_strategy`, `conflict_target_name`
- row outcomes: `attempted_rows`, `inserted_rows`, `duplicate_rows`, `updated_rows`, `noop_rows`, `failed_rows`
- payload cost: `payload_bytes`, `bytes_per_row`, `payload_size_bucket`
- timing: `write_ms`, `payload_build_ms`, `db_round_trip_ms`
- payload shape: `largest_json_field_name`, `largest_json_field_bytes`, `json_field_count`, `top_level_key_count`, `blob_bytes`, `has_large_payload`

The production dashboards are expected to use these axes directly:

- outcome: `inserted`, `duplicate`, `updated`, `noop`, `failed`
- source: `ingest`, `replay`, `retry`, `bootstrap`, `projector`, `transport`, `unknown`
- payload bucket: `small`, `medium`, `large`

Duplicate rows keep one separate reason taxonomy for RCA:

- `replay_duplicate`
- `retry_duplicate`
- `already_persisted_same_event_id`
- `same_seq_same_scope_duplicate`
- `same_fact_hash_duplicate`
- `bootstrap_reemit_duplicate`
- `projector_rebuild_duplicate`
- `transport_reemit_duplicate`
- `unknown_duplicate`

The current implementation also emits `same_batch_event_id_duplicate` when one batch repeats the same `event_id` before the database can explain it; this is a narrow guard added so the pipeline exposes truth instead of failing opaquely on the unique constraint.

## Dashboard Map

Primary dashboards:

- `BotLens Overview`
- `Candle Continuity` (repurposed in place from the old diagnostics dashboard UID)
- `BotLens Per-Run Deep Dive`

Secondary focused dashboards:

- `Queue Pressure & Backlog`
- `Pipeline Latency & Throughput`

Retired:

- the blank `new-dashboard.json` placeholder

The continuity dashboard is intentionally small:

- trend defect gap count by boundary stage,
- trend classified gap counts by bounded gap type,
- trend worst gap severity by boundary stage,
- compare latest boundary summaries by run/series/stage,
- inspect recent `candle_continuity_summary` events.

That is enough to determine the first broken candle boundary on the next fresh run without adding another dashboard family.

Durable health-event identity is semantic rather than purely transport-shaped:
- it ignores `known_at` heartbeat churn,
- it includes `runtime_state`, `progress_state`, `last_useful_progress_at`, degraded/churn/terminal state, normalized pressure meaning, and recent transitions,
- and it preserves distinct durable history for meaningful health changes without keying on every incidental pressure snapshot timestamp.

`storage_target` labels currently identify:

- `bot_run_view_state`
- `bot_runtime_events`
- `portal_bots`
- `portal_bot_runs`
- `portal_bot_run_lifecycle`

## Structured Event Taxonomy

Preferred operational event families are:

- run lifecycle: `run_created`, `run_phase_changed`, `run_started`, `run_live`, `run_degraded`, `run_paused`, `run_terminal_detected`, `run_completed`, `run_stopped`, `run_failed`, `run_crashed`, `run_startup_failed`, `run_evicted`
- runtime health / causality: `runtime_state_transition`, `runtime_state_transition_rejected`, `degraded_condition_started`, `degraded_condition_cleared`, `churn_detected`
- symbol lifecycle: `symbol_projector_created`, `symbol_bootstrap_applied`, `symbol_facts_applied`, `symbol_stale_session_rejected`, `symbol_stale_facts_drained`, `symbol_projector_cancelled`, `symbol_projector_failed`
- queue / pressure: `telemetry_backpressure_entered`, `telemetry_backpressure_recovered`, `symbol_fact_queue_overflow`, `run_lifecycle_queue_overflow_dropped_oldest`, `run_notification_queue_overflow`, `fanout_channel_overflow`
- bootstrap / snapshot: `bootstrap_received`, `bootstrap_superseded`, `bootstrap_applied`, `initial_state_load_started`, `initial_state_load_completed`, `initial_state_load_failed`, `viewer_snapshot_started`, `viewer_snapshot_load_failed`, `viewer_snapshot_sent`, `viewer_snapshot_buffer_overflow`
- delivery: `viewer_added`, `viewer_removed`, `viewer_send_failed`, `fanout_delivery_error`
- persistence: `db_write_failed`, `db_write_retried`, `db_write_slow`, `db_seq_collision`

The implementation normalizes names to concise snake_case and rejects one-off freeform naming.

## Emission Policy

- Counts are emitted; rates are derived in dashboards.
- Each physical queue or queue-like buffer has one queue-state owner with one stable label set.
- Shared queue gauges do not use producer-local labels such as `message_kind` or per-producer `series_key`.
- `message_kind` metric labels are bounded. Unknown kinds normalize to `unknown`; deprecated legacy kinds normalize to `deprecated`.
- Queue gauges are interval-gated to keep hot paths cheap.
- Payload sizes are measured at backend boundaries, not sprayed into arbitrary logs.
- High-frequency typed-delta INFO logs are removed.
- Pressure/drop events that may repeat under sustained overload stay in the event stream, but backend log mirroring is disabled for the noisy per-drop cases.

## Legacy Cleanup Decisions

### REPLACE IMMEDIATELY

- `TypedDeltaInstrumentation.log_emission` hot-path INFO logging.
- Emitter enqueue/dequeue/send logs in `container_runtime_telemetry.py`.
- One-off queue-full logs in mailbox, projector, fanout, and run-stream paths.
- Ad hoc payload-size/timing logging replaced by counters/histograms.

### DEPRECATE

- `PROJECTION_REFRESH_KIND` / `bot_projection_refresh` intake messages. Intake now classifies them as deprecated unknown-kind input instead of treating them as a live projection path.
- The deprecated compatibility kind itself. Active runtime logging now refers to lifecycle event delivery instead of projection refresh.

### KEEP TEMPORARILY

- `portal_bot_run_steps` step-trace persistence, because it is still a separate runtime profiling path. It is outside the BotLens v1 observability contract and should not gain new BotLens dependencies.

### REMOVE FROM RAW EVENT PAYLOADS

- `typed_delta_metrics` on raw BotLens runtime event payloads in `portal_bot_run_events`.

## Fresh-Run Audit

For the next candle continuity audit, compare boundaries in this order:

1. `source_bootstrap` / `source_facts`
2. `ingest_admission`
3. `run_bootstrap_snapshot`
4. `selected_symbol_snapshot`

Interpretation:

- if gaps already exist at the source boundary, the provider/source returned sparse truth,
- if source is clean and ingest is sparse, admission/persistence is the first broken seam,
- if ingest is clean and snapshot shaping is sparse, the projector/snapshot path is the first broken seam,
- if all stages agree, the frontend is correctly preserving sparse truth rather than inventing gaps.

## Rollout Notes

- No destructive schema migration is executed by the backend changes in this contract.
- Manual storage cleanup and schema moves are tracked in `BOTLENS_OBSERVABILITY_MIGRATION_CHECKLIST.md`.
- DB-backed exporter/storage wiring now lives behind the shared substrate without changing call sites.
