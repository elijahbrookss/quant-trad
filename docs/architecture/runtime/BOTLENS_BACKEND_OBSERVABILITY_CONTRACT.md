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
  - portal/backend/service/observability.py
  - portal/backend/service/bots/container_runtime_telemetry.py
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
- counter / histogram / gauge wrappers,
- interval-gated queue gauge helpers,
- payload size helpers,
- timed block utilities,
- structured event emission,
- normalization for event names and failure modes,
- a process-local sink for tests and future exporter wiring.

Metrics and structured events are separate on purpose:

- metrics describe flow, pressure, latency, and payload size,
- structured events describe transitions, anomalies, recoveries, and failures,
- backend logs only mirror warning/error events by default,
- INFO-level structured events stay in the observability sink unless a caller opts into logging them.

## Canonical Context

Stable operational context is attached whenever available:

- `bot_id`
- `run_id`
- `instrument_id` or `series_key`
- `component`
- `worker_id`

Allowed metric labels:

- `queue_name`
- `pipeline_stage`
- `message_kind`
- `delta_type`
- `storage_target`
- `failure_mode`

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

### Projectors

- symbol projector: `symbol_projector_*`
- run projector: `run_projector_*`
- lifecycle transition events normalized onto `run_phase_changed`, `run_terminal_detected`, and `run_evicted`

### Fanout / delivery / run stream

- fanout queue: `fanout_*`
- delivery loop: `fanout_delivery_*`
- viewer socket delivery: `viewer_send_total`, `viewer_send_fail_total`, `viewer_send_ms`, `viewer_broadcast_*`, `viewer_payload_bytes`
- snapshot hydration: `viewer_snapshot_load_ms`, `viewer_snapshot_total_ms`, `snapshot_replay_count`

### Persistence

- `db_write_total`
- `db_write_fail_total`
- `db_write_ms`
- `db_write_rows`
- `db_write_payload_bytes`
- `db_write_retry_total`
- `db_duplicate_skip_total`
- `db_stale_update_total`
- `persistence_wait_ms`
- `db_initial_load_ms`
- events: `db_write_failed`, `db_write_retried`, `db_write_slow`, `db_seq_collision`, `db_initial_state_load_completed`, `db_initial_state_load_failed`

`storage_target` labels currently identify:

- `bot_run_view_state`
- `bot_runtime_events`
- `portal_bots`
- `portal_bot_runs`
- `portal_bot_run_lifecycle`

## Structured Event Taxonomy

Preferred operational event families are:

- run lifecycle: `run_created`, `run_phase_changed`, `run_started`, `run_live`, `run_degraded`, `run_paused`, `run_terminal_detected`, `run_completed`, `run_stopped`, `run_failed`, `run_crashed`, `run_startup_failed`, `run_evicted`
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

## Rollout Notes

- No destructive schema migration is executed by the backend changes in this contract.
- Manual storage cleanup and schema moves are tracked in `BOTLENS_OBSERVABILITY_MIGRATION_CHECKLIST.md`.
- Exporter/dashboard wiring can be added later behind the shared substrate without changing call sites again.
