---
component: observability-boundary
subsystem: observability
layer: boundary
doc_type: architecture
status: active
tags:
  - observability
  - diagnostics
  - metrics
  - logs
  - botlens
code_paths:
  - portal/backend/controller/bots.py
  - portal/backend/service/observability.py
  - portal/backend/service/observability_exporter.py
  - portal/backend/service/bots/runner_observability.py
  - portal/backend/service/bots/container_runtime_telemetry.py
  - portal/backend/service/bots/botlens_intake_router.py
  - portal/backend/service/bots/botlens_candle_continuity.py
  - portal/backend/service/bots/botlens_run_stream.py
  - src/engines/bot_runtime/runtime/components/step_trace_buffer.py
  - src/engines/bot_runtime/runtime/components/step_trace_rollup.py
  - src/engines/bot_runtime/runtime/components/overlay_delta.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - portal/backend/service/storage/repos/observability.py
  - portal/backend/service/storage/repos/market_collection.py
  - portal/backend/service/market/collector_service.py
  - portal/backend/workers/market_data_collector.py
  - cli/logs.py
  - src/core/logger.py
  - src/utils/logging_utils.py
  - scripts/db/manual_migration_observability_metric_rollups_v1.sql
  - scripts/db/manual_migration_versioning_hard_cutover.sql
  - docker/docker-compose.yml
  - docker/promtail/config.yml
  - docker/loki/config.yml
  - docker/grafana
  - docs/architecture/observability/diagrams/observability-flow.mmd
---
# Observability Boundary

## Purpose

The observability boundary explains runtime and projection health without becoming runtime truth. It records lifecycle transitions, queue pressure, latency, payload size, storage health, fallbacks, and degraded states.

Related diagram: [observability-flow.mmd](diagrams/observability-flow.mmd).

## Boundary Contract

Observability owns operational signals. Runtime/domain events own trading truth.

Observability can answer:

- is the runtime alive?
- did a projection fail?
- are payloads too large?
- is a stream dropping or lagging?
- did execution fall back?
- did a runner pause or container lifecycle event explain degraded ownership?
- which run/symbol/phase is affected?

It cannot answer by itself:

- whether a trade should have executed,
- whether PnL is correct,
- whether a decision was valid,
- what wallet truth is.
- whether a run is golden-certified or research-valid.

Observability rows are not certification evidence unless a material runtime or
reporting boundary explicitly promotes the same fact into canonical evidence.
BotLens-selected-symbol, bootstrap, viewer, and debug rows are diagnostic by
default. They may explain what an observer saw, but they must not change
`data_snapshot_hash`, semantic fingerprints, golden certification, lifecycle
truth, wallet/order/trade facts, or research-valid status.

## Diagram Walkthrough

[observability-flow.mmd](diagrams/observability-flow.mmd) shows:

1. Runtime, projectors, storage, and transport emit structured metrics/events.
2. The in-memory sink keeps hot-path emission cheap.
3. The exporter persists durable observability rows after applying a storage budget.
4. Grafana, Loki, and operator tools inspect health and incidents.

`qt logs` is the operator-facing Loki tool. It does not create observability
truth; it queries Loki, parses Quant-Trad structured log lines into fields, and
keeps run incident investigation away from ad hoc curl command knowledge.

Backend and runtime application logs enter Loki through one normal path:
container stdout/stderr, Docker log storage, Promtail, then Loki. Runtime code
must not synchronously post ordinary log lines to Loki. Bot-runtime processes
append `run_id`, `bot_id`, `service=bot-runtime`, and `runtime=bot` to log
lines so run-centered searches remain cheap enough without indexing every run as
a Loki stream label.

## Storage Budget

Observability storage is diagnostic, not canonical run truth. Raw backend metric
samples are live-only by default and must not be appended permanently at hot-path
cardinality. The exporter converts them into durable rollups keyed by
run/component/metric/bounded-label/time bucket.

- high-volume BotLens transport, fanout, projector, intake, and telemetry
  metric families are source-budgeted before they enter the export queue,
- source-budgeted records preserve represented sample count, sum, min, max, and
  latest value so rollups can still explain pressure without one record per
  emitted sample,
- source budgeting applies to hot-path wait/apply/enqueue/retention counters
  and latency samples; failure, error, overflow, retry, and storage-write metric
  families bypass source budgeting so operational blockers stay exact,
- low-value live transport counters and payload-size samples are skipped by the
  durable exporter policy; failures, drops, retries, overflow, storage writes,
  and exporter health metrics are always durable,
- rollups store count, sum, min, max, latest value, conservative p95/p99,
  first-seen, and last-seen,
- metric samples are merged in memory for the current rollup bucket before DB
  upsert; shutdown forces a final drain,
- counters are summed into bucket `value_sum`,
- latency panels read rollup p95/p99 fields instead of raw samples,
- pressure gauges such as depth, utilization, age, high-water, and byte metrics
  keep the bucket maximum,
- step-profiler storage is duration-only; queue pressure, persistence lag,
  payload size, worker health, and debug counters are retained through this
  observability budget instead of duplicated into per-step rollups,
- labels are bounded to stable diagnostic dimensions; unbounded ids, messages,
  and errors are not part of the durable metric identity,
- low-value repeated overflow events are compacted into one latest event with a
  suppressed duplicate count.

Exporter write latency is itself observable through
`observability_raw_samples_seen`, `observability_metric_records_seen`,
`observability_live_only_metric_records_skipped`,
`observability_live_only_raw_samples_skipped`,
`observability_rollup_rows_written`, `observability_rollup_reduction_ratio`,
`observability_source_budget_reduction_ratio`,
`observability_export_db_ms`, and `observability_export_errors`. Dashboards must
use these alongside storage `db_write_*` metrics; `db_write_ms` alone is not a
complete database pressure signal.

## What Belongs Here

- queue depth and drops,
- latency and freshness,
- payload size,
- runtime-to-portal BotLens fact-stream build/serialize/enqueue attribution by
  surface,
- runtime fact-stream compaction effectiveness for health/runtime state,
  overlays, series stats, and symbol summary facts,
- backend live transport build/serialize/dispatch attribution by surface,
- fallback and degrade events,
- runner clock-gap diagnostics,
- Docker container lifecycle diagnostics for Quant-Trad containers,
- structured Loki inspection through `qt logs` for run incident forensics,
- Promtail-scraped backend and bot-runtime stdout/stderr logs,
- control-plane telemetry flush status for runtime lifecycle and bootstrap
  messages,
- storage write timing,
- scheduled market-fact worker heartbeat/expiry and active-attempt scope,
- bounded collector attempt stage timing for pacing, provider request,
  normalization, validation, persistence, and total visibility lag,
- projection failures,
- continuity summaries,
- lifecycle and startup timing.
- coordinator wait attribution such as `decision_order_top_waits_merged`,
  which explains shared-wallet market-progress waits but is not material
  wallet/order/trade evidence.

## What Does Not Belong Here

- trade truth,
- decision truth,
- wallet truth,
- synthetic reconstruction of runtime events,
- compatibility aliases for missing domain fields.

## Failure And Recovery

- Observability drop/overflow must itself be visible.
- Missing observability weakens debugging but must not alter execution results.
- Missing observability must not be repaired by certifying from viewer/debug
  facts. Reports should fail loudly when canonical evidence is missing.
- Runtime fallbacks should emit WARN-level or metric diagnostics with enough context to investigate.
- Runner clock gaps and Docker lifecycle events explain operational liveness;
  they must not become strategy, wallet, order, trade, or report truth.
- Runtime lifecycle and bootstrap messages use the telemetry control lane. Close
  may drop ordinary projection/debug telemetry, but it must attempt a bounded
  control-lane flush and emit `telemetry_control_flush_timeout` if delivery
  cannot be proven.
- Container telemetry transports must avoid sync websocket background clients;
  websocket open/send/close belongs to the async telemetry worker or the bounded
  direct fallback path.
- Runtime fact transport may coalesce superseded non-material
  `botlens_runtime_facts` messages by run/series while preserving control-lane
  and material trade, wallet, and decision fact delivery. Coalescing is a live
  projection pressure valve only; canonical fact persistence remains the source
  of durable truth. Coalescing volume is retained as a source-budgeted metric,
  not one durable event per superseded projection message, so diagnostic noise
  cannot consume a run report's bounded event-evidence window.
- Runtime step traces are aggregated in memory into compact profiler rollups
  before persistence. The hot path records timing samples, but the writer ships
  mergeable bucket rows instead of one payload per bar. Shutdown drains pending
  rollups; persist failures remain visible diagnostics.
- BotLens ingest routes projection batches before waiting on diagnostic durable
  writes. Persistence runs in bounded background batches and emits explicit
  errors on failure so API websocket receive loops are not held hostage by
  ordinary projection/debug storage pressure.
- The telemetry ingest WebSocket yields to the event loop after every routed
  frame. Backlogged frames may otherwise let receive and routing complete
  synchronously long enough to starve Uvicorn keepalive handling and cause
  deterministic 40-second code-1006 reconnect churn.
- Scheduled collector liveness is a mutable worker-state projection. Heartbeat
  expiry means the process is not proven alive; it does not rewrite previously
  accepted market facts. Attempt timing stays bounded inside the existing typed
  attempt evidence instead of creating one durable metric row per stage.
- Dashboard gaps should point back to missing instrumentation or storage, not hidden execution semantics.
- If Promtail/Loki are down while a short-lived bot container starts and exits,
  and the container is later removed, Loki cannot retroactively recover that
  runtime stdout/stderr stream. The fix is observability availability and
  durable Docker/Loki storage, not a second runtime logging path.

## Invariants

- Logs and metrics include IDs when available: `run_id`, `bot_id`, `strategy_id`, `instrument_id`, `symbol`, `timeframe`, `trade_id`, `bar_time`.
- One event should mean one lifecycle or diagnostic fact.
- Observability is designed for traceability from QuantLab to strategy to bot to trade to playback.
- Durable observability rows must be bounded enough that observing pressure does
  not become the pressure source.
- Loki labels must stay bounded to routing dimensions such as `job`, `service`,
  `runtime`, `container`, and `compose_service`; run and bot identity belongs in
  the structured log line unless a future measured need justifies the
  cardinality cost.
- Future MCP inspection/debug calls are observationally safe by default: pure
  reads must not create material evidence, and optional diagnostic writes remain
  non-material and best-effort.

## Related Docs

- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Engineering observability overview](../../engineering/observability.md)
- [ADR 0033: Use Promtail as Runtime Loki Ingress](../decisions/0033-use-promtail-as-runtime-loki-ingress.md)
