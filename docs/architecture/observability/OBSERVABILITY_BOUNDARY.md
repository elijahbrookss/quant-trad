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
  - portal/backend/service/bots/container_runtime_telemetry.py
  - portal/backend/service/bots/botlens_run_stream.py
  - src/engines/bot_runtime/runtime/components/overlay_delta.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - portal/backend/service/storage/repos/observability.py
  - scripts/db/manual_migration_observability_metric_rollups_v1.sql
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
- which run/symbol/phase is affected?

It cannot answer by itself:

- whether a trade should have executed,
- whether PnL is correct,
- whether a decision was valid,
- what wallet truth is.

## Diagram Walkthrough

[observability-flow.mmd](diagrams/observability-flow.mmd) shows:

1. Runtime, projectors, storage, and transport emit structured metrics/events.
2. The in-memory sink keeps hot-path emission cheap.
3. The exporter persists durable observability rows after applying a storage budget.
4. Grafana, Loki, and operator tools inspect health and incidents.

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
- storage write timing,
- projection failures,
- continuity summaries,
- lifecycle and startup timing.

## What Does Not Belong Here

- trade truth,
- decision truth,
- wallet truth,
- synthetic reconstruction of runtime events,
- compatibility aliases for missing domain fields.

## Failure And Recovery

- Observability drop/overflow must itself be visible.
- Missing observability weakens debugging but must not alter execution results.
- Runtime fallbacks should emit WARN-level or metric diagnostics with enough context to investigate.
- Dashboard gaps should point back to missing instrumentation or storage, not hidden execution semantics.

## Invariants

- Logs and metrics include IDs when available: `run_id`, `bot_id`, `strategy_id`, `instrument_id`, `symbol`, `timeframe`, `trade_id`, `bar_time`.
- One event should mean one lifecycle or diagnostic fact.
- Observability is designed for traceability from QuantLab to strategy to bot to trade to playback.
- Durable observability rows must be bounded enough that observing pressure does
  not become the pressure source.

## Related Docs

- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Engineering observability overview](../../engineering/observability.md)
