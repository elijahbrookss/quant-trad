---
component: adr-observability-bounded-non-canonical
subsystem: observability
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - observability
  - diagnostics
  - metrics
  - logs
code_paths:
  - portal/backend/service/observability.py
  - portal/backend/service/observability_exporter.py
  - portal/backend/service/storage/repos/observability.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - docker/grafana
  - docs/architecture/observability/OBSERVABILITY_BOUNDARY.md
---
# ADR 0011: Keep Observability Bounded And Non-Canonical

## Status

Accepted, backfilled on 2026-05-13.

## Context

Quant-Trad needs strong traceability across runtime, storage, BotLens,
reporting, and frontend surfaces. Logs and metrics are part of making runtime
behavior debuggable. But observability can become its own source of pressure if
hot-path counters, payload samples, and repeated events are stored without
budgeting.

Observability also cannot become trading truth.

## Decision

Observability records operational signals: lifecycle transitions, queue
pressure, latency, payload size, storage health, fallbacks, projection failures,
and degraded states. It does not decide trades, wallet truth, or report truth.

Durable observability rows are bounded through source budgets, rollups,
compaction, and exporter policy. High-cardinality raw samples remain live-only
unless explicitly justified.

## Consequences

- Operational failures and fallbacks stay visible with correlation fields.
- Dashboards explain pressure without storing one row per hot-path sample.
- Missing observability weakens debugging but does not alter execution results.
- Durable metric labels must remain bounded and stable.

## References

- [Observability boundary](../observability/OBSERVABILITY_BOUNDARY.md)
- [Engineering observability overview](../../engineering/observability.md)
- [Engineering contract](../../contracts/platform/03_engineering_contract.md)

