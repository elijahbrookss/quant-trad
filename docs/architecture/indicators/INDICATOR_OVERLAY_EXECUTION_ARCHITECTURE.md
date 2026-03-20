---
component: indicator-overlay-execution
subsystem: indicators
layer: service
doc_type: architecture
status: active
tags:
  - indicators
  - overlays
  - runtime
  - service
  - quantlab
code_paths:
  - portal/backend/controller/indicators.py
  - portal/backend/service/indicators/async_dispatch.py
  - portal/backend/service/indicators/indicator_service/api.py
  - portal/backend/service/indicators/indicator_service/runtime_graph.py
  - portal/backend/workers/indicator_worker.py
  - src/engines/indicator_engine/runtime_engine.py
---
# Indicator Overlay Execution Architecture

## Problem and scope

QuantLab overlay requests must respect the same walk-forward runtime semantics as strategy preview and bot runtime, but they must not block the backend API process with CPU-heavy indicator execution.

This document defines the execution boundary for indicator overlays.

In scope:
- QuantLab overlay request execution,
- async job routing for overlays,
- runtime replay behavior for overlay collection,
- dependency-closed indicator graph execution for overlays.

Out of scope:
- frontend overlay ownership and chart merging behavior,
- transport diffing or websocket streaming,
- strategy signal execution.

## Canonical flow

```mermaid
flowchart LR
    A[QuantLab POST /indicators/{id}/overlays] --> B[enqueue overlay job]
    B --> C[indicator worker claims job]
    C --> D[build dependency-closed runtime graph]
    D --> E[fetch canonical candle window]
    E --> F[replay IndicatorExecutionEngine over bars]
    F --> G[collect overlays only on terminal replay step]
    G --> H[return canonical overlay payload]
```

## Execution rules

- Overlay requests execute through the shared indicator async job queue.
- Overlay requests do not run CPU-heavy replay on the backend request thread or event loop.
- Workers execute the same dependency-closed runtime graph used by other runtime surfaces.
- Indicators advance state on every replay bar through `apply_bar()`.
- Overlay collection is requested only on the terminal replay step when the consumer needs only the final current overlay state.
- Consumers that need full overlay history must assemble that history from the runtime timeline; indicators must not rebuild chart history inside `apply_bar()`.

## Dependency semantics

- The requested indicator id is the root of the overlay job.
- Explicit instance dependency bindings are resolved transitively before execution.
- Independent overlay requests may run in parallel across worker slots.
- Dependent indicators execute in the same runtime graph so overlay semantics stay aligned with typed outputs.

## Logging contract

Required lifecycle logs:
- `indicator_overlay_request_started`
- `indicator_overlay_request_finished`
- `indicator_overlay_request_failed`
- `indicator_worker_job_started`
- `indicator_worker_job_succeeded`
- `indicator_worker_job_failed`
- `indicator_overlay_execute_complete`
- `indicator_runtime_instance_built`

High-signal timing fields:
- request duration
- runtime graph build duration
- source fetch duration
- candle build duration
- engine replay duration
- overlay collection duration

These logs must include indicator id/type plus symbol/timeframe context when available.

## Notes

- This architecture removes API-loop blocking for overlay execution, but it does not yet add explicit cancellation or stale-result invalidation at the job layer.
- Correctness still comes from deterministic runtime replay and dependency-closed execution, not from frontend-side reconstruction.
- QuantLab may trigger multiple overlay jobs together, but frontend publication is per-indicator:
  each completed indicator replaces only its own overlay slice, and a slower indicator must not block already-finished indicator overlays from appearing.
