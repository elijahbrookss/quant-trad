---
component: adr-control-plane-telemetry-flush
subsystem: observability
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - observability
  - botlens
  - telemetry
  - lifecycle
code_paths:
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/bots/container_runtime_telemetry.py
  - tests/test_portal/test_container_runtime_transport.py
---
# ADR 0026: Use Control-Plane Telemetry Flush

## Status

Accepted on 2026-05-20.

Amended on 2026-08-03 after retained overlay evidence made pre-emptive
general-lane discard unsafe.

## Context

A parallel backtest stress run completed successfully, but terminal telemetry
shutdown exposed two transport smells:

- terminal lifecycle used a fresh direct websocket before the persistent sender,
  so the most important lifecycle event competed with connection setup during a
  loaded shutdown path,
- the persistent telemetry sender used `websockets.sync`, whose background
  receive thread can raise shutdown assertions after the runtime process has
  otherwise completed cleanly.

The durable lifecycle checkpoint was persisted and the run report was produced,
so the incident was operational telemetry degradation, not runtime truth loss.
Still, terminal lifecycle delivery is control-plane information and deserves a
stronger delivery path than best-effort viewport facts.

## Decision

Use the persistent telemetry sender as the primary control-plane transport for
terminal lifecycle and bootstrap control messages.

The sender owns two lanes:

- `telemetry_control_queue` for lifecycle and bootstrap/control messages,
- `telemetry_emit_queue` for ordinary runtime fact batches.

Control messages can be sent with `send_and_wait`, which enqueues the message
and waits for the worker to confirm transport delivery. Runtime terminal
lifecycle now tries this control flush first. If delivery cannot be proven
within the bounded timeout, runtime logs a WARN and falls back to a direct
ephemeral websocket attempt.

The telemetry worker uses one async websocket connection owned by its worker
thread. It no longer imports or uses `websockets.sync`. Close semantics are:

- stop accepting new telemetry,
- prioritize control messages,
- drain accepted control and general-lane messages up to one bounded timeout,
- never coalesce material facts such as trades, decisions, wallet ledger
  events, or retained overlay operations,
- mark remaining messages failed and log
  `telemetry_shutdown_flush_timeout` with the owning lane when the deadline
  expires,
- close the async websocket from the worker loop.

## Consequences

- Terminal lifecycle delivery no longer depends first on creating a fresh
  websocket during shutdown pressure.
- The `websockets.sync` recv-thread shutdown assertion is removed from the
  runtime telemetry path.
- Runtime facts remain projection/debug telemetry, but already accepted facts
  receive a bounded shutdown delivery attempt. Lifecycle control messages stay
  prioritized.
- A control flush timeout is visible as observability, not hidden by a silent
  queue clear.
- The fallback direct websocket can still duplicate a lifecycle event if the
  queued control delivery succeeds after the timeout. Lifecycle consumers must
  continue to treat `(run_id, seq/status/phase)` as idempotent projection input.

## References

- [Observability Boundary](../observability/OBSERVABILITY_BOUNDARY.md)
- [BotLens Projection Boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
