---
component: adr-batch-and-degrade-botlens-projection-drain
subsystem: botlens-projections
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - botlens
  - projections
  - performance
  - observability
code_paths:
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/botlens_state.py
  - portal/backend/service/bots/botlens_transport.py
  - portal/frontend/src/components/bots/botlensProjection.js
  - src/engines/bot_runtime/runtime/components/canonical_facts.py
  - src/engines/bot_runtime/runtime/components/step_trace_rollup.py
  - portal/backend/service/storage/repos/runtime_events.py
  - docs/architecture/botlens-projections/BOTLENS_MESSAGE_FLOW_TROUBLESHOOTING.md
---
# ADR 0029: Batch And Degrade BotLens Projection Drain

## Status

Accepted on 2026-05-31.

## Context

Recent completed runs showed symbol fact queues reaching capacity while the
normal symbol apply time stayed low. The pressure was not primarily fact
construction and was not explained by frontend delivery. The backend symbol
projector was draining one queued fact envelope per event-loop turn, then
emitting a run-summary notification and fanout batch for that single envelope.

That design preserved correctness, but it amplified burst pressure. A burst of
runtime facts became many scheduler turns, many run-summary queue writes, and
many fanout packages even when the projector could apply the actual state
changes quickly.

## Decision

Symbol projectors drain ready fact envelopes in bounded batches. Within one
drain turn they preserve fact order, apply material state in order, collect the
resulting deltas, and emit one run notification plus one fanout package for the
drain.

Run health warnings remain run-level state. The run health payload now carries a
single warning summary with per-symbol buckets and grouped warning conditions.
The frontend maps only the matching symbol bucket into symbol runtime state
instead of copying the entire run warning list into every symbol panel.

Runtime-side BotLens projection dispatch is degradable. Canonical persistence
still fails loud on overflow, writer failure, or terminal drain timeout.
Projection dispatch overflow drops the oldest queued projection handoff, marks
projection as degraded, and keeps the runtime from failing because a debug
surface is behind.

Step rollups no longer persist repeated queue-depth, queue-lag, or worker-count
gauges on every bar step. Those belong to queue observability and low-rate
health, not the per-step performance table.

## Consequences

- Burst handling improves because one projector turn can drain many queued
  fact envelopes.
- Run notification and fanout pressure drops because downstream messages are
  emitted once per drain turn rather than once per queued envelope.
- BotLens projection can fall behind or require resync without killing an
  otherwise valid run.
- Canonical runtime truth remains strict: persistence pressure is still a
  runtime failure.
- Warning counts become easier to reason about because run health carries one
  grouped summary and symbol views receive symbol-specific buckets.
- The step rollup table should grow more slowly and remain focused on timing
  and payload analysis.

## References

- [Runtime Contract](../../contracts/platform/01_runtime_contract.md)
- [BotLens Projection Boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [BotLens Message Flow Troubleshooting](../botlens-projections/BOTLENS_MESSAGE_FLOW_TROUBLESHOOTING.md)
- [Persistence Boundary](../persistence/PERSISTENCE_BOUNDARY.md)
