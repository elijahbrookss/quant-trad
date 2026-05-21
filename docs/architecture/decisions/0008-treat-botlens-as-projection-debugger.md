---
component: adr-botlens-projection-debugger
subsystem: botlens-projections
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - botlens
  - projections
  - debugger
  - read-model
code_paths:
  - portal/backend/service/bots/botlens_projector_registry.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_transport.py
  - portal/frontend/src/features/bots/botlens
  - docs/architecture/botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md
---
# ADR 0008: Treat BotLens As A Projection Debugger

## Status

Accepted, backfilled on 2026-05-13.

## Context

BotLens needs rich playback, selected-symbol switching, live deltas, overlays,
trade markers, diagnostics, and forensic reads. Those are inspection needs, not
execution responsibilities. If BotLens created truth, UI state or projection
failure could change runtime semantics.

## Decision

BotLens is a debugger built from runtime/domain facts and projector snapshots.
Normal selected-symbol reads use projector-backed run and symbol snapshots.
Ledger replay is reserved for explicit rebuild, reconciliation, and forensic
paths.

Projection failure is explicit unavailable or degraded state. BotLens does not
fabricate empty valid charts, decisions, trades, or wallet state.

## Consequences

- BotLens can cache, compact, window, and stream projection state without
  becoming canonical execution truth.
- Selected-symbol readiness uses explicit vocabulary such as
  `catalog_discovered`, `snapshot_ready`, `symbol_live`, and `run_live`.
- Stream and overlay cursors protect reconnects from stale deltas.
- Golden-run validation can block on unresolved projection failures while still
  treating durable runtime truth as the authority.

## References

- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Runtime contract: BotLens selected-symbol reads](../../contracts/platform/01_runtime_contract.md)
- [BotLens concept](../../concepts/botlens.md)

