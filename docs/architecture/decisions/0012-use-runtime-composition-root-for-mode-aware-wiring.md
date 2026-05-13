---
component: adr-runtime-composition-root-mode-aware-wiring
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - composition
  - dependency-injection
  - runtime-mode
  - portal
code_paths:
  - portal/backend/service/bots/runtime_composition.py
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/bots/runtime_control_service.py
  - docs/architecture/execution-runtime/RUNTIME_COMPOSITION_ROOT.md
---
# ADR 0012: Use A Runtime Composition Root For Mode-Aware Wiring

## Status

Accepted, backfilled on 2026-05-13.

## Context

Runtime-facing portal services need storage, stream managers, watchdogs,
runners, lifecycle services, BotLens bootstrap/projection services, and control
plane collaborators. Letting leaf services construct those dependencies through
hidden imports makes tests brittle and spreads runtime-mode conditionals.

Backtest is the implemented default today, but paper and live seams need to stay
visible so they can diverge intentionally.

## Decision

Portal runtime wiring goes through a runtime composition root. The composition
root selects collaborators by runtime mode and injects them into services.
Mode-specific differences belong in builders before they appear as scattered
leaf-service conditionals.

## Consequences

- Unsupported modes fail early.
- Tests can override collaborators explicitly.
- Backtest, paper, and live wiring remain visible even when they currently share
  collaborator shapes.
- Composition does not create execution truth; it wires services that produce,
  persist, and project it.

## References

- [Runtime composition root](../execution-runtime/RUNTIME_COMPOSITION_ROOT.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Engineering contract](../../contracts/platform/03_engineering_contract.md)

