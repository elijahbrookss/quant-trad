---
component: storage-runtime-boundary
subsystem: storage
layer: service
doc_type: architecture
status: active
tags:
  - storage
  - runtime
  - boundary
code_paths:
  - portal/backend/db/models.py
  - portal/backend/service/storage/storage.py
  - portal/backend/service/storage/repos/strategies.py
  - portal/backend/service/storage/repos/bots.py
  - portal/backend/service/storage/repos/trades.py
  - portal/backend/service/bots/runtime_composition.py
  - portal/backend/service/bots/runtime_control_service.py
  - portal/backend/service/bots/runtime_dependencies.py
---

# Storage Runtime Boundary

Runtime services consume storage through explicit service boundaries.

## Current Boundary

- `BotRuntimeControlService` depends on a narrow storage collaborator (`upsert_bot`) for state transitions.
- `bot_service.py` uses composition-provided storage gateway for run listings and latest view-state reads.
- Worker runtime persistence crosses the storage boundary only through `BotRuntimeDeps`, which is built on the portal side in `runtime_dependencies.py`.
- Strategy authoring metadata may persist named variant presets in `portal_strategy_variants`.
- Bot configuration persistence may carry strategy provenance fields such as saved variant identity and resolved params, but runtime compile/evaluation still consume only concrete strategy inputs.

## Contract

- Storage behavior must be injectable for tests.
- Runtime services should avoid deep importing storage internals at module scope.
- `PG_DSN` remains the only persistence DSN.
- Persistence required for reports/runtime audit trails must fail loud with actionable context.
- FK-constrained write paths are authoritative; tests should build a valid parent graph instead of bypassing the contract with partial rows.
