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
  - portal/backend/service/storage/repos/lifecycle.py
  - portal/backend/service/storage/repos/strategies.py
  - portal/backend/service/storage/repos/bots.py
  - portal/backend/service/storage/repos/trades.py
  - portal/backend/service/bots/bot_run_diagnostics_projection.py
  - portal/backend/service/bots/runtime_composition.py
  - portal/backend/service/bots/runtime_control_service.py
  - portal/backend/service/bots/runtime_dependencies.py
---

# Storage Runtime Boundary

Runtime services consume storage through explicit service boundaries.

## Current Boundary

- `BotRuntimeControlService` consumes an explicit storage gateway for bot rows, run rows, and lifecycle checkpoints.
- `bot_service.py` uses composition-provided storage reads for run listings, lifecycle state, and latest view-state reads.
- Worker runtime persistence crosses the storage boundary only through `BotRuntimeDeps`, which is built on the portal side in `runtime_dependencies.py`.
- Backend/container startup lifecycle state persists through:
  - `portal_bot_run_lifecycle`
  - `portal_bot_run_lifecycle_events`
- Run diagnostics for UI consumers are projected from those lifecycle tables; they do not introduce a second persistence schema.
- Strategy authoring metadata may persist named variant presets in `portal_strategy_variants`.
- Bot configuration persistence may carry strategy provenance fields such as saved variant identity and resolved params, but runtime compile/evaluation still consume only concrete strategy inputs.

## Contract

- Storage behavior must be injectable for tests.
- Runtime services should avoid deep importing storage internals at module scope.
- `PG_DSN` remains the only persistence DSN.
- Persistence required for reports/runtime audit trails must fail loud with actionable context.
- Startup truth must be durable enough to survive refresh/reconnect without depending on in-memory process state.
- Frontend diagnostics contracts should be derived from durable lifecycle rows and append-only checkpoint trails, not from client-side reconstruction of raw event semantics.
- FK-constrained write paths are authoritative; tests should build a valid parent graph instead of bypassing the contract with partial rows.

## BotLens Observability Note

BotLens backend observability now separates runtime truth from observability signals at the contract level:

- runtime state and replay ledgers remain storage responsibilities,
- metrics/events are emitted through the backend observability substrate and exported into dedicated observability schemas instead of being embedded into hot-path logs,
- and manual schema cleanup for table ownership drift is tracked in `BOTLENS_OBSERVABILITY_MIGRATION_CHECKLIST.md`.
