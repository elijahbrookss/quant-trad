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
  - portal/backend/service/storage/storage.py
  - portal/backend/service/bots/runtime_composition.py
  - portal/backend/service/bots/runtime_control_service.py
---

# Storage Runtime Boundary

Runtime services consume storage through explicit service boundaries.

## Current Boundary

- `BotRuntimeControlService` depends on a narrow storage collaborator (`upsert_bot`) for state transitions.
- `bot_service.py` uses composition-provided storage gateway for run listings and latest view-state reads.

## Contract

- Storage behavior must be injectable for tests.
- Runtime services should avoid deep importing storage internals at module scope.
- `PG_DSN` remains the only persistence DSN.
