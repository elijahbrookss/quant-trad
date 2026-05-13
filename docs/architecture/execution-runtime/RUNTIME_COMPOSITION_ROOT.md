---
component: runtime-composition-root
subsystem: execution-runtime
layer: service
doc_type: architecture
status: active
tags:
  - composition
  - dependency-injection
  - runtime
  - portal
  - runtime-mode
code_paths:
  - portal/backend/service/bots/runtime_composition.py
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/bots/runtime_control_service.py
---
# Runtime Composition Root

## Purpose

`runtime_composition.py` is the portal-side composition root for runtime-facing services. It keeps runtime wiring explicit, testable, and mode-aware.

## Boundary Contract

Composition owns collaborator selection. Leaf services should not deep-import storage, watchdog, runner, stream manager, or control-service construction as hidden globals.

## Current Modes

- `backtest`: implemented default.
- `paper`: explicit seam, currently same collaborator shape unless code says otherwise.
- `live`: explicit seam, currently same collaborator shape unless code says otherwise.

Mode-specific behavior belongs in composition builders before it appears as scattered conditionals inside leaf services.

## Collaborators

`RuntimeComposition` wires the portal control plane:

- stream manager,
- bot config service,
- runtime control service,
- storage gateway,
- watchdog,
- runner-facing dependencies.

Worker runtime dependencies are built separately and injected into container runtime code.

## Upstream And Downstream

Upstream:

- API/controller requests,
- bot config,
- runtime mode,
- storage/session access,
- runner/container configuration.

Downstream:

- container runtime startup and stop commands,
- lifecycle storage,
- BotLens/bootstrap/projection services,
- stream broadcasts,
- watchdog behavior.

## Failure Behavior

- Unsupported modes fail early.
- Missing collaborators fail construction or startup with explicit context.
- Tests should override composition explicitly instead of monkeypatching deep imports.

## Invariants

- Composition is the only place that should know the full runtime service graph.
- Mode seams remain visible even while paper/live share backtest collaborator shapes.
- Runtime composition does not create execution truth; it wires services that produce and persist truth.

## Related Docs

- [Execution runtime boundary](EXECUTION_RUNTIME_BOUNDARY.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Observability boundary](../observability/OBSERVABILITY_BOUNDARY.md)
