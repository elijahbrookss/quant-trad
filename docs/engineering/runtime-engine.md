# Runtime Engine

This page summarizes runtime internals. Use the deep architecture docs for full contracts and implementation detail.

## What It Is

Bot runtime is the walk-forward execution engine. It prepares strategy series, advances indicators, evaluates strategies, resolves decisions, executes fills, applies fees and margin, updates wallet state, emits runtime events, and publishes read models.

## Core Timeline

```text
initialize -> apply_bar -> snapshot
```

Runtime preparation builds strategy series and indicator state. The per-bar loop advances state in dependency order. Snapshots and runtime events become the source for BotLens, reports, and storage projections.

## Execution Ownership

Runtime owns:

- accepted and rejected decisions,
- order and trade lifecycle,
- FAST/FULL execution semantics,
- intrabar fallback diagnostics,
- fees, margin, settlement, and wallet effects,
- run lifecycle status,
- runtime event emission.

## Integration Boundaries

The backend composes runtime dependencies and controls startup/shutdown. Provider services supply candles. Strategy code supplies decision logic. BotLens and reports read runtime facts; they do not rerun execution as a normal read path.

## Next

- Source of truth: [runtime contract](../contracts/platform/01_runtime_contract.md).
- Execution detail: [execution model](../concepts/execution-model.md).
- Engine state model: [engine state model](../architecture/engine/ENGINE_STATE_MODEL.md).
- Execution boundary: [execution runtime boundary](../architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md).
- Service orchestration: [runtime composition root](../architecture/execution-runtime/RUNTIME_COMPOSITION_ROOT.md).
- Runtime events: [persistence boundary](../architecture/persistence/PERSISTENCE_BOUNDARY.md).
