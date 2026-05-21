# Runtime Timeline

The runtime timeline is Quant-Trad's core model for causality.

## What It Is

All derived runtime outputs should come from one sequence:

```text
initialize -> apply_bar -> snapshot
```

Indicators initialize state, consume one bar at a time, publish typed outputs through snapshots, and optionally expose overlays or details for display and debugging.

## Known-At Timing

Derived artifacts have a known-at time. A strategy or playback view can use an artifact only when it would have existed by the current evaluation time.

This means:

- Indicators must not prebuild future overlay history.
- Strategies consume typed outputs, not indicator internals.
- Playback shows what runtime knew and did.
- Reports summarize runtime facts after the run.

## Deterministic Walk-Forward Model

Given fixed inputs, parameters, versions, and provider data, walk-forward execution should produce stable outputs. The runtime does not wait for missing values, retry silently, or substitute data that did not exist.

Invalid runtime state should fail loudly with context.

## Why Runtime Is The Source Of Truth

The runtime timeline prevents semantic drift between QuantLab, strategy preview, bot execution, BotLens, and reports. If a consumer needs data that is missing from `snapshot.payload`, the contract should be extended rather than reading mutable engine internals or reconstructing a parallel path.

## Next

- Source of truth: [runtime contract](../contracts/platform/01_runtime_contract.md).
- Execution behavior: [execution model](execution-model.md).
- Strategy flow: [strategies and signals](strategies-and-signals.md).
- Engineering summary: [runtime engine](../engineering/runtime-engine.md).
