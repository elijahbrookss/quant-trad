---
component: indicator-runtime-boundary
subsystem: indicator-runtime
layer: boundary
doc_type: architecture
status: active
tags:
  - indicators
  - runtime
  - typed-outputs
  - overlays
  - known-at
code_paths:
  - src/engines/indicator_engine
  - src/indicators
  - docs/architecture/indicator-runtime/diagrams/indicator-runtime-contract.mmd
  - docs/architecture/indicator-runtime/diagrams/indicator-surfaces.mmd
---
# Indicator Runtime Boundary

## Purpose

The indicator runtime boundary converts source facts and dependency outputs into typed, known-at outputs. It also exposes chart and debug projections without letting those projections become strategy inputs.

Related diagrams:

- [indicator-runtime-contract.mmd](diagrams/indicator-runtime-contract.mmd)
- [indicator-surfaces.mmd](diagrams/indicator-surfaces.mmd)

## Boundary Contract

Indicators own private state. The engine owns call order, dependency resolution, and output validation.
The engine also owns the indicator commit clock. Indicators return plain
runtime outputs; `IndicatorExecutionEngine` stamps `indicator_commit_seq` only
after it has applied the bar and validated the declared snapshot surface.

| Surface | Consumer | Contract |
| --- | --- | --- |
| `snapshot()` | decision layer, runtime | canonical strategy-visible typed outputs |
| `overlay_snapshot()` | BotLens, charts, previews | visual projection of indicator state |
| `detail_snapshot()` | operator/debug views | diagnostic payload, not a strategy input |

Strategies consume typed outputs only. They do not inspect overlays, details, helper caches, or mutable indicator internals.

## Diagram Walkthrough: Runtime Contract

[indicator-runtime-contract.mmd](diagrams/indicator-runtime-contract.mmd) shows one bar:

1. `IndicatorExecutionEngine` resolves declared dependency outputs.
2. The engine calls `apply_bar(bar, inputs)`.
3. The indicator mutates only its own internal state.
4. The engine reads `snapshot()` and validates declared output names, types, readiness, and bar time.
5. The engine stamps typed outputs and output deltas with the next
   `indicator_commit_seq` for that indicator.
6. Overlay/detail surfaces are read for projection/debug consumers when requested
   and inherit the source indicator commit sequence for provenance.
7. Typed outputs flow to the decision layer; projections flow to BotLens or chart surfaces.

This is the indicator-specific form of `initialize -> apply_bar -> snapshot`.

## Diagram Walkthrough: Indicator Surfaces

[indicator-surfaces.mmd](diagrams/indicator-surfaces.mmd) separates three surfaces:

- typed outputs answer "what may strategy logic use?"
- overlays answer "what should an operator see?"
- details answer "what should a debugger inspect?"

All three can derive from the same indicator-owned state, but only typed outputs are part of the decision contract.

## Inputs

- Provider-backed candle bars.
- Declared dependency outputs by `OutputRef`.
- Indicator runtime specs, params, and replay-window hints.

## Outputs

- `RuntimeOutput` values typed as `signal`, `context`, or `metric`.
- `RuntimeOutputDelta` `set` operations carrying
  `base_indicator_commit_seq`, `indicator_commit_seq`, and
  `indicator_commit_seq_status=indicator_scoped`.
- `RuntimeOverlay` values for visual inspection.
- `RuntimeDetail` values for debug inspection.
- Guard metrics and warnings when output/projection payloads are expensive or invalid.

## State And Lifecycle

Indicators should have one internal timeline:

```text
source facts / dependency outputs -> evidence -> committed state -> snapshot outputs -> projections
```

Every declared output is returned every bar. `ready=false` means the output exists but is not usable yet. The engine should not wait, substitute, or reconstruct missing values.
All declared outputs from the same indicator/bar share the same
`indicator_commit_seq`. Downstream consumers use that sequence to replay typed
output transitions in indicator-local causal order without relying on wall-clock
or unordered mapping iteration.

## Failure And Recovery

- Missing declared outputs fail at the engine boundary.
- Bar-time mismatches fail because they break known-at semantics.
- Invalid output types fail because they break strategy contracts.
- Overlay/detail failures should be visible to projection/debug consumers without becoming strategy truth.
- Consumers that need new strategy-visible fields should add typed outputs, not read overlays.

## Invariants

- Indicators never predict or backfill future state.
- Dependency outputs are read through declared refs.
- Signal, context, and metric outputs are typed contracts, not arbitrary blobs.
- Overlays and details are projections.
- Indicator commit sequence is engine-owned; indicator implementations must not
  fabricate or persist alternate clocks.
- Indicator-specific docs should exist only when an indicator family has architecture behavior beyond ordinary authoring guidance.

## Related Docs

- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Decision layer boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)

## Known Gaps

- Full indicator tutorials are intentionally deferred to guide docs.
- Existing indicator families may need focused architecture notes only if they introduce distinct runtime contracts.
