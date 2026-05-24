---
component: engine-state-model
subsystem: engine
layer: architecture
doc_type: architecture
status: active
tags:
  - engine
  - known-at
  - runtime
  - deterministic
  - state-machine
code_paths:
  - src/engines
  - src/engines/indicator_engine
  - src/engines/bot_runtime/runtime
  - src/strategies/evaluator.py
  - docs/architecture/engine/diagrams/engine-boundaries.mmd
  - docs/architecture/engine/diagrams/engine-known-at-timeline.mmd
---
# Engine State Model

## Purpose

The engine boundary is the cross-cutting state model that keeps Quant-Trad deterministic. It is not a single Python package. It is the rule that indicators, strategy evaluation, bot execution, projections, and reports all derive from one walk-forward timeline.

Related diagrams:

- [engine-boundaries.mmd](diagrams/engine-boundaries.mmd)
- [engine-known-at-timeline.mmd](diagrams/engine-known-at-timeline.mmd)

## Core Contract

```text
initialize -> apply_bar -> snapshot
```

`initialize` prepares state. `apply_bar` advances one known market event. `snapshot` exposes the state that is known after that event.

This model applies to:

- indicator runtime outputs,
- strategy-visible signal/context/metric values,
- decision artifacts,
- bot runtime state,
- BotLens playback/read models,
- reporting datasets that replay or summarize runtime truth.

## Diagram Walkthrough: Engine Boundaries

[engine-boundaries.mmd](diagrams/engine-boundaries.mmd) shows the ownership chain:

1. Data supplies source facts.
2. Indicator runtime turns those facts into typed output timelines.
3. Decision layer consumes typed outputs and emits decision artifacts.
4. Execution runtime resolves accepted behavior into fills, wallet effects, settlement, and events.
5. Persistence stores durable event/trade truth.
6. BotLens, reports, and frontend state project from that truth.

Dashed projection paths are intentionally one-way. Overlays, details, live snapshots, and report summaries help inspection, but they are not execution inputs unless promoted into a typed runtime contract.

## Diagram Walkthrough: Known-At Timeline

[engine-known-at-timeline.mmd](diagrams/engine-known-at-timeline.mmd) follows one bar at time `t`:

1. The bar becomes available.
2. Indicators apply the bar and publish outputs known at `t`.
3. Decision rules evaluate against those outputs and bounded prior history.
4. Runtime resolves execution using the configured execution semantics.
5. Runtime events are appended and downstream views update.

No layer may reach forward to future bars, future indicator states, post-run report facts, or UI playback state while evaluating `t`.

## Boundary Responsibilities

| Area | Owns | Upstream | Downstream |
| --- | --- | --- | --- |
| Indicator runtime | Dependency ordering, `apply_bar`, output validation, overlay/detail reads | Data boundary, indicator manifests | Decision layer, projections |
| Decision layer | Rule/guard evaluation, signal consumption, decision artifacts | Typed outputs and bounded history | Execution runtime |
| Execution runtime | Ordering, execution mode, fills, fees, margin, wallet, settlement, domain events | Decisions and runtime config | Persistence, BotLens, reports |
| Projection/read models | Bounded inspection state | Runtime/domain events | Frontend, forensics, reports |

## Failure Behavior

- Missing or malformed indicator outputs fail at the indicator boundary.
- Missing strategy inputs make a rule false or produce a rejected decision with context.
- Execution ambiguity uses only contract-defined fallbacks and emits diagnostics.
- Projection failures surface unavailable/degraded state instead of fabricated valid state.
- Consumers that need data not present in `snapshot.payload` should extend the engine contract, not read mutable internals.

## Invariants

- Runtime truth is single-source.
- Known-at timing is preserved across every boundary.
- Output payloads are typed at real contracts, not giant unbounded blobs.
- Hot-path payloads stay small enough for live projection and replay.
- Cold-path debug/history reads can be heavy, but they cannot mutate historical truth.

## Related Docs

- [System model](../system/SYSTEM_MODEL.md)
- [Indicator runtime boundary](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md)
- [Decision layer boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
