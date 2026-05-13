---
component: decision-layer-boundary
subsystem: decision-layer
layer: boundary
doc_type: architecture
status: active
tags:
  - strategy
  - decision
  - signals
  - rules
  - runtime
code_paths:
  - src/strategies
  - portal/backend/service/strategies
  - portal/backend/controller/strategies.py
  - src/engines/bot_runtime/strategy
  - portal/backend/service/bots/strategy_loader.py
  - docs/architecture/decision-layer/diagrams/decision-flow.mmd
  - docs/architecture/decision-layer/diagrams/signal-consumption-contract.mmd
---
# Decision Layer Boundary

## Purpose

The decision layer converts typed indicator outputs into explicit decision artifacts. Signals belong here conceptually: a signal is an indicator output consumed by strategy rules and preserved as decision provenance.

Related diagrams:

- [decision-flow.mmd](diagrams/decision-flow.mmd)
- [signal-consumption-contract.mmd](diagrams/signal-consumption-contract.mmd)

## Boundary Contract

The decision layer owns:

- strategy compilation and material hashing,
- rule triggers and guard evaluation,
- signal consumption,
- context and metric gating,
- bounded output history for held/window guards,
- decision artifacts,
- accepted/rejected decision reasons.

It does not own:

- indicator state,
- overlay geometry,
- fill ordering,
- fees,
- margin,
- wallet state,
- settlement,
- BotLens projection state.

## Diagram Walkthrough: Decision Flow

[decision-flow.mmd](diagrams/decision-flow.mmd) shows the runtime path:

1. Typed indicator outputs arrive as `signal`, `context`, and `metric` values.
2. Bounded output history is updated for held and lookback guards.
3. The compiled strategy supplies ordered rules, triggers, guards, intents, and priorities.
4. The evaluator emits decision artifacts.
5. Execution runtime accepts or rejects selected artifacts based on runtime state, risk, wallet, and position policy.
6. Runtime events preserve the decision and trade lifecycle.

Rejected decisions matter. A missed trade should be inspectable through an artifact and reason, not disappear.

## Diagram Walkthrough: Signal Consumption

[signal-consumption-contract.mmd](diagrams/signal-consumption-contract.mmd) shows the signal contract:

- indicators publish `type=signal` outputs,
- rules consume signals through `signal_match` and signal-window guards,
- context and metric outputs gate those signals,
- selected decisions carry signal provenance,
- runtime emits decision/trade events with distinct IDs.

`signal_id` and `decision_id` are intentionally different identifiers. Do not alias them for legacy compatibility.

## Inputs

- `CompiledStrategySpec` including strategy ID, strategy hash, timeframe, rules, and history needs.
- Typed indicator outputs for the current bar.
- Bounded output history.
- Instrument/series context from runtime.

## Outputs

- Decision artifacts with `decision_id`, `strategy_id`, `strategy_hash`, `instrument_id`, rule ID, intent, direction, and evidence.
- Compact `referenced_outputs` snapshots for the typed outputs that caused or gated the decision. These snapshots carry output identity, type, readiness, bar time, and indicator commit sequence, but not overlays, details, debug blobs, or full indicator state.
- Rejection artifacts with stage and reason.
- Runtime-facing provenance fields for event emission.

## State And Truth

Decision artifacts are runtime truth candidates. They are not fills. Runtime decides whether an artifact can become execution behavior.

The decision layer can remember bounded output history because some guards ask whether a condition held or a signal was seen/absent within a window. That history must be built from known-at outputs only.

## Failure And Recovery

- Missing typed outputs make dependent rules false or rejected with context.
- Invalid strategy specs fail at compile/load time.
- Runtime rejections should include explicit reason codes and blocking context.
- Strategy previews must not use a different semantic path than runtime decisions.

## Invariants

- Signals are decision-layer inputs, not a separate architecture boundary.
- Strategies read typed outputs, not indicator internals.
- Runtime decision provenance captures only rule-referenced typed outputs at the decision boundary; reporting must not reconstruct indicator context from hidden state.
- `strategy_hash` travels with decisions for reproducibility.
- Bounded history never includes future bars.
- Execution state can reject a valid strategy decision, and that rejection is part of truth.

## Related Docs

- [Indicator runtime boundary](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md)
- [Regime context boundary](REGIME_CONTEXT_BOUNDARY.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Identity and correlation boundary](../identity/IDENTITY_AND_CORRELATION_BOUNDARY.md)

## Known Gaps

- Full strategy-authoring tutorials are intentionally outside architecture.
- Strategy preview and runtime decision paths should continue to be checked for semantic drift as features evolve.
