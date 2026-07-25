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
  - portal/backend/service/strategy_variant_resolution.py
  - portal/backend/service/strategies
  - portal/backend/service/bots/config_service.py
  - portal/backend/controller/strategies.py
  - cli/main.py
  - cli/experiments/instrument_matrix.py
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

## What A Decision Carries Forward

The evaluator starts with a `CompiledStrategySpec`, typed indicator outputs for
the current bar, bounded output history, and instrument/series context from
runtime. It emits decision artifacts with stable decision, strategy, rule,
instrument, intent, direction, and evidence fields. It also emits rejection
artifacts with stage and reason when a rule path cannot become a trade
candidate.

Every provided typed output must carry the exact evaluation bar time. The
evaluator rejects stale or future-dated outputs before evaluating rules or
advancing bounded history.

Those artifacts are runtime truth candidates, not fills. Runtime still decides
whether a candidate becomes execution behavior.

The decision record keeps compact provenance for the typed outputs that caused
or gated the decision. `referenced_outputs` carry output identity, type,
readiness, bar time, and indicator commit sequence. They do not copy overlays,
details, debug blobs, or full indicator state. `output_filter_trace` records
variant filters as audit evidence from the same guard evaluation result, not as
a second rule-evaluation path.

The decision layer may remember bounded output history because some guards ask
whether a condition held, appeared, or stayed absent within a window. That
history is built from known-at outputs only.

Strategy variants are named diffs against a strategy/default variant. Preview,
bot config, runtime loading, and report metadata must resolve the same
`effective_params` through the shared variant resolver. Runtime strategy models
carry the resulting `effective_strategy_config` and `run_strategy_snapshot` as
provenance only; these fields must not change evaluator, wallet, order, fee, or
trade semantics.

Strategy read contracts are split by concern:

- `strategy_inventory.v1` lists thin strategy rows and counts.
- `strategy_definition.v1` returns the core strategy definition and readiness
  context.
- `strategy_bindings.v1` returns compact instrument and indicator bindings
  without embedding full indicator manifests in inventory rows.
- `strategy_rules.v1` returns stored decision rules.
- `strategy_variants.v1` returns saved variant rows.
- `effective_strategy.v1` returns the compiled, runtime-effective strategy for
  the selected/default variant.
- `strategy_decision_inputs.v1` returns attached indicator signal, context, and
  metric inputs and marks which effective rules or variant filters reference
  them.
- `strategy_preview_summary.v1` returns the compact agent-facing answer for a
  preview: evaluated bars, decision artifact counts, signal counts, first/last
  signal times, event/rule breakdowns, examples, and empty-preview diagnostics.
- `strategy_preview_compare.v1` compares multiple compact preview summaries
  over one requested window. Cases are explicit strategy/instrument selections
  so cross-symbol and cross-variant research does not depend on hidden defaults.

The split read surface is for agents, CLI, MCP, and UI inspection. Runtime truth
still comes from compiled strategy specs and run snapshots, not from frontend
state or ad hoc route joins.

The full preview artifact remains the inspection surface. CLI and agent
workflows should default to `strategy_preview_summary.v1`; consumers should ask
for the full preview only when they need machine decisions, overlays, or signal
audit detail. Preview summaries and comparisons must be derived from the same
walk-forward preview artifact and must not re-run rules through another path.

## Failure And Recovery

- Missing typed outputs make dependent rules false or rejected with context.
- Invalid strategy specs fail at compile/load time.
- Runtime rejections should include explicit reason codes and blocking context.
- Strategy previews must not use a different semantic path than runtime decisions.
- Preview summaries and comparisons must not become a second strategy
  evaluator; they are read models over canonical preview results.

## Invariants

- Signals are decision-layer inputs, not a separate architecture boundary.
- Strategies read typed outputs, not indicator internals.
- Runtime decision provenance captures only rule-referenced typed outputs at the decision boundary; reporting must not reconstruct indicator context from hidden state.
- `strategy_hash` travels with decisions for reproducibility.
- Variant resolution is shared across preview, bot config, runtime loading, and
  report metadata. A selected variant must not have one effective param map in
  preview and another at runtime.
- Agent-facing readers must use split strategy read contracts instead of
  bundled detail payloads when they only need inventory, bindings, rules,
  variants, effective config, or decision inputs.
- Output-filter traces are derived from the same guard evaluation result used
  by the decision. They must not trigger a second rule evaluation path.
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
