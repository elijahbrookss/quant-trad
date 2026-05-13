---
component: adr-strategy-decisions-separate-from-execution
subsystem: decision-layer
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - strategy
  - signals
  - decision
  - execution
code_paths:
  - src/strategies
  - src/strategies/evaluator.py
  - src/engines/bot_runtime/strategy
  - portal/backend/service/strategies
  - docs/architecture/decision-layer/DECISION_LAYER_BOUNDARY.md
---
# ADR 0005: Keep Strategy Decisions Separate From Execution

## Status

Accepted, backfilled on 2026-05-13.

## Context

The platform separates research, decision logic, and execution realism.
Strategies should decide from indicator outputs, but fills, fees, wallet,
margin, and settlement depend on runtime conditions that strategy logic does not
own.

Signals also used to look like they could become a separate subsystem. In the
current model, they are typed indicator outputs consumed by strategy rules and
preserved as decision provenance.

## Decision

The decision layer owns strategy compilation, rule and guard evaluation, signal
consumption, bounded output history, and decision artifacts. Execution runtime
owns whether a decision can become a trade and how fills, wallet, fees, margin,
and settlement occur.

Rejected decisions are explicit artifacts with reasons, not missing data.

## Consequences

- A valid strategy decision can still be rejected by runtime state.
- Strategy provenance records `strategy_id`, `strategy_hash`, rule context,
  `decision_id`, and referenced typed outputs.
- `signal_id` and `decision_id` stay distinct.
- Reports and BotLens can explain both accepted and rejected decisions without
  re-evaluating hidden strategy or indicator internals.

## References

- [Decision layer boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)
- [Regime context boundary](../decision-layer/REGIME_CONTEXT_BOUNDARY.md)
- [System contract](../../contracts/platform/00_system_contract.md)
- [Strategies and signals concept](../../concepts/strategies-and-signals.md)

