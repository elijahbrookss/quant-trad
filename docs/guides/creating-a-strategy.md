# Creating A Strategy

This is a minimal guide for strategy development. It does not document every rule type or UI field.

## Where Strategy Code Lives

Strategy contracts and evaluation live under `src/strategies/`:

- [contracts](../../src/strategies/contracts.py)
- [compiler](../../src/strategies/compiler.py)
- [evaluator](../../src/strategies/evaluator.py)
- [templates](../../src/strategies/template.py)

## Strategy Template Idea

A strategy defines rules and parameters that consume typed indicator outputs. It should describe decision logic, not execution mechanics.

Execution behavior such as fills, fees, margin, wallet state, settlement, and FAST/FULL resolution belongs to Bot runtime.

## Decision Flow

At runtime:

1. Indicators publish typed outputs.
2. The strategy evaluator checks rules and guards against those outputs.
3. The evaluator produces decision artifacts.
4. Runtime accepts or rejects decisions with explicit reasons.
5. Accepted decisions move into execution.

## Typed Outputs

Strategies should use:

- `context` outputs for categorical/state checks,
- `metric` outputs for numeric comparisons,
- `signal` outputs for event-window logic.

Do not inspect indicator overlays or mutable internals from a strategy.

## Testing Expectations

Add focused tests for:

- compile-time validation,
- rule and guard behavior,
- accepted and rejected decision artifacts,
- output history/window behavior,
- runtime integration when a strategy is consumed by bot execution.

Useful examples:

- [strategy evaluator tests](../../tests/test_strategies/test_strategy_evaluator.py)
- [strategy compiler tests](../../tests/test_strategies/test_strategy_compiler_params.py)
- [strategy portal tests](../../tests/test_portal/test_strategy_compile_contract.py)

## Next

- Concept overview: [strategies and signals](../concepts/strategies-and-signals.md).
- Architecture boundary: [decision layer boundary](../architecture/decision-layer/DECISION_LAYER_BOUNDARY.md).
- Runtime source of truth: [runtime contract](../contracts/platform/01_runtime_contract.md).
