# Strategies And Signals

Strategies turn typed indicator outputs into decision artifacts. They do not own execution semantics.

## What It Is

Indicators publish typed outputs such as:

- `context`: categorical or state-like facts.
- `metric`: numeric values.
- `signal`: event-like outputs with event keys.

Strategies consume these outputs with rules, guards, parameters, and runtime context.

## Decision Flow

At each evaluated bar:

1. Indicator runtime publishes typed outputs for that bar.
2. Strategy rules evaluate against those outputs and recent output history when required.
3. The strategy emits decision artifacts.
4. Runtime accepts or rejects decisions based on rule resolution, position state, wallet/risk constraints, and execution policy.
5. Accepted decisions can become orders/trades. Rejected decisions remain inspectable artifacts with reasons.

## Boundaries

Strategies must not inspect indicator internals, overlays, or debug details. Overlays are visual/debug surfaces. Runtime details are inspection surfaces. Typed outputs are the strategy contract.

Decision artifacts should be preserved because "no trade" is often as important as "trade". A rejected decision explains why the system did not act.

## How It Fits

This layer sits between indicators and bot execution:

```text
Indicator outputs -> Strategy rules -> Decision artifacts -> Runtime execution
```

## Next

- Deep design: [decision layer boundary](../architecture/decision-layer/DECISION_LAYER_BOUNDARY.md).
- Signal context: [regime context boundary](../architecture/decision-layer/REGIME_CONTEXT_BOUNDARY.md).
- Indicator contract: [indicator runtime boundary](../architecture/indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md).
- Minimal extension guide: [creating a strategy](../guides/creating-a-strategy.md).
