---
component: regime-context-boundary
subsystem: decision-layer
layer: domain
doc_type: architecture
status: active
tags:
  - regime
  - strategy
  - decision
  - indicators
  - context
code_paths:
  - src/indicators/regime
  - src/strategies/evaluator.py
  - portal/backend/service/market/regime_blocks.py
  - portal/backend/service/reports/artifacts.py
---
# Regime Context Boundary

## Purpose

Regime is an indicator-produced decision context. It belongs between indicator runtime outputs and strategy guards, not in a standalone signal subsystem.

## Boundary Contract

The regime indicator owns:

- evidence collection,
- committed regime state,
- context and metric typed outputs,
- overlays for review.

The decision layer owns:

- which regime fields are required,
- how long a regime must hold,
- how confidence/strength/maturity metrics gate a decision,
- rejected decision reasons when regime context is absent or unsuitable.

## Runtime Flow

```text
candles / candle stats -> regime indicator -> context + metric outputs -> strategy guards -> decision artifact
```

Strategies should use:

- context guards for state classification,
- metric guards for confidence, conviction, strength, volatility, and maturity,
- held guards when a regime state must persist.

Strategies should not query overlay geometry, block summaries, or report helpers to make decisions.

## Projection Flow

Regime overlays are inspection views. `portal/backend/service/market/regime_blocks.py` can group states for display and reports, but it is not canonical runtime truth.

## Failure And Recovery

- Missing regime typed outputs make dependent guards false or rejected.
- Immature/unknown regime should be explicit in typed output fields.
- Missing overlays are projection/debug failures, not decision-layer evidence.

## Invariants

- Regime becomes known at a bar time like every other indicator output.
- Regime is not a prediction channel.
- Report and BotLens grouping can summarize regime history but cannot rewrite strategy evidence.

## Related Docs

- [Decision layer boundary](DECISION_LAYER_BOUNDARY.md)
- [Indicator runtime boundary](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
