# Overview

Quant-Trad is a quantitative trading platform for research, strategy evaluation, execution realism, and runtime inspection.

The project is built around one rule: behavior must be explainable from one walk-forward runtime timeline. If an indicator value, strategy decision, trade, report metric, or BotLens overlay would not have been known yet in live trading, it should not appear early in the system.

## System Model

```text
Data -> Indicators -> Signals -> Decisions -> Execution -> BotLens / Reports
```

- Data providers supply candles and market context.
- Indicators turn observed market data into typed outputs, overlays, and optional debug details.
- Strategies consume typed outputs only and produce decision artifacts.
- Bot runtime owns execution semantics, fills, fees, margin, wallet state, settlement, and lifecycle outcomes.
- BotLens inspects runtime facts for debugging and playback.
- Reports and RunResearchDataset expose durable run-level research views.

## Layer Ownership

- QuantLab is research and indicator exploration.
- Strategy is decision logic.
- Bot runtime is execution realism and lifecycle truth.
- BotLens is inspection and debugging.
- Reports are views over canonical run data.

## What Quant-Trad Is Not

Quant-Trad is not a promise that every provider, indicator, or dashboard is finished. It is an active-development system with strong contracts around runtime semantics and a growing set of implementation surfaces.

## Next

- Read [getting started](getting-started.md) to run the stack.
- Read [runtime timeline](concepts/runtime-timeline.md) for the core mental model.
- Read [execution model](concepts/execution-model.md) before comparing run results.
- Use [contracts](contracts/README.md) when behavior needs source-of-truth detail.
