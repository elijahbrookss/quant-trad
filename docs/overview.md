# Overview

Quant-Trad is a quantitative trading platform for research, strategy evaluation, execution realism, and runtime inspection.

The project is built around one rule: behavior must be explainable from one
walk-forward runtime timeline. If an indicator value, strategy decision, trade,
report metric, or BotLens overlay would not have been known yet in live trading,
it should not appear early in the system.

That rule is what gives the rest of the system its shape. It keeps research,
preview, execution, inspection, and reporting from becoming five slightly
different stories about the same run.

```text
Data -> Indicators -> Decisions -> Execution -> Events -> BotLens / Reports
```

Data providers supply candles and market context. Indicators turn observed
market data into typed outputs, plus optional overlays and debug details.
Strategies consume the typed outputs and produce decision artifacts. Bot runtime
owns execution semantics, fills, fees, margin, wallet state, settlement,
lifecycle, and runtime events. BotLens and reports inspect those facts after the
runtime has created them.

## Layer Ownership

QuantLab is for research and indicator exploration. Strategy code decides from
typed indicator outputs. Bot runtime decides whether those decisions become
orders, fills, wallet changes, lifecycle rows, and durable facts.

BotLens is allowed to be late, unavailable, or partial because it is a
projection. It is not allowed to invent execution truth. Reports can summarize,
compare, and export run data, but they do not reinterpret how the run executed.

## What Quant-Trad Is Not

Quant-Trad is not a promise that every provider, indicator, or dashboard is
finished. It is an active-development system with strong contracts around
runtime semantics and a growing set of implementation surfaces.

The docs should make that distinction clear. Missing support is a caveat.
Behavior that violates the runtime model is a bug.

## Next

- Read [getting started](getting-started.md) to run the stack.
- Read [runtime timeline](concepts/runtime-timeline.md) for the core mental model.
- Read [execution model](concepts/execution-model.md) before comparing run results.
- Use [contracts](contracts/README.md) when behavior needs source-of-truth detail.
