# Quant-Trad Documentation

Quant-Trad (QT) helps you turn a trading idea into repeatable evidence before
you trust it with real decisions. You define the idea, bind it to known data,
measure it, test it in time order, and inspect what happened.

You do not need to learn QT's internal vocabulary before getting value from
the system. Start with the reading path that matches what you want to do.

For a first visit, use this order:

[README](../README.md) → [What is QT?](overview.md) →
[Quick Start](getting-started.md) →
[core research workflow](guides/research-workflow.md) →
[concepts](concepts/) / [glossary](contracts/platform/04_glossary.md) →
[architecture](architecture/README.md) → [six core promises](core-promises.md)
→ contributor guidance when you are ready to change the system.

## Choose Your Path

### I want to understand QT

1. [Overview](overview.md) — what quantitative trading means here, who QT
   helps, and what a result can and cannot tell you.
2. [Six core promises](core-promises.md) — the short list of system properties
   that make QT research worth trusting.
3. [Current system](current-system.md) — what is implemented now, what remains
   bounded, and what QT deliberately does not claim.

### I have a trading idea to test

1. [Core research workflow](guides/research-workflow.md) — idea, definition,
   evidence, measurements, Check, Strategy, backtest, walk-forward review,
   paper simulation, and comparison.
2. [Getting started](getting-started.md) — install QT, start the local stack,
   and verify that it is ready.
3. [Creating an indicator](guides/creating-an-indicator.md) and
   [creating a strategy](guides/creating-a-strategy.md) — extend the system
   after the normal research path makes sense.

### I operate QT

- [Operator handbook](operators/README.md) — deployment, updates, provider
  onboarding, collector operation, backup, recovery, and node boundaries.
- [Collector operations](guides/collector-operations.md) — enroll supported
  products and inspect or operate the collector fleet.
- [Portable single-node deployment](engineering/server-deployment.md) —
  install, verify, promote, roll back, and move storage.

### I need the internals or exact rule

- [Architecture guide](architecture/README.md) — the boundary-oriented map of
  data, research, execution, persistence, reporting, and inspection.
- [System architecture model](architecture/system/SYSTEM_MODEL.md) — the
  end-to-end truth flow.
- [Engineering standards](engineering/README.md) — implementation, testing,
  CI, observability, and documentation guidance.
- [Platform contracts](contracts/README.md) — the normative rules that win
  when explanatory prose or code disagrees.
- [Platform glossary](contracts/platform/04_glossary.md) — precise definitions
  when a QT term matters.

## The Research Path In One Minute

```text
idea
  -> define the rule and failure criteria
  -> choose and freeze the evidence
  -> calculate measurements
  -> ask a bounded research question
  -> express a Strategy
  -> backtest in time order
  -> repeat across chronological windows
  -> observe with paper data
  -> inspect, compare, refine, or reject
```

A frozen **Dataset** identifies the exact market evidence used by a piece of
research. An **Indicator** produces a measurement. A **Check** asks a bounded
question of evidence. A **Strategy** turns declared measurements into decisions.
The bot runtime—not the Strategy—simulates fills, fees, wallet effects, and
lifecycle. BotLens and reports explain the result; they do not rewrite it.

## Backtest And Paper Simulation

A backtest replays frozen historical evidence in time order. It prevents a
decision from seeing future data and applies declared execution assumptions.
It answers “what would this rule have done under this evidence and model?” It
does not answer “what will happen next?”

A paper run exercises the system as data arrives, using simulated or
observe-only behavior. It is useful for timing, data-readiness, and operational
learning. QT does not submit external exchange orders.

## Concept Pages

Read these after the main workflow when a result is surprising:

- [Runtime timeline](concepts/runtime-timeline.md) —
  `initialize -> apply_bar -> snapshot`, known-at timing, and runtime truth.
- [Execution model](concepts/execution-model.md) — fill assumptions, same-bar
  handling, and playback separation.
- [Strategies and signals](concepts/strategies-and-signals.md) — how typed
  measurements become decisions.
- [BotLens](concepts/botlens.md) — inspection and playback as views, not an
  execution engine.
- [Reporting datasets](concepts/reporting-datasets.md) — report, export, and
  comparison views.

## Additional Guides

- [Adding a provider](guides/adding-a-provider.md)
- [Chainlink structured Facts](guides/chainlink-structured-facts.md)
- [Coinbase derivatives paper setup](guides/coinbase-derivatives-paper-setup.md)
- [Binance futures public data setup](guides/binance-futures-public-data.md)
- [Developer workflow](engineering/developer-workflow.md)

## Documentation Rule

Start with intent, then follow links to the exact source. Contracts remain the
source of truth. Architecture explains boundaries. Tests and implementation
show current conformance. Missing or unavailable support should stay visible
rather than being presented as a successful result.
