# Overview

Quant-Trad (QT) is a local quantitative trading research platform. It helps you
turn a market idea into explicit rules, test those rules against identified
evidence, simulate their behavior in time order, and inspect the result.

“Quantitative” does not mean that QT finds a profitable strategy for you. It
means the idea is stated precisely enough that another run can use the same
inputs and rules and produce an explainable result.

## Who QT Helps

QT is built for:

- a trader who wants to know whether an observation survives measurement
  instead of relying on chart memory;
- a researcher who wants comparable experiments over the same evidence;
- a strategy builder who needs decisions separated from fill and accounting
  assumptions; and
- an engineer who needs data, runtime, reports, and inspection views to tell
  the same story.

The practical value is not merely a backtest score. QT keeps the evidence,
identity, timing, execution assumptions, and diagnostic trail needed to explain
where the score came from.

## From Idea To Evidence

The normal workflow is:

```text
trading idea
    -> precise definition
    -> frozen market evidence
    -> measurements
    -> bounded research question
    -> Strategy decisions
    -> backtest
    -> chronological comparison
    -> paper simulation or observation
    -> inspect, compare, refine, or reject
```

The [core research workflow](guides/research-workflow.md) explains each step and
shows the corresponding QT commands.

QT introduces a few names because they mark important boundaries:

- A **Fact** is one typed market observation with source and timing evidence.
- A frozen **Dataset** identifies the exact Facts used by research or a
  backtest.
- An **Indicator** calculates a measurement from available data.
- A **Check** asks a bounded research question and records what evidence was
  examined.
- A **Strategy** turns declared measurements into decisions. It does not own
  fills, fees, or wallet state.
- The bot runtime simulates execution and owns the resulting orders, fills,
  accounting, and lifecycle events.
- BotLens and reports let a human inspect those results without becoming a
  second source of execution truth.

Use the [glossary](contracts/platform/04_glossary.md) only when you need the
precise platform meaning of a term.

## One Timeline, No Early Knowledge

QT is built around one walk-forward rule: a result may use only information
that would have been known at that point in the run.

```text
market evidence -> measurements -> decisions -> simulated execution
                                      |
                                      v
                              events -> reports / BotLens
```

Adding future data must not rewrite an earlier decision. Reports and playback
read what the runtime produced; they do not secretly rerun a different
strategy. This is the foundation for the
[six core promises](core-promises.md).

## Backtest Versus Paper

A **backtest** replays a frozen historical Dataset in chronological order. It
uses declared models for execution, fees, wallet effects, and other trading
assumptions. It is useful for testing a rule repeatedly and comparing variants.
It is not a prediction and it cannot eliminate market-regime, model, or data
risk.

A **paper run** exercises the system while market data arrives. QT may simulate
the declared behavior or run observe-only. Paper operation can reveal timing,
readiness, provider, and lifecycle problems that a historical replay does not.
It still does not prove that live execution would behave the same way.

QT does not submit external exchange orders. Live-capital operation would
require a separately designed and reviewed boundary.

## What Owns What

- Providers acquire external data; QT converts it into canonical evidence.
- Indicators own measurements.
- Strategies own decisions.
- Runtime owns simulated execution and accounting.
- Persistence owns durable events and evidence.
- Reports, observability, and the UI own explanation and inspection views.

Keeping these roles separate prevents a chart, report, or convenience fallback
from silently changing research or execution meaning.

## What QT Does Not Promise

QT does not promise that every provider, market, Indicator, or interface is
finished. It does not turn a good historical result into investment advice. It
does not make paper performance equivalent to venue execution, and it does not
open an external-order path.

QT is in active development. Missing support is a limitation to record, not a
reason to invent evidence or silently choose a fallback.

## Next

- Follow the [core research workflow](guides/research-workflow.md) to understand
  the complete path.
- Use [getting started](getting-started.md) to run QT locally.
- Read [current system](current-system.md) for implemented scope and known
  limits.
- Read the [architecture guide](architecture/README.md) for internals.
- Use [platform contracts](contracts/README.md) when you need the exact rule.
