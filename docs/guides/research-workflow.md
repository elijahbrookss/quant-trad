# From Trading Idea To Research Evidence

This guide is the shortest complete path through QT. It is for someone who has
a market idea and wants to learn whether the idea deserves more work.

The path is deliberately ordered:

```text
idea
  -> define
  -> evidence
  -> measurements
  -> Check
  -> optional Research Observation
  -> Strategy
  -> backtest
  -> walk-forward review
  -> paper simulation or observation
  -> inspect and compare
```

Skipping a step usually makes the final number easier to produce and harder to
trust.

## 1. Begin With A Falsifiable Idea

Start with a sentence about observable market behavior, not a Strategy name.

Too vague:

> Buy when the market looks strong.

Testable direction:

> On a declared instrument and timeframe, after measurement A crosses threshold
> B, forward return over horizon C should exceed a stated baseline often enough
> to remain interesting after costs.

Before looking at results, write down:

- instrument, venue context, and timeframe;
- the event or market condition;
- what is measured and when it becomes known;
- comparison baseline and outcome horizon;
- entry and exit assumptions if the idea reaches a Strategy;
- fees, slippage, and other execution assumptions;
- the date range or chronological windows; and
- what result would weaken or reject the idea.

This definition is part of the research. It prevents a promising chart from
quietly changing the question after the answer is visible.

## 2. Choose The Evidence

QT stores market observations as typed **Facts**. A Fact keeps the value plus
the information needed to interpret it: instrument, source, contract version,
provenance, and when it was knowable.

Research and canonical backtests use a frozen **Dataset**. The Dataset identifies
the exact Facts, range, provenance, quality evidence, and gaps used by the work.
Freezing does not claim the data is perfect. It makes the evidence stable and
inspectable.

First inspect coverage and the available data surfaces:

```bash
qt data coverage \
  --instrument-id <instrument_id> \
  --timeframe <timeframe> \
  --start <iso> \
  --end <iso>

qt data series
qt data prepare-backtest-dataset --help
qt data freeze-dataset --help
qt data dataset <dataset_id>
```

Acquisition is a separate, explicit operation. A backtest or evidence replay
must not fill a missing range by contacting a provider behind your back.

Questions to answer before continuing:

- Does the Dataset cover warmup and evaluation ranges?
- Are gaps or unsupported Facts visible?
- Is the instrument identity the one the idea actually refers to?
- Would replay still work if the provider were unavailable?

## 3. Define Measurements

An **Indicator** is a repeatable measurement over available Facts. It may
describe momentum, balance, volatility, structure, or another declared market
property. It does not become predictive merely because it has a name or a
chart.

Useful measurements have:

- declared inputs and parameters;
- typed outputs with clear units and meaning;
- an explicit point at which each output becomes known;
- warmup and missing-data behavior; and
- tests showing that future data cannot change an earlier output.

List the existing measurements before creating another one:

```bash
qt indicators list
```

If a new measurement is necessary, follow
[creating an Indicator](creating-an-indicator.md). Keep visualization overlays
and debug details separate from values a Strategy may consume.

## 4. Ask A Check Before Writing A Strategy

A **Check** is a bounded analytical question over declared evidence. It is the
bridge between “this looks interesting” and “this rule has measured support.”

Use a request file so the question is reviewable and repeatable:

```bash
qt research check requirements --request-json <request.json>
qt research check preview --request-json <request.json>
qt research check prepare \
  --request-json <request.json> \
  --freeze \
  --created-by <actor> \
  --dataset-name <name>
qt research check run \
  --request-json <request.json> \
  --dataset-id <dataset_id>
```

Preview is useful for shaping a question, but it is exploratory. A durable
evidence run binds the question to frozen inputs and can later be replayed:

```bash
qt research check replay <check_id>
```

Read the result as evidence about a defined sample, not as permission to trade.
Sample count, baseline, dispersion, adverse movement, favorable movement,
cost assumptions, and chronological stability matter alongside an average
return.

If the Check does not support the idea, keep that result. A clear rejection is
valuable research evidence and protects the next experiment from repeating the
same hidden choice.

### When A Check Becomes A Research Observation

A Check and a **Research Observation** are deliberately separate. A preview is
temporary and cannot support an Observation. A durable Check first has to
finish against frozen inputs, remain replayable, and use a reviewed Check
definition that is eligible to support an Observation.

Even then, QT does not create an Observation automatically. Recording the
finding in durable research memory is a separate, explicit choice:

```bash
qt research observe-from-check <check_id> --help
```

That operation rechecks the durable evidence and links the new Observation
back to the Check that supports it. The Observation says, in effect, “retain
this evidence-backed finding and its lineage.” It does not turn the finding
into a Strategy, certify it, authorize paper or live operation, or grant
capital or order authority. The normal ceiling for a Check or Check-backed
Observation is reproducible research evidence. QT also has a separately owned
manual Observation path; it is not a fallback for a preview or an ineligible
Check, and this guide does not use it.

### Worked Example: A Breakout That May Not Survive Costs

This is an illustrative reasoning example, not a bundled request file or a
seeded result.

1. State the hypothesis: after a declared range breakout on one instrument and
   timeframe, net forward return over the next eight bars should beat the same
   instrument's unconditional eight-bar baseline.
2. Before viewing the answer, declare the breakout measurement, eight-bar
   outcome, minimum sample, chronological development and evaluation windows,
   and fee and slippage assumptions. Also declare rejection conditions, such as
   the effect disappearing outside development or after costs.
3. Use preview only to catch a malformed request, missing inputs, or an
   obviously unworkable sample. Do not cite preview numbers as durable evidence.
4. Freeze the required evidence and run the durable Check. Inspect event count,
   baseline-relative effect, dispersion, adverse and favorable movement,
   chronological stability, costs, gaps, and unresolved outcomes.
5. Suppose the aggregate return is positive, but most of it comes from one
   short interval and the evaluation window is flat after costs. The honest
   result is “interesting in development, not supported as a stable net edge.”
   Keep the Check and, if it is eligible and useful to future work, record that
   qualified or negative finding as a Research Observation.
6. Only a later, separately declared question should change the measurement,
   filters, or parameters. A Strategy and backtest become appropriate only when
   the evidence justifies testing explicit decision and execution behavior.

## 5. Express Decision Rules As A Strategy

A **Strategy** consumes declared Indicator outputs and produces decisions. It
owns questions such as:

- Is the setup present?
- Is a guard satisfied?
- Should the decision be enter, exit, or do nothing?

A Strategy does not own fills, fees, slippage, margin, wallet state, or exchange
behavior. Those belong to the runtime so different Strategies are compared
through the same execution model.

Inspect and compile before running:

```bash
qt strategies list
qt strategies compile <strategy_id>
qt strategies preview <strategy_id> \
  --start <iso> \
  --end <iso> \
  --interval <timeframe> \
  --instrument-id <instrument_id>
```

Preview helps inspect decision logic. It is not a substitute for a bot run,
because a preview does not own the full execution and accounting lifecycle.
See [creating a Strategy](creating-a-strategy.md) when you need to add one.

## 6. Run A Backtest

A **backtest** replays a frozen historical Dataset in time order. At each step,
the Strategy can use only evidence that was known then. The runtime applies the
declared execution model and records decisions, orders, fills, fees, wallet
effects, lifecycle events, and diagnostics.

```bash
qt bots list
qt bots start <bot_id> \
  --run-type backtest \
  --dataset-id <dataset_id>
qt runs wait <bot_id> <run_id>
```

A backtest answers:

> What did this exact rule do on this exact evidence under these exact
> execution assumptions?

It does not prove that fills were available at a venue, that market impact is
fully modeled, or that future behavior will resemble the sample.

## 7. Review Chronologically, Not Only In Aggregate

Every QT bot run processes its evidence walk-forward: initialize state, apply
the next bar, then publish a snapshot. Earlier outputs must not change when a
future suffix is added. That runtime chronology prevents future data from
changing the past; by itself, it is not a statistical validation protocol.

### Ordinary Chronological Review

Research also needs a broader walk-forward discipline. Avoid tuning on one
entire date range and presenting that same range as confirmation. Instead:

1. define chronological development and evaluation windows;
2. freeze the evidence and rules used for each comparison;
3. make changes using only the allowed earlier evidence;
4. evaluate the unchanged candidate on the next window; and
5. retain failures and changes instead of rewriting the earlier record.

Experiment plans can make a repeated sequence explicit and resumable:

```bash
qt experiments validate-plan <plan.json>
qt experiments run-plan <plan.json> --experiment-id <experiment_id>
qt experiments status <experiment_id>
qt experiments events <experiment_id> --tail 50
```

Do not reduce the review to one headline metric. Look for sensitivity to date
range, market state, costs, parameters, trade concentration, and missing data.
This ordinary review supports exploratory or provisional conclusions. An
experiment plan does not become a scientific protocol merely because it uses
chronological windows.

### The Stricter Scientific Path

Use QT's scientific protocol only when the intended claim requires controlled
selection, validation, a sealed final holdout, or robustness certification. In
that path, the rules are fixed before results are available:

- **train** data may influence development and tuning within a declared search
  budget;
- **validation** data evaluates candidates under fixed chronological folds and
  declared leakage controls; and
- the final **holdout** stays private and one-use until the candidate and its
  complete experiment family are frozen.

The protocol also accounts for failed and abandoned attempts, validation
feedback, minimum sample and exposure, costs, execution quality, uncertainty,
and any required stress tests. A scientific certificate can claim only the
quality level its recorded protocol and evidence earned. It still grants no
paper, live, deployment, capital, credential, or external-order authority.

Do not describe an ordinary train/test split, a good backtest, or a durable
Check as a sealed-holdout result. Conversely, do not impose the scientific path
on early idea shaping when a clearly labeled exploratory Check is enough.

## 8. Use Paper Operation For A Different Question

A paper run exercises QT while data arrives rather than replaying a completed
historical Dataset. It can reveal provider delays, readiness gaps, lifecycle
problems, and differences between research assumptions and operational timing.

The safest first paper path is observe-only:

```bash
qt bots start <bot_id> \
  --run-type paper \
  --execution observe-only \
  --duration-seconds 30
```

Observe-only paper operation does not create orders, fills, trades, fees, or
wallet mutations. Other declared paper behavior remains simulation. QT does
not submit external exchange orders.

Paper results are not automatically better evidence than a backtest. They
answer a different question: whether the data and runtime workflow behave
coherently as time advances.

## 9. Inspect And Compare

Use reports for repeatable summaries and comparisons:

```bash
qt reports summary <run_id>
qt reports diagnostics <run_id>
qt reports export <run_id>
qt reports compare <baseline_run_id> <candidate_run_id>
```

Use BotLens and the UI to inspect candles, measurements, decisions, simulated
trades, playback, accounting, and runtime diagnostics. These are views over the
recorded run. They do not replace or repair the underlying execution evidence.

For “why did it trade?” or “why was there no trade?”, use the two surfaces
together:

- **BotLens playback** shows the candle, typed measurements, selected decision,
  and simulated trade state at the moment being inspected. It is the quickest
  visual way to see what the system appeared to know then.
- The report's **Decision Trace** and context rows expose the recorded decision
  evidence, including accepted and rejected decisions and their known-at inputs.
  Report readiness and diagnostics show missing data, unavailable context,
  runtime rejection, lifecycle problems, and other caveats.

An empty trade list is not an explanation. It may mean that no setup appeared,
an Indicator was not ready, a Strategy guard rejected the setup, runtime or
risk rules rejected the decision, or an accepted order never filled. Follow
the decision rows, context, lifecycle, and diagnostics until the no-trade path
is explicit. If BotLens is stale or incomplete, treat that as a projection
diagnostic and rely on the durable report rather than inferring system truth
from an empty view.

When comparing two runs, verify more than the return:

- same or intentionally different Dataset identity;
- same instrument and chronological window;
- same Strategy and Indicator identities except for the declared change;
- same execution, fee, and exit assumptions;
- complete lifecycle and accounting reconciliation; and
- visible gaps, warnings, unavailable evidence, and zero-trade explanations.

## 10. Decide What The Evidence Justifies

### What This Result May Claim

| Result | What it may honestly say | What it may not say |
| --- | --- | --- |
| Check preview | “This exploratory calculation ran against the current store with stated provenance.” | That evidence was frozen, persisted, replayable, or eligible for an Observation. |
| Durable Check | “This bounded analytical result and verdict came from exact frozen inputs and can be replayed.” | That a Strategy is validated, scientifically certified, or authorized to trade. |
| Research Observation | “QT retained this finding in research memory with a traceable evidence link.” | That the finding is true beyond its evidence, promoted, certified, or executable. |
| Backtest | “This exact Strategy behaved this way on this frozen history under these execution assumptions.” | That venue fills were available or future/live performance will match. |
| Paper run | “The declared observe-only or simulated workflow behaved this way as data arrived.” | That external orders occurred, simulated economics were real, or profitability was validated. |
| Scientific certificate | “The frozen candidate met the exact predeclared controls and claim level named by its certificate.” | Any stronger scientific claim, or paper, live, deployment, capital, credential, or order authority. |

A responsible outcome is one of:

- **reject** — the idea did not survive its stated test;
- **refine** — a new question is justified, with the change recorded;
- **retain as provisional** — the result is interesting but needs another
  chronological window, cost model, or operational observation; or
- **compare further** — the candidate deserves a controlled comparison against
  a baseline.

No Check, backtest, report, or paper result grants live-order or capital
authority.

Keep enough identity to reproduce the reasoning:

- idea and failure criteria;
- request or experiment plan;
- Dataset ID and range;
- Indicator and Strategy identities;
- execution assumptions;
- Check, bot-run, and comparison IDs; and
- diagnostics and known limitations.

## Where To Go Deeper

- [Six core promises](../core-promises.md)
- [Current system](../current-system.md)
- [Platform glossary](../contracts/platform/04_glossary.md)
- [Runtime timeline](../concepts/runtime-timeline.md)
- [Execution model](../concepts/execution-model.md)
- [Research-memory architecture](../architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [Check evidence boundary](../architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md)
- [Scientific research authority](../architecture/research-orchestration/SCIENTIFIC_RESEARCH_AUTHORITY.md)
- [System architecture internals](../architecture/system/SYSTEM_MODEL.md)
- [Platform contracts](../contracts/README.md)
