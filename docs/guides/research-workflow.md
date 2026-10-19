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

## Research Purpose And Stopping Decisions

QT currently develops revisable, inspectable understanding of market behavior.
Eventual trading usefulness remains downstream; research promises no profitable
Strategy. A useful result may explain a limitation or close a question without
advancing toward trading.

Ask a specific question and compare plausible alternatives. Preserve supporting
and contradicting evidence, the conditions tested, and limitations. Historical
comparisons do not establish causation. Post-hoc regime splits remain exploratory
until tested on separate evidence. A negative result applies narrowly to the
tested formulation, rather than disproving every related idea.

Assess evidence separately from the operational decision: strong evidence may
still justify deferring work, while weak evidence may justify a bounded next
comparison. Decide to **close**, **continue**, or **defer**, with reasons and
conditions for revisiting. A better question must change a research decision;
it must not justify endless infrastructure or repeated tests without new grounds.
Reopening requires new evidence or a justified alternative.

At a Study ending, use the existing summaries and linked evidence to explain the
question, evidence, tested conditions, limitations, decision, and reopening
criteria. This is reasoning guidance, not a new entity, schema, required metadata,
or knowledge subsystem. A research-memory Study owns the question and its
interpretation; admitted Checks and scientific authority keep their existing
contracts.

Reviewed 2026-10-12: general agent research guidance; no research outcome or
operational authority is implied.

## Writing Research For Human Inspection

The portal is a reading surface; exact Check evidence remains the scientific
record. Write the human explanation as part of research work, then inspect its
actual rendering once before publication. This is an author/reviewer practice,
not a new CI gate, schema, editor or research execution requirement.

- **Title:** name the question or finding in ordinary words. Use spaces between
  quantities and units. Put campaign codes, versions and hashes in existing
  evidence/provenance fields, unless a short code materially disambiguates work.
- **Study conclusion:** lead with what was learned and the operational research
  decision. Aim for one short paragraph and, where useful, two to four supporting
  bullets. Select only the numbers needed to understand the finding and its
  limits. Do not turn the whole conclusion into a large-type technical report.
- **Scope:** describe the actual assets, native timeframe, source, dates and
  comparator. Distinguish recognition/warmup from evaluation windows and state
  end-exclusive boundaries where material. A multi-asset Study need not acquire
  a fictitious single symbol or interval just to fill a table cell.
- **Limitations:** keep qualifications that change interpretation visible next
  to the finding: insufficient support, selective populations, dependence,
  exposure, missing inputs, inferred clocks, untested outcomes or confounding.
  Do not bury these behind a disclosure or replace them with generic caveats.
- **Observations:** use existing title/body to state the retained finding and
  link its exact Check/review. Distinguish a reviewer-reported verification from
  verification actually performed here. **Hypotheses:** state a testable proposed
  relationship and comparator without implying a result. **Checks:** retain
  typed definitions, parameters, outputs, units and exact hashes; give a readable
  title, but do not rewrite their immutable evidence to simplify presentation.
- Put full precision, complete counts, exclusions, reproduction identities and
  method diagnostics in existing Check payloads, linked evidence, reports and
  explicit technical detail. Human summaries may round for readability only
  when units and the unrounded source are clear; never change an evidence value,
  gate, denominator or verdict. Do not imply independent samples or profitability.

### Formatting And Progressive Detail

Separate ideas with blank lines. Prefer short descriptive headings and genuine
lists for parallel findings, rather than a dense paragraph of comma-separated
numbers. Meaning comes first; supporting numbers follow; exact evidence opens
on demand. Do not rely on color, truncation or a tooltip to carry a limitation.

The portal supports a small escaped text subset in Summary, Scope, Limitations
and record bodies: blank-line paragraphs, shallow `- ` / `* ` / `• ` lists,
`## ` headings and `**strong emphasis**`. Use real newlines, not literal
backslash-n escape sequences. All qualifications remain rendered; long historical
conclusions are not automatically truncated or interpreted as clean summaries.

The renderer maps these blocks to semantic React elements with consistent
spacing. It preserves the source string and never uses raw HTML, executes embedded
content, automatically decodes escapes, parses arbitrary Markdown or turns
untrusted text into links. Evidence links remain controlled by existing route
components. Full rich text, nested markup, tables and an editor are unnecessary.

### Research Content And Operational Receipts

Human-facing title/body/conclusion/scope are not an authorization log. Exclude
user-message IDs, approval quotations, agent coordination, attempt accounting,
PR/CI receipts and local paths unless the record explicitly documents that
operational incident. Even there, lead with the failure and its consequence;
put exact mechanics in existing evidence/receipt detail. Keep method provenance
that changes scientific interpretation: fixed/predeclared rules, actual source
and Dataset identity, missing data, replay status, and known-at assumptions.

Retain approvals in existing custody/operator receipts and actual audit facilities
that already own the operation. A publication has no arbitrary authorization field;
do not invent one or misuse scientific-authority audit events. Link existing
operational evidence when appropriate; never copy an entire permission transcript
into a scientific conclusion. Presentation choices are not access controls:
collapsing a section does not make private content private. Exports and historical
versions retain their existing access rules and can still contain earlier prose.

### One-Time Review And Historical Correction

Read the title and first screen without opening technical detail. Confirm that a
reader can identify the question, answer/decision, scope and central limitation.
Inspect desktop and phone paragraphs, list spacing, long identifiers, units and
empty states. Check that each displayed “not recorded”, “not applicable”, “multiple”
or “unresolved” label describes the actual condition; never infer scope from prose
or a campaign code automatically. Use existing authoritative instrument lookup
when an explicit instrument ID is available, and preserve the ID/provenance.

Before correcting history, inventory exact record IDs, selected revisions/hashes,
incoming references and fields. Changing title/body/metadata changes reasoning
content identity. Preserve original records and frozen snapshots. Use the existing append-only publication revision with `expected_previous_hash`
for a published narrative correction; retain unchanged exact references when only
the Study prose changes. Create a corrected Observation only if that referenced
Observation also needs correction, as in the audited B2 case. Pin unchanged
Check/evidence identities and retain previous versions. Do not silently edit an
old conclusion, backfill metadata, alter a question adopted immutably, or treat a
presentation correction as permission to rerun research. A new version alone does
not remove old prose from History/export; any actual confidentiality issue needs
separate scoped access/remediation review.


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

For new candle inputs, acquire provider-native candles at the requested
timeframe and freeze them directly. Do not coarsen, split, upsample or synthesize
candles from other timeframes, even when the arithmetic appears equivalent.
Provider pagination may divide requests in time while preserving the native
timeframe. Missing native coverage stays missing; it is not filled from another
interval. Preserve earlier derived inputs/results with their original lineage
for historical custody/replay; do not rewrite or relabel them as provider-native.

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

Discover an implemented method before writing a request:

```bash
qt research check definitions
qt research check definition event_fact_analysis --version 11
```

The list identifies registered base versions and eligibility. Detail returns the
method's purpose, required inputs, fixed and configurable settings, result
schema and a request example. Replace the example's resource bindings with
accessible server instrument/indicator/source IDs. Then use the ordinary
requirements and preparation commands below; catalogue examples do not grant
execution permission or claim those resources exist. Preparation returns
`next_request` for an evidence run after a Dataset has been frozen.

Definition 11 is deliberately a fixed Candle Stats ATR-crossing comparison.
It exposes its pinned indicator parameters, horizons and matching constraints;
it does not accept another indicator or a new matching threshold as a setting.
Other supported methods can express different questions through their existing
parameters. A genuinely new calculation is an ordinary reviewed Python method;
see [extending analytical Checks](#extending-analytical-checks).

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

Preparation is inspect-only when neither write flag is supplied. If native
candles are missing and their acquisition is authorized, use the same operation:

```bash
qt research check prepare --request-json <request.json> --acquire-missing --freeze
```

This resolves all declared primary and transitive ranges, refuses unsupported
Facts/providers/source bindings before fetching, calls the existing public
native-candle ingestor, checks coverage again and freezes exact canonical
inputs. Coinbase currently declares the supported native intervals; no other
interval is derived or filled. It never executes the Check. Review the returned
`next_request` and fresh admission before the separately authorized run.
`--acquire-missing` without `--freeze` acquires and rechecks only. Existing
`qt data ingest-candles`, coverage and freeze operations remain available for
explicit data work and provenance diagnosis; they are not mandatory manual
steps for supported research preparation.

Retain the complete preparation receipt, including ingestion/source IDs and
partial failures. Provider failure stops without automatic retry or freeze;
earlier acquired facts and failure-gap evidence may already be durable.
Unknown effects are explicit null flags, not rollback claims. Frozen gaps remain
visible, and each consumer keeps its own readiness policy. MCP exposes the same
flags on `prepare_research_check_evidence` with `confirm=true` required for
acquisition or freeze. Embedded JSON cannot enable either effect in place of
explicit CLI/MCP flags. A Check run remains provider-free.

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

### Dispatch And Keep The Receipt

For an approved asynchronous evidence run, retain the complete machine-readable
receipt:

```bash
qt research check run \
  --request-json <request.json> \
  --dataset-id <dataset_id> \
  --dispatch --single-attempt --format json
```

`--single-attempt` requires `--dispatch` and selects the distinct run-once API;
an older server must reject that route rather than silently use another policy.
Omitting the flag preserves normal dispatch. The matching MCP tool is
`dispatch_research_check_evidence` with `request`, optional `dataset_id`,
`single_attempt: true`, and `confirm: true`; its omitted/default
`single_attempt` is false. Both adapters use the existing
`ResearchOperations.dispatch_evidence` method.

The `research_job_dispatch.v1` receipt includes the job ID/type, actual status,
`reused`, `attempts`, persisted `max_attempts`, `request_fingerprint`, and status
and result URLs. Keep those fields, including when QT reuses an in-flight job.
`--format auto` (the default) preserves the existing text dispatch receipt;
`--format text` is explicit text output and requires `--dispatch`.
Synchronous execution remains JSON and accepts `--format json`.

A dispatch error may arrive after a job was enqueued or reused. Preserve its
HTTP status/body, job ID and reconciliation instructions. MCP dispatch errors
retain that status/body in JSON-RPC error data. Inspect the existing job before
retrying; do not switch policies or redispatch automatically after a receipt
failure, policy mismatch or timeout. If the response supplied no job ID, the
outcome may remain unknown. A client timeout does not acknowledge that server
work stopped. Use the existing job status/result and explicit cancellation
contracts; requesting cancellation is not acknowledgement of stopped work.

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


## Publish what a question has taught you

Keep one Research Memory Study for a durable question and scope. Connect testable
hypotheses, admitted findings and exact Checks to it rather than creating a Study
for every stage. A Study owns interpretation; a CheckDefinition pins an admitted
procedure and its settings.

Create a draft Study through the existing item operation, then explicitly adopt
its question with `qt research question adopt <study-id> --request-json <file>`.
The request contains `question` and `scope`. Adoption preserves legacy payloads
and is not preregistration. Item reads return `content_hash` for small reasoning
records. Publish using `qt research question publish <study-id> --request-json
<file>`, with `request_id`, `expected_previous_hash`, `conclusion`, `limitations`,
`scope` and `references`. Each reference requires `item_id`, `kind`, `role` and
an exact `content_hash`, or Check `result_hash` and `evidence_hash`. Preserve
supporting, contradicting, negative and inconclusive evidence, decisions and
deviations explicitly; publication cannot detect omitted scientific context.

Read `qt research question history <study-id>` or inspect the Study in V2.
Corrections publish another revision using the prior publication hash. The
historical snapshot stays readable after later edits. Complete references mean
an interpretation is durably recorded; actual replay, recovery and scientific
authority remain separate evidence and operations.

## Extending Analytical Checks

A research question describes what we want to learn. A CheckDefinition identifies
the analytical method; its evaluator is ordinary versioned Python code, not a
new database entity. Reuse an existing method when its declared inputs and
settings express the question. Changing a symbol or supported sample does not
require another implementation.

Before adding code, distinguish four ordinary choices:

- Configure an existing method when its admitted settings express the question.
- Reuse an existing calculation when the same operation occurs in another procedure.
- Extend a method through a reviewed version when its scientific meaning changes.
- Implement a distinct procedure when selection, outcomes or inference actually differ.

These are review decisions, not a fixed taxonomy of future research. A new question
alone does not require another evaluator. Spread and book-imbalance directional
comparisons already share fitting/scoring; first-return classification and future
realized-risk analysis have distinct procedures. Volatility as a directional
predictor is a different question from measuring future volatility. Existing ATR
baseline support does not admit ATR as an enriched feature.

The event-and-fact code keeps accepted settings in `event_configuration.py` and
historical procedure selection in the registry's `_resolve_definition_version`.
`event_preparation.py` selects public events; `event_facts.py` owns causal Fact
selection/features. `event_observations.py` admits observations and entry clocks,
`event_outcomes.py` resolves horizons/invalidation, and `event_statistics.py`
orchestrates distributions, folds, bins and direct tests. Existing baseline
features and paired fitting/scoring remain in `event_features.py` and
`directional_model.py`.

Distinct procedures retain their own attribution/classification orchestration.
They share readonly candle/index/ATR preparation only within one owned call on
unchanged inputs; causal selections and shifted outcomes still execute for each
pass. Public evaluations start fresh. Fixed protocol choices remain fixed, and
cohesion remains an agent/reviewer responsibility without a scanning system.

For a new method, implement the existing `CheckEvaluator` interface in
`src/research_science/check.py`: `declare_requirements(definition, request)`
describes needed evidence and `evaluate(plan, inputs)` computes the result from
the resolved, owned inputs. Small pure functions can implement or share the
calculation. Consume the canonical snapshots/Fact inputs supplied by execution;
do not independently reconstruct an Indicator or read future data through a
second engine.

Register the evaluator ID/version and immutable CheckDefinition in the existing
research registry. Put settings validation in that method's existing normalizer
and wire explicit selection in `materialize_check_definition`. Changed scientific
semantics need a new version; preserve old registrations for historical replay.
Add a description and examples to the code-backed catalogue, keeping its
metadata outside `material_rules` so documentation edits cannot change old
definition hashes. The catalogue must cover every registered definition and
label historical or preview-only methods honestly.

Tests should cover deterministic results, known-at and missing-data boundaries,
requirements, cancellation/resource limits, exact-version replay, and rejection
of unsupported settings. Catalogue tests should verify that examples select the
stated version through the real normalizer and reach the existing prepare
contract, with API/CLI parity. Frozen data, scientific admission and accounting
remain with their existing owners. There is no plugin installer, expression
language, saved configuration resource or independent evaluator lifecycle.

### Raw candle predicates with durable evidence

Discover `raw_forward_outcome` version 3 through `qt research check definition
raw_forward_outcome --version 3`. Select it explicitly with
`outcomes.schema_version: raw_candle_forward_outcome.v1` and use the existing
requirements, prepare, frozen evidence and replay workflow. Version 2 remains
preview-only. V3 requires canonical clocks and source identity, real contiguous
entry/outcome paths, and genuine volume where used. Its entry is a causal candle
close; targets end strictly before the declared evaluation boundary. Use
`continue_degraded` and inspect local exclusions and complete support counts.

The canonical `mean_forward_return_pct` and related `_pct` fields are fractions;
percentage display multiplies by 100. Compare requested cohort summaries as
emitted; the all-bar baseline edge is a different comparison. Method availability
and successful deployment do not allocate a scientific experiment or permit
tuning, more dates, extra Checks, or external order submission.


Use `qt research check definition raw_forward_outcome --version 5` for the
configurable controlled-reversal template. Keep physical windows fixed when
changing interval: the three-hour decline/outcome and 24-hour RMS choice uses
12/96/12 bars at 15m, 6/48/6 at 30m and 3/24/3 at 1h, with warmup 97/49/25.
That expresses supported variations without implementing another method. It
does not authorize new studies or choose a timeframe from favorable results.

Use `qt research check definition event_fact_analysis --version 13` for explicit
retrospective numeric association. Declare the real analysis cutoff; preserve
late receipts. The July 25–September 23 exclusive hourly example has 1,440
nominal buckets, separate long/short liquidation inputs and one fixed paired
logistic comparison beyond recent return and ATR. Its dates and train/validation
split are a template to fix before dispatch, not an allocation or measured
coverage claim. Results are conditional on unverified bucket-start meaning and
cannot supply point-in-time predictive, backtest or certification evidence.


For an explicitly bounded complete-case comparison, add
`outcomes.retrospective.input_coverage_policy: complete_paired_rows` to v13.
It admits acquired partial source ranges without relabeling them as complete,
requires liquidation buckets only within the fixed dates, and does not acquire
prices beyond the exclusive end. Missing values on either side and unavailable
boundary outcomes are excluded and counted; returned zeros remain values.
Inspect both fold counts and the retained source-quality evidence. Omitting the
option preserves the historical strict input plan. Preparation and execution
still use the existing freeze/run operations; do not backdate receipts or repair
missing buckets with invented zero values.


For the user-selected completed-reversal comparison feasibility question,
discover `raw_forward_outcome` version11 and read
[selection only](reversal-selection-only.md). This fixed84-day method reports
matched/excluded populations, completion-moment weights and dependence without
forward outcomes, arbitrary coverage gates or repeated matching adjustments.
It does not acquire data or relax canonical derivation resource limits.


## Reusing Check recipes

Start with `qt research check definitions` and
`qt research check definition <family> --version <version>`. The same read-only
catalog is available through MCP `list_research_check_definitions` and
`get_research_check_definition`. Settings/examples and returned-field metadata
come from the existing backend catalog; discovery never prepares data or runs
science. The `discovery` annotation identifies fixed historical profiles and a
preferred reusable route where one exists. Normalizers remain authoritative.

For configured reversal populations, method17 supplies outcome-blind selection
and method18 measures a validated saved population. Bind dates, symbol, original
frozen input and report populations within the published bounds instead of
copying an evaluator for a question. Keep recognition/selection identity separate
from subsequent outcome interpretation. Historical methods and their accepted
requests remain available for exact replay.

Use declared measurement units and missingness policies. Generic fractional
excursions can be negative relative to the anchor; target-relative progress
starts at zero and may exceed 100 percent or occur after failure. Historical
`_pct` field aliases retain fractional units. Extrema timing is not inferred from
an endpoint timestamp. Weighted/unweighted summaries and different accumulation
or interpolation policies are not interchangeable. Neither matched pairs nor
support components automatically provide independent observations.


### Inspect controlled-reversal excursions

For new controlled-reversal requests that need saved excursions, inspect
`raw_forward_outcome@19` and its `controlled_reversal_comparison.v3` example
through the existing definition list/detail API, CLI or MCP catalog. It uses the
same configurable matching choices as v5 and keeps the original endpoint-return
analysis. Each saved treated/control arm now carries fractional MFE/MAE and
first-attaining future-candle offset/open/close/known-at, with explicit long
entry-close anchor, horizon, status and unresolved reason. The first tied candle
wins. The entry candle is excluded; no new tail is loaded. Candle clocks cannot
establish intrabar order or a fill. Historical v4/v5 remain replayable with their
original output shape; do not retrofit old saved evidence.


### Saved selection measurements and execution observations

For a new measurement on a supported saved selection, build the exact request
from server metadata instead of copying hashes and frozen context by hand:

```bash
qt research check from-selection SAVED_CHECK_ID --profile 18 --request-only > measurement-request.json
qt research check requirements --request-json measurement-request.json
qt research jobs snapshot --request-json measurement-request.json --limit 25
```

`from-selection` reads one persisted Check and runs the existing typed binding,
manifest, native-input, scope, cardinality, weight and support validation. It
supports selection versions 12, 14, 17 and measurement profile 18 only. Optional
`--result-hash`, `--evidence-hash` and `--selection-manifest-hash` reject a changed
reference. Without `--request-only`, its JSON also includes the validated reference
and context. It does not read candles, call providers, freeze data, calculate
outcomes, allocate attempts, dispatch jobs or write a Study/ledger. Requirements
and actual execution still validate the original frozen input through their
existing paths; metadata preparation does not replace those checks.

The execution snapshot observes global queued/running/retry jobs and the actual
existing research execution lock. With a request it also lists exact dispatch
fingerprint matches, including failed and completed jobs. `--job-id JOB_ID` adds
the unchanged job-status response. The limit is 1..100 (default 25), with separate
truncation flags for inflight and matching lists. It never returns SQL or generic
database access. Its metadata queries use 5-second statement and 250 ms lock limits
under the existing 20-second read control; these are limits, not latency promises.

This is a point-in-time diagnostic, not a slot reservation, dispatch admission,
retry grant or budget decision. Request matching uses the same modeled request
and fingerprint as dispatch, including descriptive fields/defaults; it does not
claim scientific equivalence between differently worded requests. Job status is
read through the existing status operation and may be observed slightly later.
A known failed job remains failed and its consumed attempt remains recorded;
recovery uses the already authorized scope and normal fresh admission rather
than hiding or resetting it.

Existing commands already cover machine-readable dispatch, known-job status,
results and ledger reads. Prefer them over parsing human text or issuing SQL:

```bash
qt research check run --request-json measurement-request.json --dispatch --single-attempt --format json
qt research jobs status JOB_ID --json
qt research jobs result JOB_ID --format json
qt research items list --kind observation
qt research items list --kind research_check
qt research items get ITEM_ID
```

Use the established item/link operations for this session's Observation ledger;
typed authority attempt-register/attempt-complete remain the separate existing
protocol/family budget surface. No additional accounting system is introduced.
The shared operations are also exposed as read-only MCP tools
`prepare_saved_selection_measurement_request` and
`get_research_execution_snapshot` when the configured QT MCP server is available.
Their HTTP routes are `POST /api/research/checks/saved-selection/request` and
`POST /api/research/jobs/snapshot`; POST carries structured read inputs and does
not imply execution or mutation.


### Describe every completed reversal

Discover `qt research check definition raw_forward_outcome --version 20` and bind
the existing catalogue example to an exact native15m frozen Dataset. Select
`outcomes.schema_version: reversal_completed_recovery.v1`; use explicit computation
origin, one through six whole28day decision blocks and at most two named block
views. Run the normal requirements/prepare/admission/dispatch path. There is no
provider acquisition or extra outcome tail inside this method.

Version20 retains every completed formation, its causal context, inclusive
trough-to-completion candle duration N and unchanged12bar first-passage label.
Each view reports all four labels separately with original all-case clock mass,
observed/missing N mass and count, conditional weighted mean/CDF quantiles, and
context missingness. It performs no matching or context-adjusted comparison.
These descriptive summaries do not establish incremental prediction or independent
samples. An existing Dataset's metadata is not proof of a newly frozen subset:
validate exact source, slots, revisions, clocks and material/provenance, comparing
the retained original slice if claiming unchanged historical input. Freeze pins
its actual current watermark; a metadata reference cannot replace that binding.
