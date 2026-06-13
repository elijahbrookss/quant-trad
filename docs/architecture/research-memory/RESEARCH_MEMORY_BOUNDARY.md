---
component: research-memory-boundary
subsystem: research-memory
layer: boundary
doc_type: architecture
status: active
tags:
  - research
  - memory
  - observations
  - hypotheses
  - studies
  - checks
code_paths:
  - portal/backend/controller/research.py
  - portal/backend/service/research
  - portal/backend/service/indicators/indicator_service/runtime_validation.py
  - portal/backend/service/reports/contract.py
  - portal/backend/db/models.py
  - portal/backend/db/session.py
  - cli/main.py
  - scripts/db/manual_migration_research_memory_v1.sql
---
# Research Memory Boundary

## Purpose

Research memory captures why Quant-Trad users care about an idea and what the
system has already learned about it. It stores observations, lightweight
research checks, hypotheses, studies, and links to existing platform artifacts.

Research memory is not runtime truth. It is the reasoning trail around runtime,
data, indicator, experiment, report, and strategy artifacts.

## Boundary Contract

Research memory may:

- store observations, hypotheses, studies, and research checks,
- link research items to strategies, variants, indicators, instruments, runs,
  reports, experiments, and other research items,
- request source facts through the existing data boundary,
- run bounded analytical checks over source candles,
- run bounded analytical checks over persisted indicator outputs collected
  through the canonical runtime graph,
- run bounded signal audits that reconcile declared expectations against
  emitted indicator signal events using public typed output rows,
- run bounded analytical checks over canonical report datasets,
- persist check outputs as evidence items,
- recommend whether an observation should be discarded, refined, or promoted to
  a hypothesis.

Research memory must not:

- execute trades,
- simulate fills, fees, wallet, margin, slippage, or settlement,
- become a strategy engine,
- read mutable indicator internals,
- treat overlays or debug details as strategy evidence,
- fetch provider data outside existing data-boundary services,
- reconstruct report truth or runtime truth.

## Research Check Semantics

A research check is a bounded analytical run that asks:

```text
When this condition appeared historically, what happened afterward?
```

The raw source check runner is intentionally boring:

1. normalize the check request,
2. resolve the canonical instrument through the instrument/data boundary,
3. run candle coverage preflight,
4. fetch source candles through the candle service,
5. detect occurrences with known-at raw OHLCV and previous-bar OHLCV,
6. measure forward analytical outcomes,
7. ensure the check is attached to an observation, creating an ad hoc
   observation with the normalized scope when needed,
8. persist the check result as a research-memory item,
9. link the check back to the observation.

The raw source check family is `raw_forward_outcome`. It supports detector
trees over known-at source fields only: `open`, `high`, `low`, `close`,
`volume`, and their `previous_*` counterparts. It intentionally does not derive
candle stats such as body size, wick size, range percentage, or close position.
Those meanings belong to indicators when they prove useful.

The indicator check family is `indicator_forward_outcome`. It requires a
persisted `indicator_id`, collects declared typed outputs through the backend
indicator runtime graph, matches metric/context fields or signal events, then
measures the same forward analytical outcomes over the aligned source candles.
It does not create ephemeral indicator params or inspect overlays, details, or
mutable indicator internals.

The signal audit check family is `signal_audit`. It also requires a persisted
`indicator_id` and the canonical indicator runtime graph, but it answers a
different question:

```text
Given public output fields that imply a signal should exist, did the matching
signal event actually emit on the expected bar?
```

Signal audits are indicator-agnostic. The research layer does not import an
indicator family or encode family-specific event names. The caller supplies one
or more expectations over public typed output rows. A transition expectation
names a source output, a source field, a `from` value, a `to` value, the
expected signal output, and the expected event key. Optional `same_group_by`
fields require a transition to remain inside the same caller-defined grouping
before it counts as expected. Group changes are reported as excluded
candidates, not silently discarded, so signal contracts can be challenged with
evidence.

For example, a balance-breakout-style audit can be expressed as:

```json
{
  "type": "signal_audit",
  "source_output": "value_location",
  "source_field": "state_key",
  "from": "inside_value",
  "to": "above_value",
  "same_group_by": ["active_profile_key"],
  "signal_output": "balance_breakout",
  "event_key": "balance_breakout_long"
}
```

That request shape is generic: another indicator can use the same audit family
with different output names, fields, groups, and event keys. Signal audits
produce matched, missing expected, invalid emitted, and excluded candidate
counts. They do not measure profitability; forward outcome checks remain the
surface for that.

The candidate lifecycle check family is `candidate_lifecycle`. It requires a
persisted `indicator_id` and the canonical indicator runtime graph. It reads
public lifecycle typed output rows and summarizes candidate/setup funnels
without understanding the indicator family that produced them.

Lifecycle outputs are optional. They are for stateful or sequence-based signals
that have a meaningful pre-signal path. Simple one-bar signals do not need to
emit lifecycle facts. A lifecycle event should expose generic fields such as:

```json
{
  "candidate_id": "stable candidate identity",
  "family": "retest",
  "side": "long",
  "stage": "eligible",
  "status": "active",
  "group_key": "reference object identity",
  "source_event_id": "upstream event identity",
  "source_output": "balance_breakout",
  "source_event_key": "balance_breakout_long",
  "signal_output": "entry",
  "signal_event_key": "entry_long",
  "known_at": 1767229200,
  "reason": "threshold_met",
  "reference": {"kind": "price_level", "name": "reference", "price": 100.0},
  "metrics": {},
  "thresholds": {}
}
```

The research check groups lifecycle rows by `candidate_id`, counts stage
funnels, terminal outcomes, reasons, family/side buckets, and open candidates.
When a lifecycle stage declares a `signal_output` and `signal_event_key`, the
check reconciles that candidate against emitted signal events using the same
runtime evidence. This lets research distinguish "no candidate existed",
"candidate was filtered or expired", "candidate confirmed and emitted", and
"candidate confirmed but the signal did not emit" without reading indicator
private state.

Report-backed check families read `RunResearchDataset` through the reporting
contract. `run_signal_summary` counts matching signals, buckets them by
requested fields, and summarizes linked decision/trade presence. `run_decision_trade_comparison`
summarizes matching decisions by decision state and linked trade PnL. These
families do not replay runtime or rebuild indicator state; they mine completed
report evidence. Completed run datasets may also expose `candidate_lifecycle`
rows from lifecycle typed outputs captured in report artifacts, so future
report-backed checks can inspect setup funnels without replaying indicators.

Report-backed checks hydrate run context from `RunResearchDataset` before
creating ad hoc observations. Caller-supplied observations are linked as-is.
Auto-created observations inherit the analyzed run id, bot id, strategy id,
symbols, timeframe, and simulated window when those fields are available.

Failure semantics are intentionally narrow. Missing or blocked source/report
evidence may be stored as a blocked check result because that is valid research
evidence. Unsupported check families, malformed detectors, unsupported detector
operators, or internal contract errors fail loud and do not create research
items.

Future report-candle joined checks may reuse this request and persistence shape,
but they must keep the same boundary: analytical evidence only, not execution
truth.

## Research Metric Presentation

Research-memory presentation surfaces, including compact comparisons and future
leaderboard-style CLI/API views, are generic read models over emitted research
metrics. They are not indicator-specific analysis layers.

A presentation surface may group, rank, and display check outputs only through
explicit metric and dimension contracts. The rank metric must be supplied by
the caller or declared by the producing check contract. Comparable metrics
should carry enough semantics for generic handling: name, value, optional unit,
optional direction, role, dimensions, sample count, caveats, and provenance.

Presentation surfaces must not import indicator-family code, hardcode
family-specific meanings, infer signal quality from labels alone, or choose a
fallback rank metric when intent is missing. Missing rank keys, metric
directions, grouping fields, or required dimensions fail loud before presenting
misleading evidence.

## Memory Graph

The storage model is intentionally small:

- `portal_research_items` stores observations, research checks, hypotheses, and
  studies.
- `portal_research_links` stores directed links from a research item to another
  research item or platform artifact.

Every research check is a research item. A check must link to an observation.
If the caller does not supply an observation, the service creates an ad hoc
observation after the check scope has been normalized or report context has
been hydrated, so analytical work is never orphaned or stripped of available
context. Report-backed checks also link to the analyzed run so later research
can traverse observation -> check -> run without scraping reports.

Useful relations include:

- `tests`
- `derived_from`
- `supported_by`
- `contradicted_by`
- `promoted_to`
- `validated_by`

## Invariants

- Research memory stores reasoning and evidence; it does not certify execution
  truth.
- Research checks may request evidence through existing boundaries, but they do
  not own provider access or alternate candle caches.
- Check occurrence detection must use only data known at the occurrence bar.
- Raw checks must stay raw; candle-derived meanings belong to persisted
  indicator outputs.
- Indicator checks must use persisted indicator instances and the canonical
  runtime graph.
- Signal audits must be expressed as expectations over public typed output
  rows; they must not import indicator-family code or inspect mutable indicator
  internals.
- Forward outcomes are analytical summaries, not simulated trades.
- Research metric presentation must be driven by emitted metric contracts and
  explicit rank intent; it must not contain indicator-family logic or hidden
  fallback ranking.
- Report-backed checks must read `RunResearchDataset` and must not reconstruct
  runtime state from logs, frontend projections, or indicator internals.
- Report-backed auto observations must be created after the dataset is read so
  they inherit run/report context.
- Unsupported detector semantics must fail loud before any new research item is
  created.
- Check outputs must preserve data quality, sample counts, caveats,
  provenance, and recommendation.
- Reports and experiments remain the validation surfaces for executable
  strategies.

## Related Docs

- [Research orchestration boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
- [Data boundary](../data/DATA_BOUNDARY.md)
- [Indicator runtime boundary](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [ADR 0034: Use Research Checks as Analytical Memory Evidence](../decisions/0034-use-research-checks-as-analytical-memory-evidence.md)
- [ADR 0037: Keep Research Presentations Metric-Contract Driven](../decisions/0037-keep-research-presentations-metric-contract-driven.md)
