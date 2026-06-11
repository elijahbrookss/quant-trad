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

Report-backed check families read `RunResearchDataset` through the reporting
contract. `run_signal_summary` counts matching signals, buckets them by
requested fields, and summarizes linked decision/trade presence. `run_decision_trade_comparison`
summarizes matching decisions by decision state and linked trade PnL. These
families do not replay runtime or rebuild indicator state; they mine completed
report evidence.

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
- Forward outcomes are analytical summaries, not simulated trades.
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
