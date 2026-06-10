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

The check runner is intentionally boring:

1. normalize the check request,
2. ensure the check is attached to an observation, creating an ad hoc
   observation when needed,
3. resolve the canonical instrument through the instrument/data boundary,
4. run candle coverage preflight,
5. fetch source candles through the candle service,
6. detect occurrences with known-at candle data,
7. measure forward analytical outcomes,
8. persist the check result as a research-memory item,
9. link the check back to the observation.

The first check family is `candle_event_forward_outcome`. It supports candle
condition trees over known-at OHLCV fields and derived candle features such as
`range_pct`, `body_pct`, `return_pct`, wick percentages, and close position.
It measures forward returns, max favorable excursion, and max adverse excursion
over declared future bar windows.

Future check families may use indicator typed outputs or prior report cohorts,
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
observation so analytical work is never orphaned.

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
- Forward outcomes are analytical summaries, not simulated trades.
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
