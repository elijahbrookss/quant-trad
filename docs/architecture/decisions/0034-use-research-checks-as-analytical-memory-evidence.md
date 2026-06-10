---
component: adr-research-checks-analytical-memory-evidence
subsystem: research-memory
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - research
  - memory
  - checks
  - observations
  - studies
code_paths:
  - portal/backend/controller/research.py
  - portal/backend/service/research
  - portal/backend/db/models.py
  - cli/main.py
  - docs/architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md
---
# ADR 0034: Use Research Checks As Analytical Memory Evidence

## Status

Accepted on 2026-06-09.

## Context

Quant-Trad should not require the user to invent perfect trading strategies
upfront. The platform needs a compounding research-memory loop:

```text
Observation -> Research Check -> Hypothesis -> Study -> Experiment -> Report -> Strategy Variant
```

The missing layer is a lightweight way to test whether a market observation
appears historically before converting it into a strategy variant or full
experiment plan.

That layer must not compete with the existing runtime, experiment, reporting,
or strategy machinery. It should reduce idea pressure by turning small market
notes into evidence, while still preserving the platform's core rule that
runtime truth comes only from runtime/report contracts.

## Decision

Implement research checks as first-class research-memory items, not as a new
execution engine.

A research check:

- links to an observation,
- requests source facts through existing data-boundary services,
- evaluates known-at analytical conditions,
- measures forward analytical outcomes,
- persists a structured result with data quality, sample count, caveats,
  provenance, and recommendation.

If a caller runs a check without an observation, Quant-Trad creates an ad hoc
observation and links the check to it. Checks are never orphaned.

The first check family is `candle_event_forward_outcome`, which evaluates JSON
candle condition trees over OHLCV and derived candle features, then summarizes
forward returns, max favorable excursion, and max adverse excursion.

Research memory uses two tables:

- `portal_research_items`
- `portal_research_links`

This keeps table count small while allowing observations, checks, hypotheses,
studies, strategies, variants, experiments, reports, and runs to form one
auditable research graph.

## Consequences

- Observations can be checked quickly before a formal strategy/experiment
  exists.
- Checks provide analytical evidence, not simulated trade truth.
- Existing data, indicator, experiment, report, and strategy contracts remain
  the authoritative layers for their own facts.
- The system can accumulate research memory over time without turning every
  idea into a strategy variant.
- Future indicator-backed or report-cohort checks can reuse the same storage and
  link model.
- Promotion remains explicit: a promising check recommends a hypothesis; it
  does not auto-create executable trading logic.

## References

- [Research Memory Boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [Research Orchestration Boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
- [Data Boundary](../data/DATA_BOUNDARY.md)
- [Use Output Filters As The Strategy Variant Contract](0018-use-output-filters-as-strategy-variant-contract.md)
