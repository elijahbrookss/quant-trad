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
  - portal/backend/service/reports/contract.py
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
- can summarize completed report datasets without replaying runtime,
- persists a structured result with data quality, sample count, caveats,
  provenance, and recommendation.

If a caller runs a check without an observation, Quant-Trad creates an ad hoc
observation and links the check to it. Checks are never orphaned. Auto-created
observations are created after the check scope is normalized. Report-backed
checks read `RunResearchDataset` first so the generated observation can carry
run id, bot id, strategy id, symbols, timeframe, and simulated window context
when available.

Source-backed check families stay analytical and known-at. `raw_forward_outcome`
evaluates detector trees over source OHLCV and previous-bar OHLCV only, then
summarizes forward returns, max favorable excursion, and max adverse excursion.
It does not compute candle-derived features. When a candle-derived fact matters,
that fact must come from a persisted indicator through
`indicator_forward_outcome`, which evaluates declared indicator outputs collected
through the canonical runtime graph before measuring the same forward candle
outcomes.

Report-backed check families read `RunResearchDataset` through the reporting
contract. `run_signal_summary` summarizes matching report signals and their
linked decisions/trades. `run_decision_trade_comparison` summarizes matching
decisions by state and linked trade PnL. These checks link to both the
observation and analyzed run.

Missing or blocked evidence may persist as a blocked research check. Malformed
requests, unsupported detector semantics, and internal contract failures fail
loud and do not create research-memory items.

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
- Generated observations retain normalized source/report context instead of the
  raw request shape.
- Invalid detectors do not become blocked evidence; they fail before persistence.
- Existing data, indicator, experiment, report, and strategy contracts remain
  the authoritative layers for their own facts.
- The system can accumulate research memory over time without turning every
  idea into a strategy variant.
- Raw, indicator-backed, and report-backed checks reuse the same storage and
  link model while keeping their source-evidence contracts separate.
- Promotion remains explicit: a promising check recommends a hypothesis; it
  does not auto-create executable trading logic.

## References

- [Research Memory Boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [Research Orchestration Boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
- [Data Boundary](../data/DATA_BOUNDARY.md)
- [Use Output Filters As The Strategy Variant Contract](0018-use-output-filters-as-strategy-variant-contract.md)
