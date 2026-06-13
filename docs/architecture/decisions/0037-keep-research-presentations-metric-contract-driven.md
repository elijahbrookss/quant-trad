---
component: adr-research-presentations-metric-contract-driven
subsystem: research-memory
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - research
  - metrics
  - checks
  - cli
  - contracts
code_paths:
  - portal/backend/service/research
  - portal/backend/controller/research.py
  - cli/main.py
  - docs/contracts/platform/03_engineering_contract.md
  - docs/architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md
---
# ADR 0037: Keep Research Presentations Metric-Contract Driven

## Status

Accepted on 2026-06-13.

## Context

Quant-Trad is adding research checks, signal audits, lifecycle checks, report
checks, and future comparison views that agents can use to decide what deserves
deeper study.

It is tempting for compact CLI or API views to learn indicator-specific language
such as retest quality, breakout cleanliness, touch rate, or other family
semantics. That would make the first workflow feel convenient, but it would
couple reusable research surfaces to the first indicator family that needed
them. Over time, that turns a framework into a pile of named exceptions.

The platform needs small research read models that can compare evidence without
becoming another source of indicator, strategy, or report truth.

## Decision

Research presentation surfaces must be metric-contract driven.

A compact comparison, leaderboard, or ranking view may:

- read research check outputs and report comparison outputs,
- group rows by explicit dimensions such as variant, side, horizon, symbol,
  timeframe, family, or check id,
- rank rows by an explicit metric supplied by the caller or declared by the
  producing check contract,
- render metric values, units, sample counts, caveats, provenance, and
  recommendation text,
- link to full evidence artifacts for audit.

It must not:

- import or inspect indicator-family code,
- hardcode indicator-specific metric names as workflow truth,
- infer profitability or signal quality from labels alone,
- invent ranking semantics when metric direction or rank intent is absent,
- hide missing fields behind default values or fallback ranking.

Metric-producing checks should expose comparable metric records or summary
fields with enough semantics for generic presentation:

- metric name,
- numeric value,
- optional unit,
- optional direction such as higher-is-better or lower-is-better,
- role such as coverage, quality, outcome, denominator, or caveat,
- dimensions needed for grouping.

If a metric cannot be compared safely, the presentation layer should fail loud
with actionable context. The caller can then choose a different metric, adjust
the check, or promote a better metric contract.

## Consequences

- Compact research views stay useful without knowing Market Profile, retests,
  candle patterns, or any future indicator family.
- Indicator-specific intelligence remains in indicator outputs and research
  check contracts, not in CLI formatting code.
- Agents can rank evidence across variants while preserving provenance and
  auditability.
- Missing ranking intent becomes a contract problem instead of a silent guess.
- New check families can participate by emitting metrics; they do not require
  presentation-layer branching.
- Some commands will require explicit `rank_by`, grouping, or primary metric
  declarations. That friction is intentional because the system should not
  pretend there is a universal best trading metric.

## References

- [Engineering Contract](../../contracts/platform/03_engineering_contract.md)
- [Research Memory Boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [ADR 0034: Use Research Checks as Analytical Memory Evidence](0034-use-research-checks-as-analytical-memory-evidence.md)
