---
component: adr-candle-risk-observability
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - research
  - checks
  - known-at
  - evidence
code_paths:
  - portal/backend/service/research/forward_risk_evaluator.py
  - portal/backend/service/research/registry.py
  - portal/backend/service/research/planning.py
  - portal/backend/service/research/execution.py
  - portal/backend/service/indicators/indicator_service/runtime_validation.py
  - tests/test_portal/test_forward_risk_check.py
  - tests/test_market_data/test_candle_only_check_workflow_db.py
---
# ADR 0076: Declare candle-risk observability and calendar boundaries

## Context

An event-only evidence list cannot distinguish an observed ordinary period from
an unready detector. Extending every outcome past a discovery window can also
consume a reserved evaluation period. Dropping all events around any gap loses
valid shorter measurements, while filling holes invents observations.

## Decision

Register event-fact definition 10 / evaluator 9 with explicit fixed candle-risk
semantics. Public Candle Stats snapshots own detection, the Check owns analytical
cohorts and risk measurements, and the existing frozen Dataset owns source and
gap identity. Capture readiness only when requested, include it in input hashes,
and keep original default output hashes unchanged. A declared no-tail boundary
limits materialization; independently censor entry or targets outside it.

Use per-dependency and per-horizon eligibility. Preserve undetectable clock
intervals and unknown event counts. Measure raw risk separately from future/prior
risk ratios and high-low paths. The baseline excludes the trigger, prices obey
known-at, and future paths must be known by their target. Report calendar
variation, missingness, stratum coverage and dependence without inference or
promotion. Store all-clock-row digests plus bounded examples; frozen replay
reconstructs detailed rows without another persisted candle-sized population.

## Consequences

No new Indicator, provider, persistence schema, live order path or independent
state engine is introduced. Finite seed/reset initialization is explicit, not
claimed equivalent to uninterrupted EMA history. Old definitions keep their
meaning. Exact source admission, disposable frozen-workflow qualification and
resource limits remain required before empirical use. Annual readiness does not
follow from a small-window test or from absence of data acquisition errors.

## Validation

Synthetic tests exercise closed-form risk, prior and future clock boundaries,
zero baselines, delayed availability, independently missing paths, calendar end,
identity/configuration rejection, deterministic hashes, deletion sensitivity and
real-engine reset/readiness. The disposable workflow fixture freezes an actual
recorded gap, runs the registered owner, corrects mutable source data, and checks
provider-free replay against the original evidence.
