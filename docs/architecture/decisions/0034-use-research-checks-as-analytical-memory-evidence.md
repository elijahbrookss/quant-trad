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
# ADR 0034: Use Research Checks as Analytical Memory Evidence

Accepted June 9, 2026 for first-class analytical evidence. The original automatic
Observation-linking rule was superseded for new V2 records by
[ADR 0065](0065-use-explicit-frozen-check-admission-for-new-research-observations.md).
[ADR 0062](0062-use-frozen-bindings-for-durable-check-evidence.md) separates
ephemeral preview from frozen evidence. Read those decisions for current admission.

## A research finding needs evidence before execution

Research needs a bounded way to test an idea without inventing a Strategy merely
to run analysis. A Check measures declared analytical conditions and outcomes,
retains quality, sample counts and caveats, and can link findings into research
memory. It does not become a second execution engine or create trading truth.

Source-backed work consumes declared source facts or public typed Indicator
outputs. Report-backed analysis consumes the reporting contract. Presentations
and orchestration render or sequence the result; they do not independently
recalculate its meaning. Invalid requests and unsupported semantics fail loudly.

## Keep findings and calculations distinct

The original design linked every Check to an Observation and automatically
created one when absent. That choice predated the preview/evidence distinction.
It explains historical V1 records but is not the rule for creating new evidence.

Current V2 persistence may retain a durable Check without an Observation.
Observation creation is a separate explicit operation over eligible completed
evidence. Preview cannot acquire that authority. Historical records retain their
original interpretation and must not be silently promoted to current replayable
evidence.

Studies own questions and interpretation; Checks own versioned analytical
procedures and their evidence. A promising result does not automatically create
executable trading logic, authorize promotion or establish profitability.

## Consequences

This makes exploratory feedback possible without polluting durable memory.
Frozen evidence costs explicit preparation and versioned execution, but permits
provider-free replay when the admitted procedure remains executable.
Historical integrity and deterministic recomputation remain distinct; retired
procedures need explicit custody rules rather than a convenient fallback.

Reusable method schemas and synthetic examples may be public. Actual hypotheses,
selections, results and Strategy details belong in QT's private record stores.

References: [Check evidence](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md),
[research memory](../research-memory/RESEARCH_MEMORY_BOUNDARY.md),
[historical custody decision](0082-separate-historical-custody-from-active-research.md).
