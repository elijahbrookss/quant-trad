---
component: adr-explicit-frozen-check-observation-admission
subsystem: research-memory
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - research
  - checks
  - observations
  - evidence
  - replay
  - compatibility
code_paths:
  - portal/backend/service/research/service.py
  - portal/backend/controller/research.py
  - docs/architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md
  - docs/architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md
  - tests/test_portal/test_research_evidence_service.py
---

# ADR 0065: Use Explicit Frozen-Check Admission For New Research Observations

## Status

Accepted on 2026-08-24 as the Phase 3 resolution of DRR-07.

This is a prospective supersession decision. Historical records and the
accepted decisions that produced them remain part of the audit trail.

## Context

ADR 0034 made each Research Check link to a Research Observation and required an
ad hoc Observation to be created when the caller did not supply one. That model
predated the current separation between ephemeral Check preview and frozen,
replayable Check evidence.

ADR 0062 established that preview is ephemeral and cannot support an
evidence-bearing Observation, while durable Check evidence requires an exact
frozen binding, provider-free execution, versioned definitions, and replayable
material and result hashes. Current V2 implementation persists that evidence
without automatically creating an Observation and exposes a separate operation
for creating an Observation from eligible evidence.

Leaving ADR 0034's automatic-creation clauses applicable to new records would
permit two incompatible admission rules and would make preview, blocked, and
legacy behavior ambiguous.

## Decision

For new V2 Research Check records, ADR 0062 and this decision govern
Check-to-Observation admission.

- Check preview remains ephemeral. It does not persist Check evidence and cannot
  create, link, or support a Research Observation.
- Evidence-mode Checks require frozen, provider-free, replayable input binding
  and persist their definition, request, plan, evidence, result, hashes, and
  exact evidence target.
- Persisting a Check does not automatically create or require a Research
  Observation.
- Only a completed, replayable Check whose reviewed definition is
  Observation-eligible may support a new Research Observation.
- Check-backed Observation creation is a separate explicit operation. That
  operation revalidates the persisted Check and creates a `supports` link from
  the Check to the new Observation.
- Blocked, incomplete, preview, legacy-unpinned, source-mismatched, or otherwise
  replay-ineligible Checks cannot support a new Research Observation.
- Historical V1 Checks, their automatically created Observations, and their
  existing links remain readable as historical evidence. They are not rewritten,
  upgraded, or represented as current replayable evidence.
- A market observation, an Observation key in canonical market data, and a
  Research Observation remain distinct concepts.

This decision governs Check-derived Observation admission only. It does not
require every Research Observation to originate from a Check and does not change
separately owned manual Research Observation creation.

## Supersession Scope

For new records, this decision supersedes only ADR 0034's clauses that require
every Check to link to an Observation and automatically create one when absent.

ADR 0034 remains accepted for the broader decision that Research Checks are
first-class analytical-memory evidence rather than an execution, strategy, or
report-reconstruction engine.

## Consequences

A durable Check may exist as evidence without an Observation. Its exact Dataset
or run evidence remains directly traversable through its evidence link.

Observation creation becomes an explicit research-memory choice made only after
the Check has produced eligible evidence. Preview can remain fast and useful
without acquiring durable evidence authority.

Legacy V1 behavior remains visible for compatibility and audit. New code must
not invoke that compatibility path as a fallback when V2 admission fails.

## Rejected Alternatives

- Continue automatically creating Observations for every persisted Check:
  conflicts with the preview/evidence split and creates durable memory before
  evidence eligibility is known.
- Allow preview-created exploratory Observations: gives mutable-store analysis a
  durable evidence role it cannot support.
- Introduce separate preview and evidence Observation classes: expands the
  domain model without a demonstrated need.
- Rewrite or upgrade historical V1 records: destroys their original evidence
  meaning and audit lineage.
- Treat every completed Check as Observation-eligible: bypasses the reviewed
  definition and evidence-admission boundary.

## Assurance Boundary

This decision settles the prospective semantic conflict. It does not adopt
glossary entries, activate a guarantee, mark `QT-REM-004` complete, or attest
that any proof has passed.

## References

- [ADR 0034: Use Research Checks As Analytical Memory Evidence](0034-use-research-checks-as-analytical-memory-evidence.md)
- [ADR 0062: Use Frozen Bindings For Durable Check Evidence](0062-use-frozen-bindings-for-durable-check-evidence.md)
- [Check Evidence Boundary](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md)
- [Research Memory Boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
