---
component: adr-question-interpretation-publication
subsystem: research-memory
layer: decision
doc_type: adr
status: accepted
tags:
  - research
  - questions
  - publication
  - evidence
code_paths:
  - portal/backend/service/research/publication.py
  - portal/backend/controller/research.py
  - cli/research_operations.py
  - portal/frontend/src/v2/rooms/ResearchEvidenceRoom.jsx
---
# ADR 0079: Publish Question-owned Interpretation Revisions

## Status

Accepted direction by Elijah on 2026-10-10. Local implementation and qualification
do not authorize production deployment, scientific dispatch or data alteration.

## Context

Generic Study memory payloads preserve notes but do not enforce question identity,
exact evidence or historical interpretation context. Executable StudyDefinition
compositions and scientific protocols already have distinct ownership. Check
results and frozen Datasets already preserve calculated evidence.

## Decision

Use the existing Study ID as a durable question. Explicit adoption adds a reserved,
validated JSONB envelope without changing prior fields. Append immutable summary
revisions under that question, preserving exact evidence references and small
reasoning/link snapshots. Use a Study row lock, previous-publication hash and
request digest for concurrency and idempotency. No table or automatic backfill is
required for this bounded history. Reject growth beyond the explicit 1 MiB bound
without discarding citations.

Publication resolves frozen Check contracts and Dataset identities read-only; it
never invokes replay or changes execution accounting. Protocol-private Dataset
identities remain inaccessible. Completeness grants no scientific or trading
authority. Generic item creation cannot claim the reserved publication contract.

## Consequences

Hypotheses and Observations retain their existing claim/finding and admission
meaning. Historical interpretations remain reconstructible after later edits.
Canonical Check payloads are not copied. Large histories need a separate reviewed
storage decision. SQL administrative mutation remains outside the application
contract and is not publication authority.

## Rejected Alternatives

A Campaign resource or another research engine duplicates authority. A standalone
summary lifecycle adds coordination burden. RunReportDTO does not own multi-Check
interpretation semantics. A table for every reasoning concept is unnecessary for
bounded append-only publication under an existing Study transaction boundary.

## Compatibility and Migration

Legacy records remain readable and uncontracted. Explicit identical adoption is
idempotent; no retroactive preregistration, result rewriting or refunds occur.
Rollback to older code preserves JSONB history but removes the new workflow.
Production deployment requires separate approval; no migration SQL is required.

## References

- [Research Memory Boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [Check Evidence Boundary](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md)
