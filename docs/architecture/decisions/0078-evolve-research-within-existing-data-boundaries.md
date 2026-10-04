---
component: adr-research-data-evolution
subsystem: system
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - research
  - datasets
  - storage
  - known-at
code_paths:
  - src/market_data/frozen.py
  - src/market_data/fact_archive.py
  - src/research_science/check.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/fact_storage.py
  - portal/backend/service/market/normalization_service.py
  - portal/backend/service/research
  - portal/backend/service/async_jobs
  - portal/backend/workers/research_worker.py
---
# ADR 0078: Evolve Research Within Existing Data Boundaries

## Status

Proposed on 2026-10-03. This records the selected implementation direction;
enforcement and release qualification remain incomplete. It does not describe
deployed behavior or authorize research, migration, deployment, or cleanup.
The [implementation specification](../../engineering/research-data-evolution-spec.md)
owns slices, evidence, dependencies, and acceptance gates.

## Context

QT already has canonical revisioned Facts, frozen Dataset bindings, a canonical
state engine, versioned Checks, normalization, archive readers, and fenced jobs.
Research improvements should extend these owners. Most new computations do not
require changing historical source Facts. The outstanding risks are stable frozen
selection, expensive selection and provenance reads, whole-range materialization,
expensive engine computation, and resource competition with collection.

The storage candidate also changes physical identity and startup requirements.
Its combined application cannot simply run on an older layout. That packaging
dependency does not establish that H04's scientific behavior needs the physical
cutover. Existing qualification is useful evidence, not proof of a newly composed
release. Neither date partitioning nor moving history to disk guarantees faster
research.

## Decision

1. Keep one canonical data authority and `PG_DSN`. Preserve Fact identities,
   revisions, provenance, exact values, gaps, and known-at meaning. A semantic
   correction appends a revision or creates a separately versioned derivation;
   it does not overwrite the evidence used by an old run.
2. Make the existing Dataset boundary prove stable membership across concurrent
   commits. Pin the input binding, definition/evaluator and Indicator graph
   versions, parameters, clocks, seed, and evaluation range in existing Check
   records. A repeatable transaction alone is not a persisted selection proof.
3. Run Indicator computations through `initialize -> apply_bar -> snapshot`.
   Reuse existing normalization and Check results only when their full semantic
   identity matches. Reuse is explicit and never counts as independent replay.
4. Bound preparation, reads, decoding, computation, and result persistence in
   existing data and research owners. Use existing jobs for deduplication,
   ownership and atomic publication; separately admit resource use and provide
   cancellation. Start with a serial heavy-research budget, not a new scheduler.
5. Keep physical evolution behind the existing storage repositories and archive
   readers. Prefer reading supported older representations. Convert only an
   explicitly selected unit when evidence justifies it, with verified output,
   fenced atomic publication, retained source, and independent resource admission.
   Reads must never perform DDL, conversions, acquisition, or hidden backfills.
6. Qualify research against an identified supported serving layout. Prefer a
   compatible research release over requiring the whole storage rollout. Keep
   startup guards strict; review any actual schema dependency explicitly.

## Invariants and consequences

Old bindings and results remain immutable. New formats have explicit versions;
unsupported or inconsistent inputs fail with actionable context. A run pins
logical input identity and the reader/recipe it needs; physical replacements
must preserve that identity and keep required objects available. Collection has
priority over admitted research and maintenance. Lease ownership prevents stale
publication; it does not reserve CPU, memory, disk, or database connections.

This reduces mandatory historical work and permits incremental releases. It costs
compatibility tests, retained readers and pinned objects, explicit resource
accounting, and limited release composition work. Selective reuse consumes disk
and needs retention rules. An uncollected historical field remains missing; no
conversion can recreate it. Faster queries will not remove the measured H04
engine cost, and no speedup is promised before paired measurements.

## Alternatives not selected

- Complete all storage migration before research: retain this dependency only
  where the exact release or a measured capacity limit requires it.
- Partition and convert all historical data up front: significant temporary,
  index, WAL and recovery costs without evidence that every query benefits.
- Convert during ordinary reads: hidden operational work and uncertain latency.
- Add a feature store, universal artifact catalog, migration framework, second
  engine or database: existing owners already cover the demonstrated needs.
- Precompute every Indicator/parameter combination: speculative storage growth
  and invalidation cost. Start with a measured, repeatedly requested recipe.

## Evidence and enforcement

The specification records the pinned repository states, measured workloads,
unreproduced watermark concern, and required disposable concurrency, equivalence,
failure, resource and replay tests. Acceptance requires evidence for the exact
release and capabilities being enabled; saved candidate tests are not deployment
attestation. Physical cleanup is not a general research gate.

## References

- [System contract](../../contracts/platform/00_system_contract.md),
  [runtime contract](../../contracts/platform/01_runtime_contract.md), and
  [engineering contract](../../contracts/platform/03_engineering_contract.md).
- [Canonical Facts](../data/GENERALIZED_FACT_DATA_PLANE.md),
  [Check evidence](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md), and
  [research jobs](../research-orchestration/RESEARCH_ASYNC_JOB_BOUNDARY.md).
- [ADR 0062: frozen bindings](0062-use-frozen-bindings-for-durable-check-evidence.md),
  [ADR 0047: job fencing](0047-fence-async-job-ownership.md), and
  [ADR 0068: compatible promotion](0068-rehearse-explicit-server-promotion.md).
