# Bounded Storage Migrations and Research Compatibility

**Status: implementation authorized; qualification in progress. Updated 2026-10-04.**
Decision: [ADR 0078](../architecture/decisions/0078-evolve-research-within-existing-data-boundaries.md).
This revision replaces this document's earlier H04-first implementation plan.
It does not resume a migration, clear an operational hold, deploy a release or
restart research. Historical approvals and receipts remain evidence.

## What we are solving

QT already supports collection alongside Indicator development, versioned
computations, frozen research inputs and reproducible research. Keep that
architecture. Ordinary research should rarely require a storage migration.

The immediate problem is how to address storage growth without routinely
rebuilding all history or making the period of blocked work grow with the
dataset. The original storage migration had a real capacity and placement goal.
Its full-history copy, final verification and release coupling made the operation
disruptive. This was a design and execution problem, not just poor scheduling.

Migration work can legitimately scale with the amount of affected data. The aim
is to reduce that affected scope and bound the final interruption. No design can
promise that every future canonical identity or schema change will be local.

The storage milestone is a qualified decision and implementation path for the
smallest necessary storage transition. It is not a new research engine, a general
migration framework, or completion of a particular scientific experiment.

The separately authorized research workstream adds bounded historical streaming
and safe cancellation of individual research jobs. These improve resource use and
control of existing capabilities; they do not introduce another research engine.
Both workstreams are in scope for implementation. Neither authorizes production
operations, deployment, migration execution or restarting paused automations.

## Current state and why the original migration was large

Evidence comes from the prior audit and these inspected checkouts:

- **Q:** `/home/elijah/dev/quant-trad`, runtime source at
  `83c2a7f792f2d666bd666a3d252d5e6cc4c5db9e`; the earlier documentation was
  committed as `6a4203a8a40e4b550fc3f08b40c72ac44ea1ce87`.
- **S:** `/home/elijah/dev/quant-trad-codex-storage` at
  `3f4c55c6a04eb65f969043026cc870feffbbe750`.
- **R:** `/home/elijah/dev/quant-trad-codex-research` at
  `a045816960e36ed1c6096fd3872d37c3808b0b66`.

Q/S/R below identify repositories, not deployment states. The storage candidate
has implemented and tested parts; its end-to-end production cutover was not
qualified in the inspected evidence. No fresh production inspection was made.
Preserved source data, migration copies, journals, archives and recovery material
are not disposable merely because a replacement plan is preferred.

| Evidence | What it establishes |
| --- | --- |
| S `docs/architecture/decisions/0070-separate-global-fact-identity-from-dated-headers.md:29` | Payload archiving leaves headers and indexes growing. Daily header groups were chosen for independent placement, with a global identity registry to retain uniqueness. This records the selected candidate, not proof that it is the only viable solution. |
| S `docs/architecture/decisions/0073-prepare-storage-migrations-with-live-collection.md:87` | The original operator held clients through preparation and scanned historical content under final writer fences. A constrained-fixture projection exceeded 50 hours for some phases; this is not a measured production outage. |
| S `docs/architecture/decisions/0077-retain-legacy-headers-during-forward-cutover.md:36` | Old and proposed headers have identical columns and secondary search indexes, but different primary/revision keys. Retaining the old table can avoid its heap copy and search-index rebuild; identity, composite-key, reference, archive and recovery work remains. |
| S `artifacts/storage-implementation/migration-release-20260926/completion-forward-capacity-inventory-20261003.json` | Recorded source headers including indexes: 316,197,470,208 bytes; raw archive lookup relation: 130,259,869,696 bytes. Capacity admission was incomplete. Database totals include retained migration copies; do not add components to totals or credit unapproved reclamation. |
| S `portal/backend/db/session.py:1666`, `portal/backend/db/fact_storage_schema.py:208` | Combined candidate startup requires the new header/identity layout. This is a real compatibility dependency of that package. It does not make physical cutover a semantic dependency of H04. |

The header-copy stage reused existing payload bytes; the broader rollout also
included raw lookup, archive placement and recovery work. It was not simply a
research computation rewriting all market observations.

## The migration policy

Before implementing a migration, its owning storage/runbook document must state
the problem, intended benefit, affected representation and historical scope.
Classify scope as new writes only, selected historical ranges, or all history.
Justify every required copy, index build and validation scan, especially any work
inside a writer or reader fence. Use the existing owner documentation; add no
universal migration manifest, registry or DSL.

| Change | Normal treatment |
| --- | --- |
| Indicator, parameters or analytical recipe | Existing versioned computation through the canonical state engine. Leave source Facts alone. |
| New provider information or payload fields | Existing provider/Fact registration and an explicit compatible schema version where needed. Uncollected history stays missing; acquisition is separate authorized work. |
| Semantic correction or changed derivation | Append revisions or separately version derived output, with true availability and lineage. Recompute only the explicit scope; old runs retain their original inputs. |
| Physical storage format or placement | Prefer compatible readers and new-write changes. Convert only justified historical units through explicit storage work. Reads never run DDL, acquisition, conversion or backfills. |
| Canonical identity, relationship or schema meaning | Review the owning contract and ADR. Global constraints or reference changes may require broad historical work; state why and measure its impact. |

Preserve Fact IDs, revision history, exact values, provenance, known-at semantics,
Dataset bindings and archive pins. Compatibility belongs in existing storage
readers with explicit supported versions, not scattered application fallbacks.
Do not revive retired semantic stores or add another DSN or data authority.

Partitions help bound physical placement and maintenance; they do not give each
partition an unrelated relational schema. PostgreSQL partitions share their
parent's columns and have constraints on partitioned uniqueness
([PostgreSQL 15 partitioning](https://www.postgresql.org/docs/15/ddl-partitioning.html)).
Date partitioning is therefore one storage tool, not a universal answer to schema
evolution or a promise of faster queries.

## Select the smallest useful route

| Route | Recommendation and limits |
| --- | --- |
| Continue on the serving layout temporarily | Valid only with a measured capacity horizon and compatible releases. Avoid immediate migration cost, but do not describe unresolved storage growth as solved. |
| Retain existing headers; partition new writes | Preferred route to qualify for this migration, reusing S's forward-cutover work. Saves unnecessary historical header copying. Still requires identity/key/reference proof, catch-up, final attachment and applicable recovery work. |
| Rebuild all historical headers into daily groups | Not the default. Consider only if the retained-history route cannot meet a demonstrated requirement and the additional cost/interruption is explicitly justified. Failure of one route does not authorize another. |

Retaining a large legacy table on SSD does not itself reclaim SSD capacity.
Moving that retained table and its indexes to HDD still costs physical I/O and
can block reads. Define separately whether the current milestone must move it,
or whether bounded future SSD growth provides sufficient measured headroom.
Do not claim completed capacity relief from merely changing the table layout.

The small global identity registry is also not free: it grows with Fact count
and adds indexes, writes and references. Reuse existing candidate work where
qualified, but include its real cost before committing to the route.

## Reuse existing owners

The data/storage owner owns models, repositories, archive readers and migration
primitives. The release/operator owner owns exact runtime compatibility,
physical admission and recovery. Research owns its protocols and evidence, and
validates affected reads. These are existing responsibilities.

| Existing work and code areas | Treatment |
| --- | --- |
| Q `portal/backend/db/{market_data_models,market_storage_models,fact_storage_schema,session}.py`; `src/market_data/{frozen,fact_archive,archive}.py`; `portal/backend/service/storage/repos/{market_data,fact_storage,market_lifecycle}.py` | Preserve Fact/Dataset semantics, raw evidence, archive verification and lifecycle ownership. Extend only the actual persistence boundary that needs change. |
| S `scripts/db/{fact_header_forward_keys,fact_header_forward_adoption,fact_header_v2_references,fact_header_v2_handoff}.py`; `portal/backend/db/fact_header_legacy_schema.py` | Reuse implemented retained-history and reference mechanics where exact-source qualification applies. No assumption that production cutover is complete. |
| S `scripts/db/fact_header_v2_capture.py`, `scripts/automation/storage_online_*.py`; `portal/backend/service/storage/{header_movement,header_resource_claims,incremental_recovery}.py` | Reuse capture, ownership, bounded phases and recovery mechanisms. Do not create a second controller or replace their receipts with a generic framework. Retired/cancelled operations remain retired. |
| R research definitions, frozen derivation and single-attempt work; Q `portal/backend/service/research/` | Preserve completed work. H04 has no intrinsic physical schema dependency; evaluate release composition against the actual serving schema. Do not weaken startup guards or maintain a permanent second application fork. |

## Implementation slices

**Order: M0 -> M1/M2 -> M3; M4 only when the admitted capacity goal requires it.**
M1 and M2 may be developed separately, then qualified together. Each slice updates
its existing component document/runbook with the exact source and evidence.

### M0 — Choose the route and account for the work

Replace “finish the storage rollout” with a concrete capacity goal and justified
historical scope. Storage and release owners identify the serving schema/runtime,
selected candidate, archive/recovery dependencies, retained allocations, growth
horizon and required pause. Use existing inspection, inventory and admission code.

No schema/API change is required for this decision. Identify what can stay
unchanged, what must be converted or validated, and what can wait. Preserve
existing deadlines and resource limits; a different proposal requires an explicit
review, not a silently extended operation.

Completion requires a component-by-component byte/work ledger, route comparison,
compatibility decision and numeric admission limits supported by evidence.
Unknown capacity or an unjustified full-history phase blocks that operation.
Cancelling this planning slice changes no serving state. Its cost is bounded
inspection and review, not a production scan or build.

### M1 — Qualify unchanged meaning across retained and new storage

The existing repository must return the same logical Facts whether they live in
the retained table, new daily groups, hot payloads or supported archives. New
writes follow the selected layout; old data need not be reformatted to be read.

Storage owns the model/reader work identified above. If adopting S's layout,
its parent, keys, identity and legacy binding are explicit schema changes with
operator cutover requirements. Reuse those candidate structures; invent no new
compatibility catalog. Preserve SQL source/revision/known-at selection before
hydration, and test ranges spanning the old/new boundary and late corrections.

Validate exact values, identities, ordering, invalidations, provenance, material
and frozen-input hashes. Include empty ranges and corrupt/missing objects.
Do not weaken uniqueness, frozen validation or integrity checks for compatibility.
Retain the source and required readers. Unsupported versions fail explicitly;
application rollback is allowed only against a schema it supports.

Reader support and retained objects have ongoing cost. Document the support and
pin lifetime before retiring either; no automatic deletion is included here.

### M2 — Prepare and verify within bounded storage operations

Use existing migration owners to prepare keys, capture concurrent writes,
catch up identities/references and verify the selected scope while the serving
source remains authoritative. Replace blanket client holds with the specific
brief fences needed by each phase.

Bound rows, bytes, transaction/statement time and total phase time. Commit each
verified unit with its cursor; use transactional capture so a late commit cannot
fall behind a numeric cursor. Prevalidate references outside the final fence
where the actual database mechanics permit it. If a native full scan/build is
unavoidable, identify and admit it explicitly; do not label it a bounded page job.

Ownership and resource admission are separate: one actor may own a job yet lack
space or I/O headroom. Reuse current storage claims/watchers and fixed limits.
Include collection growth, spool, WAL, temporary files, indexes, source/target
overlap and backup dependencies. Release claims only after terminal outcome and
execution are known. Stop/throttle migration before collection exceeds its
declared tolerances; a timeout is not proof the server operation stopped.

Tests cover duplicate actors, late commits, page failure, lost ownership,
cancellation, restart and uncertain completion. No partial output becomes serving
truth. Preserve valid completed work and its provenance. Reusing retained data in
a separately authorized replacement requires ownership and content proof; it
cannot renew an expired or cancelled operation. This slice adds no research
scheduler or cache.

### M3 — Publish through a measured final switch and qualify the release

Catch up, perform the remaining proof and publish the layout through the existing
fenced operator transaction and durable outcome inspection. The final pause has a
declared maximum and measured evidence on a representative physical workload.
Do not put all-history comparison inside that pause by default.

For the retained-history candidate, range validation/attachment and composite
constraints still need explicit timing and lock proof. If they cannot fit, reject
or redesign the route before starting; do not keep writers stopped while inventing
the next step. Include actual affected readers in interruption accounting.

Before publication, cancellation leaves the source authoritative. After a lost
commit reply, reconcile the durable outcome before retrying or reopening writers.
After publication, use the tested forward recovery/rollback procedure for the
actual schema and any new writes. Retaining an old image or table alone does not
make rollback safe; never restore a DB over newer collection automatically.

Qualify the exact runtime/schema pair, preserved archives and usable recovery
dependencies. Separate an independently compatible research release from physical
cutover where possible. This slice does not authorize any deployment or automatic
research restart. Its costs include rehearsals and retained recovery material.

### M4 — Place historical groups only where capacity requires it

If M0 requires physical movement to meet its capacity goal, qualify that movement
separately through existing storage placement and recovery owners. A large retained
table remains a large movement unit; retaining it avoids reformatting, not the
cost of moving its bytes.

Budget the complete heap/index group, WAL, temporary overlap and read locks.
Read while a replacement is prepared only through a supported stable source;
publish verified locations atomically, preserve pins and recover uncertain moves
through existing receipts. Stop new work under pressure without discarding source
data. Test interruption, missing mounts, changed target identity and restore.

No historical cleanup is implied by successful movement. Reclamation is separate
authorized work after reader, pin and backup checks. If the goal requires a large
legacy move and it cannot fit the limits, report the capacity goal unmet.

## Evidence required before an operation is admitted

No row below is a new universal research gate. Each applies to the storage phase,
release or read path being changed. Current status is not a fresh production check.

| Requirement | Pass evidence | Current position / owner |
| --- | --- | --- |
| Necessity and historical scope | Measured capacity goal, explicit affected scope, alternatives and justification for every all-history copy/scan. | Real growth evidence; selected route and fresh horizon still need admission. Storage/release. |
| Meaning and compatibility | Exact candidate/serving-schema inventory; identical logical and frozen inputs across affected old/new/hot/cold paths; strict guards still pass. | Implemented/tested candidate parts, incomplete end-to-end qualification. Data/storage. |
| Concurrent preparation | Real collector-shaped inserts and late commits survive capture/catch-up; references remain valid; no long blanket writer fence. | Existing capture/online work is reusable evidence, not complete admission. Storage. |
| Resource and interruption bounds | Numeric peak space, phase/pause, lock, lag/spool and cancellation limits supported by measurements; pressure stops work within the bound. | Recorded capacity admission incomplete. No invented speedup, zero-interruption or reclaimed-space credit. Storage/operator. |
| Ownership and uncertain outcome | Two actors cannot publish twice; stale owners fail; crash before/after publication and lost reply reconcile without data loss or unaccounted execution. | Candidate tests exist; require coverage of the selected exact route. Storage. |
| Recovery and deployment | Applicable disposable restore/promotion checks pass for actual DB, archives, locations and keys; compatible rollback or forward recovery documented. | Historical receipts have limited scope; cancelled restore is not a pass. Release/storage. |
| Research coexistence | Affected frozen reads/replay retain identities and semantics; compatible research can run within resource limits; any pause names the operation, reason, limit and recovery condition. | No intrinsic H04 schema need; combined candidate does require new storage. Research/release. |
| Capacity outcome | Actual placement and projected growth meet M0's stated horizon, including growing identity/catalog/backup costs. | Retaining old data on SSD alone does not prove relief. Storage/operator. |

Start later qualification with disposable concurrency and failure fixtures, small
representative ranges and plan inspection. Measure header selection, hydration
and decoding separately from research computation when attributing read impact.
Compare a recent series query, a frozen historical range and the cold/legacy path
actually affected. Record collector throughput/lag, spool, DB waits, disk headroom
and relevant write/WAL/temp bytes during the operation.

Small fixtures prove behavior, not production duration. Increase scope only under
admission. Existing full-scan/build receipts can be reused when their source,
hardware and operation match; do not rerun heavy measurements merely to fill a
new template. No heavy measurement is authorized by this documentation change.

## Research continues under its existing rules

No migration is needed merely to add an Indicator or run another versioned Check.
Preserve existing scientific protocols, holdouts, trial budgets and execution
authority. H04's existing preparation and qualification remain useful, but its
completion is not this storage project's universal acceptance gate.

During bulk storage work, compatible research may continue if its reads and the
combined resource use are admitted. Where a particular operation cannot coexist,
defer that operation for a bounded, stated reason instead of freezing all research
until unrelated migration or cleanup finishes. Readers never trigger conversion.

A frozen Dataset protects input identity; it does not retain executable code.
Current Check replay also requires the producing code revision to be running
(Q `portal/backend/service/research/service.py:1437`). A release/migration plan
must account for that when promising replay, without adding historical-runtime
hosting infrastructure. Preserve results and code identity; do not silently replay
under a different version.

Nothing here clears current holds or launches work. Existing research authority
remains necessary, independently of a storage gate passing.

## Bounded research execution

Implement R0 -> R1 -> R2 alongside M0–M4. Passing research tests does not qualify a
physical migration, and unfinished historical placement does not block a release
whose exact runtime/schema pair is independently compatible. Reuse the research
changes already included in storage candidate S; preserve R as evidence for the
independent research release. Do not weaken S's layout guards to pretend that its
combined package supports the serving schema.

### R0 — Individual job cancellation in the existing queue

Extend `portal_async_jobs`, the research worker and `qt research jobs`. Before:
a CLI timeout or shutdown signal can leave a long computation running. After:
an explicit cancellation request stops the individual job at execution checkpoints,
cancels that job's active database statement, and refuses result publication.

Queued jobs can finish cancelled immediately. Running jobs retain their claim and
in-flight request identity until the worker acknowledges that computation and its
database work have stopped. A stale heartbeat is not proof of stopped execution;
report uncertainty and prevent automatic retry of a cancellation request. Under
the existing row lock, completion and cancellation have one definitive winner.
Never terminate the shared backend supervisor to cancel one research job.

Own this in `service/async_jobs`, `workers/research_worker.py`, research dispatch,
controller and CLI. Keep requests immutable and reuse existing job metadata where
possible; no new queue or scheduler. Cancellation does not publish partial Checks,
refund scientific attempts, resume checkpoints, remove source data or cancel
another job. Retrying is an explicit new request with the same pinned semantics.

Validate queued/running/terminal cancellation, duplicate cancellation, concurrent
completion, lost ownership, worker failure, blocked SQL, shutdown and CLI/API
contracts. Measure acknowledgement latency separately from request latency.
An old worker must be drained before enabling the new cancellation surface;
request acceptance must not imply that an unqualified worker can stop.

### R1 — Stream historical input through existing boundaries

Replace unnecessary complete input lists at the repository, hydration and runtime
driver boundaries with bounded pages. One engine instance consumes the ordered
stream using `initialize -> apply_bar -> snapshot`; a page boundary never resets
warmup, gaps, indicator commit clocks or known-at selection. Preserve exact input
hashes and correction choice across page sizes and hot/cold/retained layouts.

Select series, range, revisions and fields before hydration. Bound each page and
close readers on error or cancellation. Preserve materializing APIs for consumers
that need them, but apply explicit total admission limits before unbounded work.
Algorithms and outputs that intrinsically retain history still consume memory:
declare and enforce their bounds rather than claiming constant-memory research.

Owners are `storage/repos/market_data.py`, frozen Dataset validation, runtime
market-data resolution, Indicator runtime validation and Check execution. No
second canonical store, generic artifact platform or read-triggered conversion.
Incomplete computation publishes no durable evidence; retry starts from its
pinned request. Old evidence hashes and exact replay remain compatible.

Validate multiple page sizes, empty ranges, corrections crossing page boundaries,
recorded gaps, late availability, cold corruption and cancellation during reads
and engine work. Add a disposable concurrency test for the suspected frozen
watermark race; describe it as a failure only if reproduced. Repair a confirmed
gap in the existing frozen-input boundary before qualifying affected evidence.

### R2 — End-to-end limits and qualification

Use the existing research execution boundary to enforce declared row, byte,
elapsed-time and output limits, including synchronous preview/replay paths.
Cancellation and limit failures are explicit failures, never truncated successful
evidence. Keep worker concurrency separate from ownership and per-job budgets.
No cross-service priority scheduler, persistent result cache or automatic research
launch is included.

The representative workload set is: recent series/time selection; long frozen
input preparation; cold history; the recorded expensive provenance lookup; and
year-scale H04 plus an identical repeat. Use existing receipts first. Record
selection, hydration, decoding, engine, evaluator and persistence time separately,
with peak memory, rows/bytes read and written, archive verification/reuse and
collector lag/spool. Repeating a request is not assumed to be cached, and required
scientific replay must still execute. Small fixtures establish semantics and
limits; they do not establish year-scale speedups or production capacity.

Research qualification requires: equal frozen inputs under concurrency; preserved
known-at/gap/engine behavior; equal supported format reads; enforced limits;
cancel/duplicate/stale-owner/publication tests; exact release compatibility; and
unchanged protocols, holdouts and scientific budgets. Each requirement gates the
affected capability only. Deployment and resumption remain separately authorized.

## Separate follow-ups, outside this implementation

These are recorded findings, not mandatory additions to the migration:

- **Query and hydration performance:** the saved provenance lookup averaged
  2,354.83 ms over 25 calls (S `artifacts/storage-implementation/bip-delay-statement-deltas-20260924.json`).
  Optimize only relevant measured paths; no blanket index removal or new index
  policy follows from that aggregate.
- **Repeated replay and reuse:** Q `portal/backend/service/research/result_reference.py:192`
  executes replay while resolving scientific result evidence. Account for that
  work before proposing caching; preserve required scientific verification.
- **Persistent computation caches:** require evidence of worthwhile repeated
  computation and preserved scientific verification. Do not introduce a general
  scheduler, feature store, artifact catalog or new research service here.

## Documentation and handoff

Keep this specification and ADR 0078 as the central decision record. Detailed
mechanics and receipts stay with existing storage component docs/runbooks. Update
contracts only if product meaning changes; mark implemented, tested and deployed
states separately. The earlier broader plan remains in Git history, not as a
parallel active implementation mandate.

This documentation change requires index generation, `make validate-docs`,
`make sync-docs` and `git diff --check`. Later implementation requires focused
tests and the applicable [normal validation matrix](developer-workflow.md),
including disposable DB and recovery tests for affected persistence boundaries.
Unavailable or skipped evidence is not a pass. Runtime implementation and local
disposable validation are now authorized. Production migration, deployment,
transfer, cleanup and automation restart remain outside this task's authority.
