---
component: adr-research-data-evolution
subsystem: system
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - storage
  - migration
  - research
  - compatibility
code_paths:
  - portal/backend/db/market_data_models.py
  - portal/backend/db/fact_storage_schema.py
  - portal/backend/db/session.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/fact_storage.py
  - portal/backend/service/storage/repos/market_lifecycle.py
  - src/market_data/frozen.py
  - src/market_data/fact_archive.py
  - scripts/db
  - scripts/automation/server_deploy.sh
---
# ADR 0078: Bound Migrations and Preserve Compatible Research

## Status

Accepted direction on 2026-10-04 after review narrowed the storage problem and
separately authorized bounded research streaming and individual job cancellation.
Storage candidate integration and the bounded research implementation are source
changes; release and physical qualification remain incomplete. The user selected
one coordinated storage/research deployment on 2026-10-04. The combined candidate
therefore waits for its physical-layout gates; this release choice does not make
storage cutover an intrinsic research dependency.
The [implementation specification](../../engineering/research-data-evolution-spec.md)
owns the concrete route, slices and acceptance evidence. Neither document
authorizes migration, deployment, cleanup or restarting paused work.

## Context

QT already has canonical Facts and revisions, payload versions, frozen Dataset
bindings, a canonical state engine and versioned research computations. Ordinary
Indicator development does not require reorganizing historical source Facts.

Storage growth is a separate real problem. Payload archiving leaves substantial
headers, indexes and lookup metadata in PostgreSQL. The selected storage candidate
adds dated headers and global identities. Its original upgrade rebuilt historical
headers and coupled extensive preparation/verification to client holds. Other
release work then depended on completing that layout.

This was not merely a scheduling mistake: the transition's historical scope and
blocking phases grew with the dataset. Candidate work now permits retaining the
existing header table while partitioning new writes. That avoids some large copies
but still needs identity, key, reference, placement and recovery qualification.
It does not establish that the candidate is the only solution to the capacity goal.

## Decision

Keep the existing research architecture. Require each storage change to justify
the historical data it must copy, transform or validate and the phases that block
writers or readers. Prefer compatible reads and changes to new writes where they
meet the actual goal. Convert only the necessary historical scope explicitly.

For this migration, qualify retaining existing headers before requiring a
full-history daily rebuild. Keep global uniqueness and reference enforcement.
Do not credit SSD relief until actual placement or bounded growth meets the
declared capacity horizon.

Prepare and verify under existing bounded storage owners while collection
continues where qualified. Keep the final publication interruption measured and
bounded; reject a route that cannot fit rather than extending a writer hold
indefinitely. Capture concurrent commits, commit verified units with progress,
fence publication, and reconcile uncertain completion through durable receipts.

Keep physical evolution behind the current repositories and archive readers.
Use `PG_DSN` and existing controllers, catalogs and recovery boundaries. Runtime
must never perform hidden DDL, conversion or backfill. Startup guards remain
strict; old/new compatibility must be an explicit supported contract.

Separate research release requirements from storage cutover unless the exact
runtime/schema pair, affected reads or resource limits require coupling. Preserve
pinned inputs, corrections, provenance, known-at meaning and producing code
identity. Existing scientific budgets, holdouts and authority remain unchanged.

Bound research reads and computation at their existing owners. Streaming keeps
one canonical engine timeline; explicit individual cancellation fences publication
and distinguishes a request from stopped execution. These are separate release
slices, not prerequisites for every storage operation or a new orchestration layer.
The execution-local stop/budget token lives in core and is shared by the existing
queue, SQL boundary and engine; it is not a persisted job authority. Existing
queue rows own cancellation and publication. Budget exhaustion fails explicitly,
and uncertain shutdown retains ownership instead of claiming stopped execution.

On October 4 the user additionally selected a 14-day recent-data SSD window and
a bounded, disposable SSD read cache for immutable historical archive objects.
HDD archives remain durable; cache eviction never writes back or expires Facts.
The existing reader, storage admission and research execution boundaries own
this addition. It creates no schema, independent catalog or computation cache.
A shared research admission guard qualifies one heavy operation across worker
and synchronous API processes; cache usefulness is measured with existing
protocols before expanding its budget. These are authorized source changes,
not a claim of deployment or measured performance.

## Consequences and limits

Migration cost may scale with affected data. The objective is to avoid routinely
affecting all history and making downtime scale with that work, not to promise
that all future migrations are cheap. Canonical key or relationship changes may
require global validation.

Compatibility costs readers, tests and retained pinned objects. Retaining an old
table avoids reformatting; moving it still costs I/O and may block reads.
Partitions help physical maintenance but do not solve arbitrary schema evolution.
Global identities and catalogs still grow. No query speedup or zero-interruption
claim follows from choosing this direction.

Leases prevent conflicting publication; resource admission separately protects
collection and active readers. Source copies and backups remain until explicitly
authorized retirement. After publication, old code or an old table alone is not
a safe rollback plan for a system accepting new writes.

## Alternatives not selected

- Rebuild all history merely to make physical layout uniform.
- Freeze all research until every storage, recovery enhancement and cleanup task
  finishes, regardless of its actual dependencies.
- Promise that date partitioning eliminates future schema migrations.
- Add a migration framework, second data authority, feature store or resource
  scheduler before a concrete requirement exceeds existing owners.
- Make H04 or general caching prerequisites for this storage transition, or couple
  separately authorized streaming/cancellation to physical cutover by default.

## Enforcement and references

The specification requires scope/capacity justification, concurrent preparation,
logical read equivalence, bounded pause/resource evidence, crash reconciliation
and applicable recovery/release qualification for the selected route. Historical
candidate tests support only their recorded source and scenario; no completed
production transition is asserted here.

- [Engineering contract](../../contracts/platform/03_engineering_contract.md) and
  [core promises](../../core-promises.md).
- [Canonical Facts](../data/GENERALIZED_FACT_DATA_PLANE.md) and
  [Check evidence](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md).
- [ADR 0062: frozen bindings](0062-use-frozen-bindings-for-durable-check-evidence.md)
  and [ADR 0068: compatible promotion](0068-rehearse-explicit-server-promotion.md).
