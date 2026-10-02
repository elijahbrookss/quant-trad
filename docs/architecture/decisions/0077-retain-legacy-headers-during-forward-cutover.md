---
component: adr-forward-header-cutover
subsystem: persistence
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - storage
  - postgres
  - explicit-migration
code_paths:
  - portal/backend/db/fact_header_legacy_schema.py
  - portal/backend/db/fact_identity_schema.py
  - portal/backend/db/fact_series_day_schema.py
  - portal/backend/db/market_data_models.py
  - portal/backend/service/storage/header_catalog.py
  - portal/backend/service/storage/repos/fact_storage.py
  - scripts/db/fact_header_v2_handoff.py
---
# ADR 0077: Retain Legacy Headers During a Forward Cutover

Proposed October 2, 2026 after the user asked whether new storage could become
usable without rewriting all historical headers first. This is an evidence-backed
proposal, not a production release decision. The first implementation slice
supports an explicit sealed legacy binding, canonical reads and bounded physical
inventory; the forward operator and production qualification are still incomplete. The existing supported
operator must not be used to approximate this proposal with manual schema edits.

## Avoid copying a table whose contents already match

The current tiered-v1 header table and the dated v2 parent have identical column
contracts and secondary search indexes. Their primary and revision keys differ.
The full-copy operator rebuilds all historical headers and indexes so each day
can move independently. On the existing drives, copying while collection
continues also requires space for concurrent growth, queues, indexes, WAL and
recovery baselines. The failed production attempt did not finish that work.

The proposed alternative retains the existing header heap as one sealed range
partition under the same logical `market.fact_versions` parent. New storage
days use normal daily partitions. The global identity registry remains the
single native owner of Fact and revision uniqueness, as defined by
[ADR 0070](0070-separate-global-fact-identity-from-dated-headers.md). Payload and
archive references still target that registry. No second DSN, dual writer,
alternate Fact meaning or application-visible legacy mode is introduced.

This can avoid the historical header heap copy and rebuilding its search
indexes. It does not eliminate composite key construction, identity and raw
mapping catch-up, reference validation, archive work or recovery publication.
The existing work can be reused only after its ownership and content are proved
for the replacement operation. A completed baseline is not a caught-up target.

## One parent, with an explicit retained range

The physical catalogue must distinguish the single operator-installed legacy
range from newly provisioned daily groups. It must bind the actual relation,
bounds, native constraints, guards and complete indexes. Missing, detached or
changed legacy history must fail startup and movement admission. An empty
replacement, a broad exception to daily checks or a fabricated daily catalogue
entry is not an acceptable representation.

Exact-ID reads continue to resolve global identity and storage day before
reading the parent. Range reads stay in the existing STABLE, security-invoker
reader from [ADR 0071](0071-route-range-reads-through-series-day-directory.md).
They must explicitly consider the retained range even when it has no historical
series/day directory. The proposed reader considers that fixed range and uses
the normal directory for new daily groups in the same statement snapshot.
Source, known-at, commit, revision and invalidation selection stay unchanged.
A missing new-day directory entry must remain an error or prevented invariant;
legacy support cannot become a silent fallback for arbitrary missing metadata.

The fixed date boundary has no column references. Its shared helper deparses
with relation OID zero, retaining the exact OID/parent/range comparison while
avoiding a read lock merely to inspect that constant. Native lock diagnostics
showed the original relation-OID deparse blocked new-fact ingestion and existing
partition provisioning behind a legacy heap lock; zero-OID deparse lets those
operations complete. This does not make canonical history reads independent of
that lock: even a recent observation-time range can include retained revisions.
The diagnostic still timed out that read while the legacy heap was locked.
Large legacy movement therefore needs explicit measured read-impact admission;
this change is not a zero-interruption movement certificate.

The range boundary must match stored data. The simplest candidate is a future
UTC storage-day boundary: prepare and validate the old range while collecting,
then drain writers for a bounded switch before admitting the new day. Existing
storage days and market clocks must never be rewritten to force attachment.
Missing that boundary requires a qualified abort, not a longer unbounded pause.
The disposable tests use controlled placement dates; they do not prove the
wall-clock handoff or its failure behavior.

## Implemented binding and read boundary

`market.fact_header_legacy` is an immutable singleton catalogue, empty in a
clean daily layout. An explicit cutover may bind the fixed
`market.fact_versions_legacy` relation OID and exclusive `end_day`; the lower
partition bound is `MINVALUE`. The catalogue never records a filesystem node,
so later qualified movement may change a file without changing table identity.
There is no arbitrary relation routing or runtime adoption.

Startup checks the exact catalogue schema, native range/parent binding,
code-owned function semantics, always-enabled mutation/truncate seals,
attached valid indexes and cloned foreign-key enforcement. Daily catalogue
checks still reject every unknown child and overlap. New-day provisioning
refuses sealed dates. A missing catalogue on an existing database requires an
explicit operator upgrade; bootstrap never creates a replacement silently.

The existing STABLE range reader consults the same-snapshot binding, considers
all retained storage days below the boundary, and combines them with the normal
series/day selection. An absent or changed bound relation raises instead of
returning incomplete history. Clean daily layouts retain their existing query.
Native global identities, correction selection and archive hydration are unchanged.

The full-copy operator's saved four-target protocol is unchanged. Its explicit
final handoff creates the new empty catalogue. These additions do not provide
a forward-cutover entrypoint or qualify deployment. The physical catalogue now
admits the exact retained range through the shared integrity checks and includes
all heap/TOAST and ordinary-index bytes in its normal inventory. The legacy
range counts toward the existing group limit, without increasing SQL deadlines.
Its newest possible day is the stable journal key and controls eligibility;
plans and completion receipts additionally bind the exclusive range end.
A changed or missing range proof refuses execution. Existing daily-only plan
hashes omit the new optional field and remain unchanged.

## Recent performance and eventual historical placement

The retained table contains some recent history at cutover. Leaving it on SSD
initially preserves its existing access characteristics and stops it growing,
but is only an intermediate deployment. Once its entire range lies outside the
saved recent window, it can become one historical movement group. Its heap,
TOAST and every index must move together under measured capacity and lock
bounds. That group is much larger than a daily partition; existing daily
movement limits cannot be assumed to cover it.

If moving that group cannot preserve acceptable collection and read behavior,
this proposal has not satisfied the storage goal. A deployment that leaves all
old history permanently on SSD is not completion. The forecast must include
the fixed legacy allocation, new composite keys, growing global identities,
new daily groups, temporary peaks, retained copies and encrypted recovery.
No unmeasured saving is credited to production admission.

## What the disposable evidence establishes

A PostgreSQL 15 mechanical probe confirmed that the old ID-only primary key
refuses direct attachment. After preparing composite primary/revision keys and
a valid range constraint, attachment preserved the heap and search-index file
identities and reused the prepared key indexes. Transaction rollback restored
the unattached source. This was a three-row mechanical test.

A separate diagnostic used QT's real tiered-v1 fixture, full header schema and
all eleven search indexes, real ingestion, correction and no-op behavior,
frozen Dataset selection, and verified Parquet archive hydration. The retained
heap and all search indexes were reused. The unchanged directory reader
returned no historical rows; an explicit fixed legacy-range branch restored
exact results. An existing frozen Dataset remained unchanged after a new-day
correction, and corrupted archive bytes still failed checksum validation.
The first diagnostic run failed on an incorrect test-side record attribute;
the corrected run passed. Startup's catalogue refusal was asserted, not bypassed.

Subsequent tracked regressions exercise the implemented binding and reader,
including real startup, no-op/correction/frozen/archive reads, missing or detached
history, disabled seals and native integrity drift. These small fixtures do not
implement the forward operator or establish broad workload coverage, concurrent query plans, full-size
index cost, HDD movement, final pause, expired-attempt retirement or production
recovery. They must not be presented as a ready deployment or a migration-time
estimate.

## Integration order

First implement the explicit fixed legacy binding and same-reader behavior,
with native integrity and startup rejection tests. Then connect complete
physical inventory and conservative movement eligibility. The supported
operator must prepare composite keys and references, reconcile the original
expired attempt through a preserving terminal transition, and qualify a new
bounded handoff with uncertain-COMMIT handling. Expired capture is never revived
or reset, and retained queues, copies and receipts are not discarded.

Only after measured preparation, catch-up, resource and pause admission may the
operator switch the real source. Verify worker retirement before exposing
recovery mounts, activate the matching runtime, and publish the complete
encrypted database/archive recovery pair. Final acceptance still includes
historical placement and recent, correction, frozen and archive read behavior.
The full-size logical restore waiver remains unchanged; no substitute full-size
restore campaign is introduced.
