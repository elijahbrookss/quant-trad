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
  - scripts/db/fact_header_forward_keys.py
  - scripts/db/fact_header_forward_adoption.py
  - scripts/db/fact_header_v2_references.py
  - tests/test_market_data/test_fact_header_forward_keys_db.py
  - tests/test_market_data/test_fact_header_forward_adoption_db.py
  - tests/test_market_data/test_fact_header_forward_handoff_db.py
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

The range boundary must match stored data and the actual current UTC date.
The selected SQL handoff validates the native range CHECK and attaches the
retained table inside the same final transaction. No early committed CHECK can
outlive its controller and reject collection at rollover. Any failure rolls back
the CHECK, temporary-trigger removal, reference promotion and table renames.
Publishers must drain before the boundary; an already-written new-day row refuses
this operation. Existing storage days and market clocks are never rewritten.

This choice puts the native range validation scan inside the final pause. Its
actual physical cost must fit the existing final phase bound before production;
a small fixture does not establish that admission. If it cannot fit, this route
is not qualified and must not be stretched into an unbounded pause.

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

## Explicit composite-key preparation after terminal cancellation

The internal `scripts/db/fact_header_forward_keys.py` owner prepares the two
composite keys needed to attach retained headers. It first reconciles the exact
committed cancellation, admits the original v1 layout and preserved shadow
shapes, and binds the source heap/search-index identities. The original
capture, queues, targets and cancellation receipt are never changed.

Both unique indexes use concurrent native PostgreSQL builds while the same SQL
session retains controller exclusion. A separate preparation journal records
one original deadline for both indexes and retries. Valid publication with a
lost reply can be reconciled by exact index definition and OID; an invalid,
foreign or replaced index refuses without automatic removal or rebuilding.
Existing caller statement/lock limits are not weakened. No range check is
installed early that could reject collection when the UTC date changes.

This is an internal preparation primitive, not a production forward entrypoint.
The host still needs a bound resource watch, original phase wall/boot/monotonic
clocks and actual capacity/elapsed admission before using it. The retained
identity/raw baseline is not made current by adding these indexes. Subsequent
adoption must account explicitly for the uncaptured interval after terminal
cancellation, then prove catch-up and the final live reference boundary. No new
capture, header attachment, source switch, history placement or runtime/recovery
activation is authorized by a `keys_prepared` result.

## Retained-target adoption after an uncaptured interval

The internal `fact_header_forward_adoption.py` phase binds a separately supplied
forward operation intent to the exact committed cancellation, prepared keys and
retained identity/raw targets. It never resumes the expired attempt, rewrites its
journals or consumes its queues. Under a short nonwaiting writer fence it adds
native source-to-target mirrors and mutation guards, then records one original
adoption deadline. Existing native uniqueness and foreign keys remain enforced. Admission binds both application guards and internal foreign-key trigger definitions and enablement.

Retained identities are first read in bounded physical heap ranges and compared
exactly against source primary keys. The original heap extent and physical cursor
are journaled atomically; the existing OID/file/guard binding refuses table rewrites.
Each range covers at most 128 heap blocks and returns at most the configured page
row bound. This avoids fetching historical target heap rows in random ID order.
Once that reverse proof completes, source coverage reads only target IDs and
inserts missing exact source rows. Native insertion validation and immutable seals
preserve the earlier content proof, including concurrent inserts behind a cursor.
Missing-row insertion still gets an exact full-row recheck. Raw mapping scans keep
their existing primary-key comparison algorithm. Older scan journals cannot be
resumed as this protocol; preserving retirement remains available. Native target insertion
guards require an exact matching source row; mirrors cover subsequent inserts,
including keys behind a committed scan cursor. This covers the post-cancellation
gap without assuming timestamps or allocated sequences are commit ordered. Page
inserts and cursor advancement commit together; reentry retains the same intent,
clock and physical/catalog bindings. Header contents and search indexes are not
copied by this phase. Original source/copy/queue data and frozen references remain.

This is database preparation, not an executable production forward operation or
a final proof token. Synchronous writes to retained HDD targets require measured
collection/latency and capacity admission. The host must own physical/resource
checks, wall/boot/monotonic bounds and a preserving terminal transition on failure
or expiry; controller exit alone does not remove native mirrors/guards. Qualified
reference staging, live final proof, UTC range preparation/abort, table attachment,
historical placement and complete recovery/deployment remain required. Tests of
tiny owned two-filesystem fixtures do not establish those production properties.

The internal preserving terminal transaction `retire_adoption` removes only the
exact eight admitted mirror/validation/seal triggers, retaining all source,
target and original queue rows, functions and journals. It can retire an expired
phase without changing its start or deadline. Trigger removal and the durable
terminal receipt commit together; an interrupted transaction rolls back and a
lost commit reply reconciles the exact post-state. Retired work cannot resume.
This database primitive still requires the separately qualified host owner and
physical/resource admission; it is not a production operation by itself.

## Native references under the forward operation

The same fixed native foreign-key mechanics serve the active-copy and forward
owners. The former still requires its live original capture; the latter requires
its own unexpired adoption intent, exact binding and complete bidirectional
identity/raw verification. Neither owner can borrow the other's authority.
Each ordinary payload/archive reference is added unvalidated under a brief
nonwaiting writer fence, then validated in a separate transaction without that
fence. Original source references remain in place throughout preparation.

The forward journal records each staged native constraint identity and advances
only the exact target-side trigger changes caused by native reference publication.
The payload parent reuses validated leaf constraints. New payload partitions must
be independently validated before parent adoption; afterward they inherit its
validated reference and are admitted only through the normal native inventory.
Changed native enforcement, a foreign dependency or a replaced constraint refuses.

Preserving retirement first removes the exact staged reference roots and inherited
children, then removes the forward mirrors and guards in the same transaction.
Stopping mirrors while staged dependencies remain would strand future source
writes; rollback therefore restores the whole pre-retirement state. Original
source constraints, all records, old queues and both operations' clocks survive.
These internal steps still require the host's physical/resource admission and do
not implement final attachment, runtime activation or recovery publication.

The forward inventory also includes the retained header's composite
(id, storage_day) reference to the identity registry. Validating this native
constraint before attachment avoids scheduling its full history validation inside
the final switch. Its key, referenced key and immediate native enforcement remain
the final model's contract. The source identity mirror has an explicitly quoted
name that runs before PostgreSQL's native AFTER FK triggers; source and target
validation still occur within the same transaction. No constraint is deferred or
disabled to make source publication succeed. Forward cancellation removes this
staged header FK along with the external references before retiring mirroring.

The selected access-path change follows an actual read-only diagnostic on the
existing drives: three disjoint canonical-ID bands of 2,048 rows each took about
10.7–10.9 seconds and exposed missing retained identities after cancellation.
This was aggregate-only sampling, not a full runtime estimate. The earlier probe
used incorrect lexical anchors and repeated one warm leading range; those fast
repeats are not throughput evidence. The new physical scan and key-only coverage
still require native correctness, actual physical access/visibility and insertion
cost qualification before production admission. No cache flush, unproved saving,
extra worker speedup or weaker proof is assumed.

## Atomic retained-table SQL handoff

`stage_forward_tables` is an internal SQL phase under the live verified adoption
context. It holds the fixed source/target/reference fences, verifies completed
coverage and native references, and permits trigger removal only in that same
transaction. An unfinished context raises inside its savepoint even when an
outer caller catches the error. It promotes the retained identity/raw targets,
attaches the original header heap, reuses its search-index files, installs the
canonical v2 contracts and records a terminal switch receipt atomically.
Previously copied private headers, catalogues, raw source and journals remain.
A switched adoption refuses preserving retirement instead of addressing obsolete
relation names. This is not yet the canonical host operation or its commit
reconciliation, physical admission, recovery activation or deployment receipt.
