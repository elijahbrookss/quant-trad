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
  - scripts/db/fact_header_forward_placement.py
  - scripts/automation/storage_online_forward.py
  - scripts/automation/storage_online_forward_worker.py
  - scripts/automation/storage_online_reschedule.py
  - tests/test_storage_forward_reschedule.py
  - scripts/automation/storage_online_keys.py
  - scripts/automation/storage_online_terminal.py
  - tests/test_storage_forward_successor.py
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
  - scripts/db/raw_mapping_v2_placement.py
  - tests/test_market_data/test_raw_mapping_history_placement_db.py
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

## Keep the unchanged canonical raw table

Direction selected October 7, 2026; candidate implementation, not deployed.
The current raw archive mapping table and its private replacement have identical
columns, keys and logical indexes. Replacing this authority merely to change
physical placement requires unnecessary source/target reconciliation. Retain
`market.raw_archive_record_mappings` and its OID instead. All readers and writers
continue using that same model. Preserve the unfinished private copy as closed
operational evidence; do not delete it or count its allocation as free space.

The existing stopped-worker V2 amendment can explicitly select
`raw_mapping_mode: retain_source`. It requires completed identity checks and
unchanged live guards. All identity and raw scan counters, original timestamps,
file bindings and prior receipts remain intact. Only the raw source mirror is
removed, so collection stops maintaining the abandoned copy; identity guards
remain continuous. Raw checks remain **unfinished**,
with no assertion of verified content; only the required-target selection changes.
The final transaction keeps the canonical raw table and quarantines the private
copy. The normal copy route remains compatible with existing requests/receipts.
No registry, new migration service or runtime compatibility reader is added.

This choice separates logical cutover from raw HDD placement. The canonical raw
heap and indexes initially remain on SSD and the handoff explicitly reports
`raw_history_placement_pending`. Existing source bytes already reduce observed
free space; do not subtract them again or credit their future removal. Admit
collection growth, WAL, temporary allocations and recovery through the selected
operation horizon. Long-term capacity is **not solved** by this bridge.

Before the storage goal is complete, qualify an explicit physical move through
the existing storage boundary: native tablespace relocation, finite lock/time
and resource limits, cancellation/rollback, lost-reply inspection, correct runtime
mounts and a recovery baseline for the resulting layout. No historical row-by-row
raw reconciliation is justified for a move of the authoritative relation itself.
Moving files still copies bytes and blocks access; neither a short pause nor
production throughput has been established. Do not hide that move inside the
schema switch or an ordinary read, or claim that the current candidate executes it.

The follow-up database boundary is a separate **unqualified candidate**, with no
CLI/runtime wiring or production dispatch. It extends the existing fixed physical
owner to move this one canonical raw heap and its two secondary indexes to HDD.
The measured raw primary-key index remains on SSD under `recent_lookup_indexes`;
that durable index is outside the disposable cache and 14-day window.

The existing `portal_storage_plans` journal owns its explicit request, immutable
deadline, cancellation and atomic completion. The existing `fact_storage_state`
evidence points to that completion, leaving the historical handoff unchanged.
Native catalog/file checks remain authoritative. This avoids a new table,
generic mover, second data authority or fresh row-copy reconciliation. The old
handoff inspector now recognizes only this verified post-cutover transition;
an unexplained physical change still refuses. Production use additionally needs
host/recovery integration, native qualification, measured movement and WAL costs,
and bounded collection/spool impact. It does not amend the running cutover.

Remaining cutover work is concrete: native references to the adopted identity
table, archive inventory/copy catch-up, reference-catalog placement, the final
header range check/attachment, application activation and recovery. Finished
identity proof is reused only while its guards remain continuous. Stop the worker
through its existing process owner when replacing it; **do not retire the adoption
or remove identity mirrors merely to stop obsolete raw scans**. Retirement would
create another unproved write interval. The cutover still obeys its bound and
actual UTC range boundary; this decision does not authorize a renewed clock.

## Successor ownership after preserving retirement

Candidate implementation: a separately supplied operation may use the existing
adoption owner after an exact predecessor retirement is reconciled. Its adoption
row, trigger functions and archive capture/progress/queue live in a private
namespace derived from its operation digest. The first operation keeps its
original namespace and serialized binding. No predecessor row, queue, clock or
terminal receipt is reset or renamed. Readers select the explicit operation;
they never infer the newest attempt. The successor pins the predecessor row's
OID and content digest and rejects drift.

This extends the existing temporary migration owner because its singleton
records cannot represent a new attempt without overwriting the retired one.
It adds no market-data authority, generic job registry or runtime migration.
Retained target rows and source keys are reused, while a fresh bounded proof
checks the uncaptured interval and retained content. The prior cursor is not
trusted across a period without its guards. Setup, mirrors and proof publication
remain transactional; an interrupted setup rolls back, and a lost commit reply
reconciles the same new operation and original new deadline.

The candidate host route extends the existing migration command and fixed
terminal worker. An explicit lookup package names the retired predecessor and
its exact terminal receipt, then places only the three selected indexes under
the existing physical resource owner. A separate host intent bounds transport
and reconciliation; the SQL receipt still owns the atomic move and its original
clock. The command reconciles an uncertain completion before considering another
dispatch. It does not publish a successor or start collection mirrors.

A v2 forward package explicitly names that completed placement, predecessor and
terminal-file digest. Publication preserves the previous publication, launch,
terminal and failed-recovery records. The new publication, launch, initializer
and terminal records use the new operation identity. Active request/worker/runtime
files accept only journaled old/new preimages. Reentry retains the same clocks;
a foreign preimage or expired incomplete phase refuses. Completed keys are
reused without rebuilding. The host and confined terminal select the same exact
SQL adoption; no reader chooses the latest operation.

This handles the current first-successor transition, not an arbitrary chain of
migrations. Old all-HDD receipts retain their meaning through the explicit
three-index transition in [ADR 0070](0070-separate-global-fact-identity-from-dated-headers.md).
The implementation is a candidate pending its exact-image host rehearsal and
production resource/collector-impact qualification. Final switch, paired recovery
and application activation remain separate gates. Passing these tests does not
renew a retired attempt, start research or establish production performance.

## Reschedule a stopped worker without discarding guarded proof

The V1 operator supports one explicit reschedule of an active successor
after its worker has stopped, while the source still serves. A later UTC boundary
must fit the original adoption expiry, including the final pause allowance.
The original operation digest, request, SQL start/expiry, proof cursors, mirrors,
seals and reference/archive ownership remain bound. A qualified replacement image
and a finite increase in archive object count and matching descriptors may change;
bytes, memory, page limits, resource floors and other budgets do not.

The existing initializer owns the revised request binding. The existing host
publication owner retains the original plan/publication/launch preimages in one
operation-specific amendment journal. This is temporary migration metadata, not
a new dataset authority. Ordinary startup cannot amend it or backfill data.

Publication takes the deployment lock, checks actual worker retirement and source
identity, admits fresh capacity, and changes the initializer under controller and
storage exclusion. It verifies that only the binding changed. Lost SQL replies
are inspected without redispatch; partial host publication accepts only recorded
old/new bytes within its original five-minute publication bound. Unresolved or
expired incomplete publication refuses normal launch. The original worker and
all evidence remain retained. A separate canonical invocation starts the qualified
replacement under every original wall, monotonic and boot clock.

The follow-up V2 candidate allows one explicit extension after a completed V1
amendment, normally before the current adoption expires. It binds the exact preceding
amendment and request, and increases the cumulative adoption duration to a
declared bound of at most 96 hours from the original SQL start. Fresh capacity
must cover that new expiry. Adoption duration/expiry, the archive capture's
matching expiry and the existing package/schedule binding change atomically;
original start times, completed proof, archive cursors/queue identities, guards,
initializer clocks and the five-minute publication bound remain intact. The
previous amendment is retained alongside one V2 journal. Ordinary retries cannot
revive expired work; this is not an indefinitely renewable migration allowance.

The explicit V2 option `continue_guarded_proof: true` (candidate, not deployed)
permits that same amendment after the work allowance expires, only with
`raw_mapping_mode: retain_source`. Complete identity proof, an unretired adoption
and exact native table/file/guard/reference/archive bindings are required.
ALWAYS mirrors and immutable guards preserve the proof while the worker is
stopped. Expiry alone need not force another historical scan. Missing or changed
guards, retirement, incomplete identities, reboot and uncertain publication
still refuse. The canceled original capture is never reactivated.

The original start, proof counters, queues, receipts and five-minute publication
clock remain. The maximum is still 96 cumulative hours, with a fresh capacity
forecast and a future boundary inside the new allowance. Old packages retain
the pre-expiry rule. No further deadline amendment, automatic renewal, larger approved
allowance, new registry or market-data authority follows from this option.

This exception addresses measured preparation that misses its first revised
window without making another full-history verification the recovery path. Its
cost is longer-lived guards, queues and retained allocations; admission must
budget those costs without assuming cleanup. Ordinary reads and research gain
no migration authority. The V2 candidate requires native SQL and confined-host
qualification before an explicitly admitted production transition.

This avoids repeating validated history merely because the schedule changed. It
does not rescue a retired adoption or guarantee that the remaining work will
fit. The guarded option above is the sole exception to pre-expiry amendment. Native concurrent-publication/rollback checks and the confined Docker
reschedule/reentry/retirement rehearsal are release gates. Production use and
throughput remain separate qualification; this candidate is not deployed by
documentation or by passing tests.

The V3 candidate permits one package correction after completed V2 raw retention.
This addresses a qualified worker defect without coupling its replacement to
another schedule extension or historical scan. The existing publication owner
binds the exact V2 predecessor, stopped worker and replacement image; the SQL
initializer changes only candidate request provenance. Dates, all clocks, budgets,
physical bindings, data and completed progress remain fixed. The cost is one
additional receipt in the existing private publication journal. Original bytes
remain recoverable, uncertain SQL completion is observed without replay, and
partial file publication resumes only inside its original five-minute window.
An expired adoption still refuses. Native rollback, confined replacement/reentry
and lost-reply qualification are required before production use; the correction
does not by itself admit the final pause or deploy the release.

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
bounded handoff with uncertain-COMMIT handling. Canceled capture is never revived
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
Missing-row insertion still gets an exact full-row recheck. Older identity scan
journals cannot be resumed as this protocol; preserving retirement remains
available. Native target insertion
guards require an exact matching source row; mirrors cover subsequent inserts,
including keys behind a committed scan cursor. This covers the post-cancellation
gap without assuming timestamps or allocated sequences are commit ordered. Page
inserts and cursor advancement commit together; reentry retains the same intent,
clock and physical/catalog bindings. Header contents and search indexes are not
copied by this phase. Original source/copy/queue data and frozen references remain.

The raw-mapping candidate applies the same exact-target-first proof, with bounded
physical scans on both sides. Source heap order keeps archive-object groups
together while inserting missing mappings; raw-ID order scattered a measured
4,096-row sample across 4,059 objects. Physical samples of about 2,560 rows covered
7–19 objects. These samples motivate the change but do not establish production
throughput. Key-only target coverage follows the completed content proof, and
missing rows still receive exact comparisons. The explicit adoption job records
physical extents without resetting prior key cursors, completed passes or clocks.
No index movement, deadline extension or worker replacement follows from this
query change; each remains subject to its existing operational admission.

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

## Archive interval proof for forward adoption

Forward preparation reuses the existing bounded archive page and inventory
mechanisms with fixed separate journal and queue names. It binds the forward
adoption intent and its original deadline, not the retired capture. A catalog
insert trigger covers records that commit behind an earlier ID cursor. Original
canceled queues and completed files remain evidence, never fresh copy authority.
Final capture retirement requires the same transaction's live inventory for
that exact forward operation. Preserving cancellation also stops archive capture
without consuming its queued rows or deleting any files. This lifecycle belongs
to the same adoption terminal transaction, including rollback and reentry.
The host integration and production performance qualification remain outstanding.


## Durable outcome under the existing final owner

Forward adoption uses the existing supervised commit/certificate and initial
policy boundaries. The exact parent/catalog identities are known only after the
atomic attachment, so their certificate is published later in that same SQL
transaction, before COMMIT. An unfinished switch has no externally visible ready
state. The receipt binds the retained heap, archive proof and original adoption
journal digest; finite baseline counters are not advertised as current row totals.

The existing outcome inspector validates this explicit receipt form, native
legacy seal and archive retirement while retaining the original ownership fence.
It can reconcile a lost reply after the work deadline without restarting work.
Source file identities are switch-time evidence, not a ban on later policy-owned
historical HDD relocation. The fixed catalog mover accepts the same forward
intent and actual owning connection. Canonical host/controller wiring and physical
admission are still required before this internal boundary can be deployed.


Forward orchestration retains one controller-owned SQL session across page,
reference, catalog and final work. Reopening a separate pooled session would
conflict with its controller lock and cannot establish equivalent ownership.
The existing controller therefore selects this mode explicitly and preserves
its shared resource, publisher and final-deadline guards. The host route must
bind this session choice to its exact forward operation; the old host route's
two-session reply rule is not silently weakened.


Concurrent key builds use the existing resource watcher on the actual owning
SQL session, including AUTOCOMMIT and progress publication. Fixed physical
placement, declared maintenance/temp/WAL/growth allowances and existing claims
are freshly admitted under the storage session lock. Cancellation preserves
completed indexes and original deadlines; invalid partial indexes still refuse
without repair. The host route must select the supervised entrypoint and supply
measured allowances. Disposable cancellation/reentry tests cannot establish
production build duration or collection impact.

The final-session wire observation now includes the exact forward intent,
committed cancellation intent, original adoption start/expiry/duration and UTC
end day. The host login and gated rollback verifier permits equal owner/backend
PIDs only when its already-admitted binding contains that exact, valid forward
identity. A reply cannot opt a legacy host binding into forward mode; absent,
changed or malformed identities refuse. The original capture remains separate
evidence and never supplies the forward work clock. This wire integration does
not yet create the canonical forward publication/request/worker authority or
admit a production source stop.


### Confined worker preparation clocks

The fixed worker entrypoint explicitly selects the forward request after its
existing image, source-read identity and database checks. It calls supervised
composite-key preparation under one persisted background deadline (at most
3600 seconds). Only after verifying the complete keys and their exact OIDs does
it persist a separate initialization receipt (at most 600 seconds), before
installing adoption or archive guards. It never calls expired capture preparation
or changes the original capture/initial receipt.

The initializer retains its actual SQL session and storage/controller exclusion.
The existing physical inventory and resource watcher bound that same session.
Adoption, archive interval capture and the complete initialization receipt commit
in one transaction. Interruption rolls them back while retaining the initial
intent and completed keys; reentry keeps their original clocks. A lost COMMIT
reply reconciles the same complete record. A complete initializer verifies the
original active adoption instead of building keys or starting another clock.
Foreign requests, changed keys, expired incomplete initialization and retired
adoption refuse. Source/copy/queue rows and original cancellation remain intact.

This connection does not admit the canonical host launch. Host verification of
the completed publication/cancellation, original retired worker, actual new phase
clocks and login-gate authority remains required, as do production physical and
pause qualification, recovery and deployment. A direct worker or small disposable
fixture result cannot grant those authorities or establish production performance.


The host publication inspector reconstructs the effective forward request from
its completed publication, original operation and committed cancellation. It
checks exact published request/runtime/plan bytes, retained inventory, and the
permitted worker-binding changes. The original plan remains immutable; a
recomputed journal digest cannot substitute foreign runtime or source bindings.
Worker lifecycle progression still belongs to the launcher and is not granted
by this read-only inspection.

The host can read initialization and adoption together in one database snapshot,
without scanning historical rows. It rejects absent, ambiguous or retired
adoption. Request/cancellation identity and actual initialization/adoption clocks
must match exactly. Completed initialization need not remain unexpired; the
original active adoption expiry controls subsequent work. This observer does
not renew either clock or implement canonical host launch, login closure or
preserving terminal invocation. Those lifecycle integrations remain required.
