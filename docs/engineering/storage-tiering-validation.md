# Storage tiering implementation and validation

The target is a bounded SSD working set and growing database history on enrolled
HDDs, including historical metadata and indexes. Recent and historical queries
must preserve the same identities, revisions, ordering, known-at cutoffs, gaps,
and frozen dataset results. Adding a drive should enroll capacity for new work;
it should not require rewriting all existing objects.

The first release is frozen to the existing SSD and HDD, one automatic history
policy, correct recent/history/frozen queries, minimal Storage settings, rotated
local recovery copies with a restore test, and a measured whole-system forecast.
Existing enrollment keeps adding another HDD a configuration operation. Advanced
placement controls, automatic rebalancing and hypothetical arrangements are
deferred; they are not release gates.

Current implementation covers drive identity, allocation rules, database-backed
enrollment/review, the compact V2 page and an internal atomic movement primitive.
It is not an executed production migration. Policy application remains blocked
until automatic execution and its operational prerequisites are qualified.

## Prove the physical design

Use disposable local data to verify schema and worker behavior. Benchmark the
actual server HDD only after its identity and contents are audited and the
filesystem is prepared. Test files and databases must have separate, explicitly
named roots; never overwrite live relations or existing recovery artifacts.

Compare the current layout with candidate layouts using the same workload and
record source revision, drive UUIDs, row counts, byte counts, query parameters,
cache state, concurrency and timings. Small cached samples establish correctness,
not two-year performance. Include a sample larger than memory when feasible,
and distinguish sequential throughput from random indexed reads.

- Measure representative recent queries at p50/p95/p99 while normal collection
  and movement compete. Initial acceptance target: recent p95 within 10% of the
  matched baseline, with no sustained collector backlog.
- Query historical-only and SSD/HDD boundary ranges. Compare ordered IDs and
  content hashes, revisions, known-at boundaries, frozen datasets and missing
  data behavior. Set a cold-query latency budget from actual user workloads
  before declaring performance acceptable.
- Replay representative collector batches and measure accepted throughput,
  transaction latency, CPU, I/O wait and backlog. Require movement throughput
  above the observed arrival rate with headroom; a short burst is not proof.
- Interrupt copying, verification, location commit and source retirement
  separately. Retry after process restart and lease expiry. Verify no lost,
  duplicated, replaced or unreadable object and no stale worker can commit.
- Remove or make a test target read-only, exhaust its reserve. Expect explicit
  blocked work while existing reads remain bound to recorded locations.
  Existing additional-drive enrollment coverage is preserved; expanding storage
  arrangements is deferred from this release.
- Restore a completed, rotated recovery copy into an isolated database and
  verify referenced archive objects and representative frozen dataset results.

## Two-year partition-horizon check

Run the selection experiment with the isolated database runner:

```bash
./scripts/ci/run_test_suite.sh db tests/test_market_data/test_fact_header_horizon_db.py -s
```

It runs 32-day smoke and 730-day full-horizon cases with one synthetic row per
day plus a late correction, and exercises the actual canonical range selector. An observation on the first day receives a
correction stored on the last day. Latest selection must return that correction;
commit-frozen and known-at-frozen reads must retain the original. Payload
hydration is replaced only in this selection fixture and remains covered by the
separate storage integration tests. The fixture uses a partitioned hot table
covering its most recent 32 days and reports executed hot partitions as well.
A latest correction must join its hot payload; an older frozen row outside that
window must remain selected for cold hydration.

The report includes PostgreSQL planning/execution time for the production
directory reader and an unpruned reference using the same selector and filters.
Directory rows come from the actual ORM table and insertion trigger, not a
fixture backfill. Late/direct writes, rollback, concurrent range expansion,
missing/incompatible capture and a new-day commit between internal reads are
covered by the directory guard tests. PostgreSQL's
[STABLE function snapshot semantics](https://www.postgresql.org/docs/15/xfunc-volatility.html)
keep directory resolution and the dynamically planned header read on the calling
statement's snapshot. Separate READ COMMITTED statements do not provide that
guarantee.

The selection/guard fixtures omit extension installation in their disposable
databases because they exercise native PostgreSQL behavior. Existing migration
fixtures retain their TimescaleDB and pgcrypto setup. A function scan hides its
nested execution plan, so its executed-partition metric is unavailable rather
than zero; report total execution time, including internal planning.

These small, warmed local results reveal partition fan-out and selection errors.
They do not establish cold HDD latency, concurrent ingestion performance, full
payload hydration, realistic index size, or two-year capacity. Do not turn the
reported wall-clock times into machine-independent test thresholds.

## Time the migration before promising the cutover

Measure copy, index construction, catch-up, verification, required lock windows
and rollback separately. Use sustained rates under collection load. The user's
limit is at most 24 hours for the complete migration sequence, not just the
initial copy. If catch-up throughput does not exceed incoming writes, or total
duration exceeds that limit, stop before cutover and compare a different layout
or an explicitly approved clean start. A service restart does not imply a data
wipe. Preserve the existing data and recovery material by default.

The fixed preserving primitives reuse capture's original prepared_at timestamp
for their 24-hour attempt deadline and cumulative per-step SQL budgets. A new
connection does not restart the clock. Rehearsals must show a timed-out page
rolls back its own work, retains earlier committed progress and leaves original
collection usable. An expired attempt stops before further copying; source and
capture are retained for an explicit operator decision. This deadline guard is
not a throughput measurement or a completed final-switch implementation.

## Finish the operational boundary

Finish the fixed SSD/HDD layout, schema admission, recoverable automatic history
movement, archive resolution on the assigned HDD, and automatic recovery-copy
scheduling before enabling Apply. Reuse existing mechanisms; a drive-pool or
rebalancing framework is not required for this release. CLI and UI must call the
same backend-owned actions. Add drive enrollment must include administrator-prepared filesystem
identity and stable container mounts; it must not accept shell text from a UI.

Capacity forecasts must sum database heaps, indexes, retained raw/canonical
objects, bounded recent storage, recovery copies, migration scratch and reserve.
Use measured daily deltas, label sample dates, show an uncertainty range, and
report time to reserve as well as time to full. An archive-only estimate is not
a whole-system forecast. Local HDD recovery copies do not protect against loss
of that HDD; off-server/S3 copies are deferred by the user.

No seven-day waiting gate applies. Completion requires the concrete correctness,
performance, recovery and migration evidence above, actual deployment, physical
placement verification, and updated capacity evidence.

## Prepared HDD initialization

The host helper scripts/automation/storage_host_prepare.py consumes a reviewed
JSON plan with device (stable by-id path), expected_serial, expected_size_bytes,
filesystem_uuid, mountpoint and owner. The exact device must pass the existing
read-only audit and a SMART health check. It refuses partitions, foreign mount
points, existing signatures and conflicting fstab entries.

Initial execution requires explicit --initialize-empty-device authorization. It
creates whole-disk ext4 with the plan UUID, a 64 KiB inode ratio and zero
filesystem reserved percentage; QT's own capacity reserve remains separate.
Inode and journal initialization finish before benchmarking. An interrupted run
can reuse only ext4 with that exact planned UUID. No force-format flag is used.

The helper mounts the drive below /srv/quanttrad/storage, creates a private
benchmark directory for the named owner, and preserves an original fstab copy
before atomically adding a UUID mount. The nofail boot option lets the host
boot if this auxiliary drive is absent; eventual storage consumers must still
require UUID admission and the database service's mount dependencies. This
preparation does not activate app policy, change running containers, move the
database, or enable retention. A signature-free disk is not proof that it has
no valuable raw data; the operator must approve initialization of that device.


## Disposable catalog and filesystem qualification

The catalog and namespace verifier have passed focused disposable-database qualification.
After any currently running isolated database suite finishes, run these cases
through the supported disposable runner:

```bash
./scripts/ci/run_test_suite.sh db tests/test_market_data/test_header_catalog_db.py tests/test_market_data/test_header_namespace_db.py tests/test_market_data/test_header_history_move_lock_db.py -x -s
```

The test image includes PostgreSQL 15 server tools with distribution-managed
cluster creation disabled. The namespace fixture creates its own private
cluster as a non-root OS user, disables TCP listening and host authentication,
and uses a temporary Unix socket. Its subprocess receives no inherited DSN or
password environment. It stops and removes only its generated cluster.
The normal test database remains under the existing isolated runner.

The fixture uses real PostgreSQL control/process/file observations and two
synthetic udev UUID entries. Private PGDATA lives under /tmp and the history
tablespace under an already-mounted /dev/shm directory; their device IDs must
differ. Both generated roots are removed after the private postmaster stops.
No mount or format operation occurs. It is evidence for
catalog and namespace correctness, not actual SSD/HDD performance, backup
durability, migration throughput or deployment readiness. Do not replace its
temporary directory or socket with a server path.


The historical-lock probe holds an exclusive lock on an old header partition,
proves that the lock blocks a direct historical read, then requires the actual
series/day reader and a recent insert to finish within a one-second statement
budget. This is a minimum responsiveness gate before choosing a physical
mover. If it fails, investigate query routing or a different copy/cutover
strategy before enabling movement. A pass still requires actual concurrent
table/index movement, exact-ID paths and hot-join workload qualification.


The namespace fixture also injects transaction failures after the table move
and after the table-plus-index moves. It requires the original file paths,
physical file identifiers and row hashes after rollback. This checks the
PostgreSQL primitive only; a durable worker still needs tests for process loss,
ambiguous COMMIT results, retry idempotency and reservation reconciliation.


Private-cluster setup has a separate budget: initdb may take up to 120 seconds
on a cold local Docker filesystem, pg_ctl has a 45-second server wait, and the
worker is bounded to 300 seconds. Initialization duration is reported. These
setup budgets do not change the catalog/verifier operation budgets or the
one-second recent-work lock probe.


Qualification on 2026-09-17: all 11 catalog cases passed. The first local
namespace setup exceeded its original 20-second initdb budget; with a separate
bounded setup allowance, initialization took 42.88 seconds and all four
namespace/rollback cases plus both historical-lock cases passed in 66.89
seconds. All 17 cases also passed in CI for the implementation commit.
That CI run had a separate cleanup-fixture failure because telemetry was still
enabled in the CI service; the workflow now sets the same telemetry-off
configuration as the local test stack. The follow-up CI run must confirm that
environment correction. These results do not qualify a movement worker,
production namespace wiring, full-volume performance or server migration.


### Header journal qualification

The isolated database suite is
`tests/test_portal/test_header_journal_db.py`. It uses disposable PostgreSQL
with synthetic verified filesystem observations to exercise intent and capacity
transactions. It covers duplicate retries, rollback, advisory-lock contention,
policy/database/freshness admission, capacity changes, overlapping heap claims,
cancellation, incomplete accounting, bounded evidence and database uniqueness.
The focused run passed all 28 journal cases plus six existing storage-management
cases (34 total). The normal backend suite passed 2,931 cases and documentation
validation passed 10. These tests do not execute physical moves or certify mounts;
the full current-tree database regression remains a separate check. Running/blocked movement
reconciliation and ambiguous physical-DDL commits remain separate worker tests.


### Prepared tablespace and cross-namespace checks

The filesystem suite adds prepared default/empty custom destinations, CREATE
privilege and role checks, missing/redirected/version-mismatched directories,
changed permissions, and different source/destination mounts despite a shared
control file. It also exercises refusal of a server path that redirects through
an absolute symlink and a server-only read-only mount. These use temporary
fixtures and synthetic catalog/UUID observations. The final focused filesystem
and planner run passed 80 cases, including a worker-only alias that must not
substitute for PostgreSQL's actual catalog path.

The disposable catalog suite adds requested destination metadata and missing-OID
cases. The real private-cluster namespace fixture adds verification of empty
custom and existing default destinations before copying, and refusal of a
repointed destination observation. CI run 35205685682 at 1ca27269 passed all six namespace cases and
207 host database contracts, including destination and journal registration
checks. The earlier local full-suite image cannot certify later source changes.
Do not start overlapping local DB stacks to obtain a newer result.


### Registered destination and bound-review qualification

The pure destination-review suite uses synthetic observations to check
deterministic binding, missing destinations, changed OIDs/names/inodes,
invalid paths/identities, duplicate observations and stable versus volatile
registration fields. It and the existing filesystem/planner suites passed
101 focused cases. These checks perform no disk or database operations.

The journal database fixture now explicitly registers its synthetic prepared
destination and reviews that binding before reserving. Added transaction
cases cover immutable registration, reused identity with fresh inode/device
observations, duplicate tablespace ownership, rollback, missing registration,
changed destination reviews, persisted per-move evidence, registration foreign
keys and freshness admission. All 40 journal cases passed as part of the
207 host database contracts in CI run 35205685682. Apply remains disabled.


### Integrated registration across two filesystems

The private namespace fixture now feeds actual catalog and filesystem
observations into registration, a destination-bound review, committed
reservation, retry and unstarted cancellation. The fixture requires one
historical group, a copy budget no larger than 4 MiB and distinct source/history
devices. It checks the persisted tablespace OID, configured root, UUID and
directory inode, verifies retries do not double-reserve, and checks cancellation
releases capacity without changing source files.

After cancellation, the existing transactional DDL tests move the real table,
TOAST and ordinary index across those filesystems, including injected rollbacks.
This does not exercise a movement worker: no worker exists yet. The ten-case
extended fixture has been collected but still awaits a fresh disposable run.
Neither the synthetic HDD label on tmpfs nor successful DDL establishes physical
HDD performance, power-loss durability or full application query correctness.


### Single-group transaction qualification

The catalog suite adds cases for an exact selected heap/registry binding,
retained exclusive locks, caller rollback and timeout preservation, refusal of
a stale isolation snapshot, current index membership after caller DDL, invalid
indexes, and independence from an unrelated child's exclusive lock. A shorter
caller timeout must also bound the operation. The namespace fixture adds file
verification before and after uncommitted table/index moves on the same
connection, followed by the existing whole-group rollback checks.

Pure file/review tests require explicit single-group scope and preserve
inventory_complete=false; such observations must not authorize global planning
or reservations. The focused run passed 108 cases. The new database cases need
a fresh disposable run and are not qualified by an older image or CI commit.
This remains executor preparation, not proof of an automatic movement worker.


### Reserved-move inspection qualification

Pure comparisons cover changed physical identity, added/missing indexes,
overgrown copies, malformed stored intent, incomplete TOAST proof and retaining
members already on the destination. The combined inspection/file/review/planner
run passed 128 cases. Transactional inspection tests use synthetic physical
adapters with disposable PostgreSQL to cover plan/review/state admission,
revision drift, aggregate claims, changed observations and current capacity.

The private namespace fixture additionally composes the real locked catalog and
filesystem adapters with a saved reservation, confirms copy bytes, and witnesses
that cancellation is refused while inspection owns the storage-management lock.
It then cancels normally and checks the source remains unchanged. These new
inspection cases passed in the local disposable run: 40 journal, 24 catalog,
12 namespace and 13 inspection tests (89 total) in 376.94 seconds. The real
fixture's copy was 139,264 bytes and its private initialization took 45.453
seconds. The runner cleaned its owned stack. These tests do not exercise a
physical worker or establish WAL/temp/growth headroom.


### Atomic movement qualification

The internal movement primitive adds a completion identity and couples native
DDL, post-verification, state and reservation release in one caller transaction.
Pure checks cover logical membership, actual destination, retained files,
path identity and legitimate byte growth.

The private two-filesystem fixture now includes Python failures after table and
index moves, outer rollback after completion staging, actual backend termination
after table/index copy and before COMMIT, and a private Unix-socket protocol
proxy that withholds PostgreSQL's successful COMMIT response. A reconnect must
verify completion without DDL or a second release. Another case moves only the
remaining index of a split group, proves the existing HDD heap is untouched and
refuses an obsolete earlier completion identity.

The proxy connects only inside the fixture's owned /tmp root, uses no TCP or
ambient credentials, does not log protocol payloads, and bounds frame size and
waits. The first atomic run passed all 104 namespace, journal, inspection and
catalog cases in 420.64 seconds. The owned stack was removed. All 20 namespace
cases passed, including the real lost-COMMIT-response proof. Backend checks
passed 3,018 cases and documentation checks passed ten.

An added contention probe blocks the final capacity-row update after the files
move. It requires statement timeout, rollback of DDL and journal state,
preservation of prior caller work and restoration of the caller's timeout.
The final private namespace run passed all 21 cases in 71.49 seconds, including
this contention probe; its owned stack was removed. These tests qualify the
internal PostgreSQL primitive, not an activated worker, physical HDD durability,
full frozen/current application queries or WAL/temp/growth resource admission.


### Resource-envelope qualification

Synthetic per-filesystem tests cover shared WAL/history placement, a source
drive that cannot accommodate continued growth, missing and unknown resource
maps, stale/future capacity, invalid identities, integer overflow, cancellation
grace and elapsed observation age, copy-reservation replacement and exact
free-space boundaries. These
exercise conditional accounting only. They do not establish real producer
rates, enforce peak limits or certify PostgreSQL WAL and temporary-file roots.
The calculation remains disconnected from automatic execution.


### Resource-path qualification

Disposable file tests cover normal and relocated WAL, absolute and relative WAL
links, server/worker disagreement, unregistered paths, absent children, custom
temporary locations plus the database fallback, unwritable directories and
quoted identifier tokenization. They share the existing filesystem regressions.

The private PostgreSQL fixture adds real caller transaction/timeout preservation,
a custom temporary table allocation, stale configuration after tablespace rename,
quoted/comma-containing names and WAL relocation while the owned server is
stopped, followed by fresh observation after restart. All 113 selected database
cases passed in 474.28 seconds: 29 namespace, 24 catalog, 47 journal and 13
inspection cases. The runner removed its owned stack. The combined resource/
filesystem unit suite passed 85 cases, including directory and settings changes
during observation; the full backend passed 3,085 cases with 33 existing warnings.
These prove path observation, not peak resource enforcement or production
throughput.


### Reserved-move resource composition

The combined inspection is tested against the real private PostgreSQL namespace
and both disposable filesystems. It derives WAL and temporary targets, replaces
the existing copy claim exactly once, preserves the caller timeout and leaves
the reservation and physical source unchanged. Competing cancellation must
still refuse while inspection owns storage.

Disposable database cases with synthetic filesystem adapters cover stale,
future and mismatched resource identity, a full source drive, mutation of
caller allowance maps, a declining deadline, tighter caller SQL timeout,
invalid review and transaction ownership. Results remain conditional and
non-executable. These checks do not qualify peak limits, other-session temporary
objects, worker supervision or production performance.

The local combined run passed 57 database cases in 166.30 seconds: 32 real
namespace cases, 12 resource-composition cases and 13 existing inspection
regressions. The non-database backend passed 3,101 cases with 33 existing
warnings; documentation checks passed ten. These remain disposable-data results.


### Durable auxiliary ownership

The private atomic fixture now acquires real resource reservations before its
existing failure, backend-termination, outer-rollback and lost-COMMIT tests.
Each rollback must preserve the original auxiliary counters; verified durable
completion must release them, and reconciliation must not subtract them again.

Additional disposable database cases cover claim replay, changed limits,
caller rollback, competing ownership, insufficient source headroom, invalid
saved allocation/UUID/hash, aggregate underflow and idempotent cancellation.
These checks exercise ownership of declared space, not hard resource ceilings.

The affected disposable pytest run passed 109 database cases in 507.52 seconds,
including real backend termination and lost-COMMIT recovery with auxiliary
ownership. The backend run passed 3,105 cases; its only failure was the generated
architecture index, corrected by refreshing the index and passing all ten
documentation checks. Focused budget/inspection checks passed 85 cases.

The outer shell wrapper for that run later exited with a syntax error after
cleaning its stack: the runner source had been edited while its shell was still
executing. Its database assertions passed, but that wrapper exit was not a
successful run. Do not edit an executing runner; subsequent storage-demo runs
exercise the updated runner and cleanup path.

### First-release end-to-end demonstration

Run bash scripts/ci/run_test_suite.sh storage-demo. This explicit mode adds
only a disposable topology for the existing test runner: a full TimescaleDB
database, shared source/history volumes and the database process namespace.
The test uses PostgreSQL's UID without privileged mode. The history volume is
tmpfs to establish a second real filesystem locally, not to simulate HDD speed.
Generated test credentials, database and volumes belong to the run and are
removed by its existing cleanup trap.

The demonstration feeds synthetic observations through the real canonical
ingestion repository, freezes results, accepts a later correction, runs the
existing payload archive/reclaim executor, and moves the eligible detailed
headers and their indexes through real catalog/filesystem verification.
It terminates a backend during movement, checks unchanged data and retained
reservations, retries, and measures collection plus recent/history/frozen and combined cross-drive reads
concurrently with the successful move. The structured report records latency
and requires samples to overlap the actual copy through commit, not just the
preflight. Baseline and movement use the same concurrent application callers. A small fast copy cannot prove
full-volume HDD throughput or the one-day migration bound.

This is the next first-release milestone, not a deployment claim. The first
release is the existing SSD and HDD with one automatic policy, minimal settings,
correct cross-drive/frozen reads, rotated local recovery copies plus restore,
and a complete measured capacity forecast. Growing global identity/raw mapping
storage, one automatic execution path and the preserving migration remain
required if the demonstration shows they are not resolved. Advanced placement,
automatic rebalancing and hypothetical storage arrangements are deferred.

### Demonstrated outcomes, 2026-09-18

The final disposable application run completed successfully and removed its
owned database, history/source volumes and image. It archived and reclaimed
eligible payloads, physically moved the historical header table and every index,
and left the recent table/indexes on the source filesystem. Historical, recent
and combined reads returned the expected content and ordering. A later
correction appeared in current reads while previously frozen results stayed
unchanged. Terminating the moving PostgreSQL backend rolled back the partial
move; retry completed and released the reserved space.

All application workloads overlapped the actual copy through commit. The
fixture had 256 historical records, 56 final recent records and 647,168 bytes of
historical headers/indexes. The successful move took 0.936 seconds, of which
0.8753 seconds ran from the first copy statement through commit.

| Application operation | Largest baseline latency | Largest latency with movement |
| --- | --- | --- |
| Collection write | 613 ms | 1,134 ms |
| Recent query | 175 ms | 35 ms |
| Historical query | 1,590 ms | 2,197 ms |
| Frozen query | 1,743 ms | 2,240 ms |
| Combined recent/history query | 1,772 ms | 2,722 ms |

These are short, local concurrency measurements, not production percentiles.
The lower recent-query number does not establish a speedup. Growing latency
for collection and historical reads prevents calling performance qualified;
this small, cached tmpfs fixture does not identify the physical HDD's limit or
prove sustained collection throughput. Use a representative final-layout
workload before accepting performance; do not redesign based on this sample.

The report identifies base revision 3ab3a97a81f697116f2fb651e0738e59f12c5919 and
working source-tree hash
adf759010c4793b9866a7beae18a190295dbc7d47983c80926b8aeb528793edc.
Its structured evidence is emitted as QT_STORAGE_DEMO_REPORT by the supported
runner. This is local implementation/test evidence, not merge or deployment
evidence.

First-release blockers after this milestone:

- Growing shared identity and raw-archive lookup rows/indexes still need HDD
  placement; otherwise the SSD is still not bounded.
- Connect and qualify one automatic history policy with visible failures;
  the demonstrated primitive is not yet unattended operation.
- Complete routine rotated local recovery copies and restore referenced
  archives plus frozen results; successful movement is not recovery evidence.
- Qualify full-layout collection/query performance and rehearse the preserving
  migration within 24 hours; this fixture cannot establish either.
- Include database/index growth, archives, recovery copies, scratch and reserve
  in the measured capacity forecast; partial archive sizing is not HDD runway.

The compact settings implementation is preserved. Advanced placement controls,
automatic rebalancing and hypothetical arrangements remain deferred. There is
no seven-day waiting gate and no authorization to erase existing data implied
by this disposable demonstration.

### Fixed shared-metadata candidate, 2026-09-18

The same supported storage-demo command now compares the source-resident
metadata control with one fixed history-filesystem candidate. It adds real raw
spool publication, archive acknowledgement, record mapping and canonical
raw-reference admission alongside the existing collection and query workload.
No new product placement control, query layer or rebalancing mechanism is added.

The candidate places these existing tables and every index on the history
filesystem: fact_identities, raw_archive_record_mappings,
fact_archive_material_aliases and fact_archive_canonical_dependencies.
The concurrency fixture populates the first two. A separate existing cold-book
and frozen-feature scenario now populates all four on the history filesystem,
covering the archive aliases and dependency records absent from that fixture.

The bounded test-only preparation compares all catalog row content before and
after placement. An injected failure after moving the first table rolls back
to the original physical files; retry preserves the rows and moves all table,
index and TOAST members. This preparation is not a production migration
command and must not be used as the existing-data cutover.

Both layouts passed the complete application demonstration. New Fact identity
rows and raw mappings continued landing on their assigned filesystem. Archive
mapping lag stayed zero; published bytes and raw-reference admission remained
valid. Historical, recent, combined and frozen reads retained their expected
content, including a late correction. The existing real-backend interruption
and header-movement retry also passed with shared metadata on history storage.

For the HDD-role candidate, the largest local latency during header movement
was 40 ms for a recent query, 785 ms for a canonical collection write, 2,281 ms
for a combined recent/history query and 2,807 ms for raw publication plus its
read/reference checks. These include application work on a tiny, cached local
fixture. The history filesystem is tmpfs, not the server HDD, and the short
sequential layout comparisons do not qualify physical performance or establish
a statistically stable speed comparison. The saved physical HDD identity
benchmark remains separate evidence with its original limitations.

The result supports a fixed tablespace layout without changing QT query
semantics. It does not close the production metadata-placement blocker: the
preserving v1-to-v2 cutover, physical performance, one automatic policy,
recovery-copy restore and capacity forecast
remain necessary. No existing data, server, PR merge or deployment changed.

Candidate evidence base revision 33e6c4928b930c531c2782d807d80614a441875d; working source-tree hash
adf759010c4793b9866a7beae18a190295dbc7d47983c80926b8aeb528793edc.

The focused populated cold-family run also completed successfully and removed
its disposable stack. It reused the existing archive/reclaim/frozen-feature
scenario with the four shared catalogs on history storage. Exact frozen
features, checkpoint-versus-delta replay, known-at results and dependency holds
were preserved. Missing/corrupted required source bytes still prevented unsafe
reclamation, and unrelated raw archives remained independently releasable.
This closes the populated alias/dependency correctness gap without adding
another product execution path. It does not simulate a full local-copy restore.

The normal backend validation completed successfully. The new fixture helpers
are test-only and explicitly restricted to the disposable topology; no API,
startup migration, production table move or automatic policy was enabled.


### Local recovery rehearsal and restore ordering

The disposable recovery scenario now takes a PostgreSQL exported snapshot while
holding QT's existing shared archive-expiry fence. A concurrent collection commit
stays on the source but outside the snapshot. It copies the archive bytes, then
replaces the reader path with those copied bytes; the original files remain
separately retained and cannot satisfy restored reads.

A plain serial restore exposed a real ordering dependency: hot payload CHECKs
call market.validate_fact_payload, which looks up market.fact_schemas. PostgreSQL
does not infer that table-data ordering from the function. Restore pre-data first,
then data with the dump's unique fact_schemas TABLE DATA entry first in the TOC
list, then post-data. Keep all CHECKs enabled. Use TimescaleDB's pre_restore and
post_restore hooks; reset restore mode even when an attempted restore fails.
The earlier historical backup runbook certified its recorded schema only.

The ordered restore reached QT startup and exposed a second blocker: PostgreSQL
15 rewrites an array-wide text cast in the inflight async-job index predicate as
per-element casts. A minimal isolated dump/restore reproduced the equivalent
expressions. The startup guard now accepts only that exact additional spelling;
changed status literals (including case), null handling, columns and uniqueness
still fail. No index or constraint is disabled or rewritten by startup.

This is a recovery-proof fixture, not the routine backup scheduler or rotation
executor. A successful fixture is necessary but cannot qualify full-volume
restore time, backup creation space or a whole-system capacity forecast.

The corrected full recovery rehearsal passed on disposable PostgreSQL 15 with
two owned filesystems. QT started against the restored database; recent,
historical and frozen reads and book replay matched the captured results.
All four populated shared metadata tables retained their contents, and every
heap/index/TOAST file was verified on history storage. The later collection
commit remained on the source and was absent from the recovery snapshot.
Copied archive checksums matched and readers used copied files with distinct
inodes. The expiry fence excluded concurrent archive deletion while collection
continued. The owned test stack and temporary filesystems were removed.

This closes the small-data recovery correctness proof. Routine scheduling and
rotation, full-volume restore duration/space, the preserving migration within
24 hours, final hardware performance and complete capacity remain release
blockers. The corrected restore sequence is exercised by the fixture; it is not
yet a deployed operator or unattended recovery service. No merge, deployment,
production data movement or seven-day waiting gate occurred.

### Bounded recovery creation and rotation, 2026-09-18

The recovery rehearsal now uses the implemented local generation operation,
rather than test-only dump/copy orchestration. It publishes a verified database
dump plus the snapshot's required archive objects, retains two completed copies,
and restores the newest copy. A forced byte-budget failure preserved the earlier
completed copy. Retry removed the marked partial, and a third successful copy
retired only the oldest completed generation.

Full startup, recent/history/frozen queries, book replay and the physical
metadata/index locations remained correct after restoration. Separate filesystem
failure checks preserved unrelated files, refused corrupt archives and changed
drive identity, excluded a second writer, and resumed interrupted retirement.
The test stack was removed after completion.

This is implemented and tested locally. Runtime scheduling, shared capacity
admission and shutdown cancellation remain required before activating it.
The byte limit covers streamed dump/archive/inventory contents; filesystem
allocation overhead, concurrent growth and other jobs still belong in the
capacity budget. Recovery generations on the same HDD do not protect against
losing that drive. No new UI controls, server changes, merge or deployment occurred.

### Due recovery and existing maintenance loop

The saved backup interval/count now drive an internal due-copy service. Disposable
validation confirmed disabled policy creates no copy, competing management or
archive-expiry work is skipped, insufficient creation capacity is refused, and
a successful copy suppresses another before its next due time. Cancellation and
low source-drive headroom preserve the completed copy. Ownership is released on
every checked refusal so later maintenance can proceed.

The existing lifecycle supervisor executed the real due-copy service and reported
its result separately. Focused loop checks confirmed recovery still runs after
retention fails or is busy, a failed/blocked recovery result remains visible, and
a recovery-only loop never executes disabled retention. Shutdown cancellation
ended an active recovery attempt without leaving the recurring thread running.
The ordinary supervisor's behavior without a recovery runner remains covered.

The full populated generation/restore proof also passed with the maintenance
admission changes. The final loop integration only changes the caller and busy
outcome; it does not change the verified dump, archive-copy or restore sequence.
Backend and documentation validation completed, and all owned test stacks were
removed.

This is local capability and validation. Production composition still must supply
the verified PostgreSQL namespace, trusted utilities and measured growth/copy
budgets. The current production collector entrypoint does not supply that runner.
No policy activation, live backup, server change, merge or deployment occurred.


### Preserving the raw archive lookup on HDD

The fixed raw-record lookup copy now uses the header migration's existing
SSD/HDD binding. The disposable rehearsal killed a database backend during
copying, verified that source rows and the committed cursor stayed intact, and
resumed successfully. Real archive publication continued during copying and
after the baseline scan; captured keys brought the private target up to date.
Frozen manifests and original archive bytes stayed unchanged.

The refusal rehearsal preserved progress when it encountered an unexpected
source index or dependent view, disabled capture, damaged target content,
a misplaced lookup index or a changed filesystem identity. Preparation failure
left neither an unregistered target nor a capture queue behind.

A populated cold-book rehearsal switched headers and the copied raw lookup
together in the tiny guarded fixture. Rolling that switch back restored the old
authoritative layout. After the successful fixture switch, normal startup,
recent/history reads, frozen research and book replay agreed with their original
results. Actual relation files, indexes and TOAST belonged to the history
filesystem. Newly published raw records used the new table while retained
original rows stayed unchanged.

This is preservation and placement evidence on disposable filesystems, not
production HDD throughput or a migration-time estimate. The production operator,
remaining shared metadata placement, complete capacity budget, full-layout
performance and the complete migration within 24 hours remain release blockers.
The shared raw lookup accepts new entries on HDD too; its ingestion cost belongs
in the full-workload measurement. No server data, policy, merge or deployment
was changed.


### Supervised movement cancellation and retry

The real two-filesystem rehearsal held an already copied historical relation
inside its PostgreSQL transaction, then injected shutdown, an expired deadline,
a low-space observation and changed drive identity separately. The watcher
cancelled the active backend statement. Each attempt rolled back placement and
completion, retained the existing capacity claims for retry, and left frozen
data readable on the source drive.

The later successful execution committed the move and released its reservations.
A completed retry verified the physical result without another ALTER or release.
A separate stalled-watcher check confirmed the backend is invalidated before it
can return to the pool and receive unrelated work. No watcher remained after
the real fault/retry sequence. Backend and documentation checks passed.

This validates one supervised reserved move; it does not enable a recurring
policy or public Apply. Space supervision uses sampled filesystem availability,
not per-backend WAL/temp attribution or an instantaneous allocation cap. Actual
producer rates, cancellation margin and HDD performance still need qualification.
The owned disposable stack was removed. No live data, merge or deployment changed.

### Saved-policy history passes, 2026-09-18

The disposable two-filesystem scenario now drives historical movement from a
saved Storage policy through the existing lifecycle supervisor. It collects
records on two historical days and one recent day, freezes their results, and
moves one historical day per pass. Existing HDD placement remains unchanged and
the recent day stays on the source filesystem. Full inventory validation remains
in force despite the bounded work selection.

A real PostgreSQL backend termination after the first physical move statement
rolled back placement; the pending intent, its capacity claims and frozen reads
remained intact. A subsequent real committed move with an injected lost response
was reconciled by the next pass without a duplicate move or second release.
Disabling the policy between reservation and execution cancelled that unstarted
intent, released its claims, and left completed placement intact. Re-enabling
advanced the remaining day, then an idle pass created no new work.

Missing prepared destinations and insufficient auxiliary capacity prevented
execution; failed admission left no orphan plan or reservation. A competing
storage owner returned busy. The supervisor reported history failures and still
considered recovery afterward. Focused loop checks also preserved retention's
reported failure state when later maintenance succeeded, and cancellation stopped
a history-only loop without running disabled retention.

This closes the local saved-policy execution and retry seam. It does not activate
production runners or public Apply, establish actual HDD performance, complete
the preserving operator migration, or measure whole-system runway. Those remain
release blockers. The local topology uses separate disposable filesystems; it is
not the server's SSD/HDD pair and supplies no hardware acceptance claim.

### Minimal Storage health from existing worker evidence, 2026-09-18

The real saved-policy movement and recovery scenarios now persist their lifecycle
snapshots in the existing collector worker-state table and read them through the
Storage service. Successful history appears only for the observed policy; a new
revision makes the previous success unconfirmed, and an expired heartbeat makes
it stale. The real completed recovery copy is visible as not due. An injected
subsequent recovery failure replaces that success with failed/needs_attention.

Focused checks also prevent a fresh heartbeat from refreshing an old maintenance
result, prevent one worker's freshness from validating another worker's success,
and keep in-progress work separate from completion. The existing compact page
shows backup state independently of any last-copy timestamp, so an older copy
cannot conceal a newer failure. No new controls, tables or scheduler were added.

This is local implementation and disposable validation of the status connection.
It does not configure production runners, enable Apply, qualify actual HDD
performance, or complete the preserving migration and capacity forecast.


### Exact final-copy verification rehearsal

The fixed preserving copy now compares bounded ordered pages of every header,
global identity and raw archive lookup field while source and shadow writes are
fenced but ordinary recent/history queries continue. The rename phase requires
its separate nonwaiting exclusive fence. Its tiny handoff fixture retains the 1,024-row safety limit. Dedicated
disposable scenarios cover same-count content damage, missing/extra records,
incomplete routing, pending capture and an active writer. An interrupted check
must release its own fence while preserving outer work, source collection and
frozen archive bytes. Existing switch interruption and post-switch collection
scenarios exercise this same verification path.

This is an internal prerequisite, not the finished migration command. The final
scan still fences writers for its measured duration; there is no production
downtime claim, completed one-day rehearsal or post-resume rollback qualification.


### Fixed archive-reference placement rehearsal

The preserving migration has an internal step for its material-alias and
canonical-dependency tables and indexes. Native disposable scenarios exercise
actual two-filesystem relocation, continued source collection while the selected
catalog is fenced, database termination after heap movement, and atomic rollback
of heap/index placement. A lost commit reply must reconcile from verified
physical placement without recopying. A real allocation on the owned history
tmpfs must trip the existing space guard and restore the original placement.

Other scenarios refuse a busy catalog, wrong drive, mixed heap/index placement
and insufficient declared capacity. The preserving fixture then switches using
prevalidated references and must retain frozen results, earlier records and
records collected during movement. The limits are explicit synthetic allowances;
passing does not qualify real HDD rates, operational budgets, full source sizes,
a complete under-one-day migration or production deployment.

### Recovery after preserving handoff

The disposable local-copy rehearsal also starts from a populated tiered-v1
fixture, copies dated headers/global identities and raw lookups to their fixed
destinations, relocates archive-reference catalogs and indexes, and performs the
existing tiny verified handoff. It then commits a new observation, a correction
and new raw archive mappings before creating rotated recovery generations.

Restore uses a fresh database and independent copied archive bytes. The checks
compare recent/history/frozen reads, order-book replay and all shared metadata;
verify historical/recent partition placement; and require a new observation on a
new recent day after restore. Retained v1 rows remain unchanged, demonstrating
why they alone cannot recover writes committed after switching.

This remains a tiny fixture with its existing row guard, not a production
operator or representative migration/restore-duration qualification. It does not
qualify resuming an unfinished migration after logical restore: captured OIDs,
database identity and physical bindings would need separate explicit admission.

The post-handoff rehearsal passed locally with retained v1 rows, new v2
observations/corrections, an independent collected book session and its derived
features. Its recovery snapshot restored matching recent/history/frozen results,
both book replays, shared HDD metadata/indexes and dated header placement; fresh
collection on a new recent day succeeded after restore. This is recovery of the
snapshot, not writes committed after that snapshot or an automatic rollback.

A separate existing replay limitation was exposed while constructing the
fixture: current book state is keyed by series and is replaced when another
definition/session takes over that series. replay_book_session previously expected
the older definition/session's current state during reconciliation. The
diagnostic found the original canonical headers intact but that operational
row absent. This is not evidence of storage loss. The bounded correction now
uses retained canonical event hashes and matching immutable terminal validity,
through the existing hot/cold reader, rather than the disposable current row.
The focused rollover rehearsal releases the old lease and collects a later
session on the same series; it also checks missing current state, clean and
invalidated endings and incorrect evidence. The independent-series restore proof
alone must not be counted as this rollover proof. No new state store or replay
engine is added. Full hardware/migration qualification remains separate.
