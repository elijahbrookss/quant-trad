---
component: adr-online-storage-migration
subsystem: persistence
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - storage
  - migration
code_paths:
  - scripts/automation/storage_online_prepare.py
  - tests/test_storage_online_prepare.py
  - scripts/automation/server_deploy.sh
  - tests/test_server_promotion.py
  - scripts/automation/storage_online_controller.py
  - scripts/automation/storage_online_launch.py
  - scripts/ci/rehearse_online_launch.py
  - scripts/ci/rehearse_online_prepared.py
  - tests/test_storage_online_launch.py
  - tests/test_storage_online_controller.py
  - tests/test_market_data/test_storage_online_controller_db.py
  - tests/test_market_data/test_storage_online_entrypoint_db.py
  - scripts/db/fact_header_v2_cancel.py
  - scripts/automation/storage_online_worker.py
  - scripts/ci/rehearse_online_worker.py
  - tests/test_storage_online_worker.py
  - scripts/db/fact_header_v2_online.py
  - tests/test_market_data/test_fact_header_online_prepare_db.py
  - scripts/db/fact_header_v2_online_proof.py
  - scripts/db/archive_root_v2_online.py
  - scripts/db/archive_file_v2_proof.py
  - scripts/db/fact_header_v2_capture.py
  - scripts/db/fact_header_v2_copy.py
  - scripts/db/raw_mapping_v2_copy.py
  - scripts/db/fact_header_v2_handoff.py
  - scripts/automation/storage_handoff_pause.py
  - tests/test_market_data/test_fact_header_online_db.py
  - tests/test_market_data/test_fact_header_online_proof_db.py
  - tests/test_market_data/test_archive_online_copy_db.py
  - tests/test_archive_file_proof.py
  - tests/test_market_data/test_fact_header_online_references_db.py
---
# ADR 0073: Prepare storage migrations with live collection

Proposed September 26, 2026 after the user rejected a multi-day collector outage.
This decision records the required replacement workflow. Bounded copying and protected exact SQL page proofs
are implemented; a complete online operator and short final switch
remain unqualified. The existing whole-migration host hold is not the authorized
production release path.

## Problem

The fixed v1-to-v2 migration has transactional insert capture and resumable
copies, but its host operator holds clients throughout preparation. Final
verification also scans all header, identity, raw lookup and archive content
under writer fences. The saved constrained-fixture projection is over 50 hours
for only some phases. Increasing the database increases this outage.

Removing the host hold alone is insufficient. Verification, reference
relocation, archive publication, source/runtime compatibility and recovery
activation also need explicitly qualified concurrency boundaries.

## Decision

Retain one authoritative serving source throughout preparation. Prepare the
existing fixed SSD/HDD destination with an explicit operator; application startup
must not migrate it. Preserve current, historical, correction and frozen reads.

Separate elapsed migration work from collector downtime:

1. Install transactionally committed capture at a short, nonwaiting writer
   boundary. Corrections remain appended immutable revisions. An allocated
   sequence watermark cannot substitute for committed change capture.
2. Copy a finite baseline in bounded transactions. Commit each verified page
   with its durable cursor. Interruption retains committed progress and leaves
   the source authoritative. Process headers and raw lookup catch-up fairly;
   an endless header tail must not prevent raw lookup work.
3. Qualify exact background verification whose proof remains valid under later
   writes. Track or reject changes that invalidate a checked range. Counts,
   sampled hashes, a stale empty queue, and an earlier scan alone cannot prove
   final equality. Complete large reference and archive work while collecting.
4. Admit a final switch only when the remaining delta, locks, reference work,
   archive reconciliation and runtime activation fit a measured short outage
   budget. Stop/drain publishers, reconcile the bounded committed tail, switch
   atomically, and restart the matching runtime. Exceeding the pre-switch
   budget must leave the original source serving through a qualified abort.
   Once the switch commits, use explicit outcome reconciliation rather than
   blindly starting the old runtime.
5. Continue regular history placement and incremental recovery under their
   existing policy, resource guards and maintenance ownership. This is a fixed
   migration workflow, not a generic placement or rebalancing framework.

Total duration and final pause have separate reported budgets. Every retry uses
the original saved attempt deadline; no expired capture is revived. The present
96-hour ceiling is not a promised completion time. Any further prospective
change requires evidence and explicit bounded admission.

Online capacity must account for continuing source/target growth, transactional
capture backlog, WAL archival, temporary/index allocations, retained originals,
and initial/replacement encrypted baselines. The previous held-writer growth
sensitivity is not an online capacity approval. Preserve the saved reserve and
retention. Slow or nonconverging catch-up prevents switch eligibility rather than
stopping collection indefinitely.

## Implemented slice

The internal fact_header_v2_online.copy_pass accepts an already prepared,
physically bound copy and its existing resource limits. It commits a bounded
number of pages, finishes finite baselines before live tails, and alternates the
two tails. Private identity/raw relocation remains a separately admitted phase
boundary, preserving the tested SSD allocation order. The immutable retained
rollback table must already be observed on HDD when it exists.

The pass reuses exact page comparison, source change queues, filesystem
inspection, resource cancellation and the original capture clock. Its report
always says migration_ready=false and final_switch_authorized=false.
Observing both queues empty in separate transactions is explicitly not an atomic
readiness observation.

The opt-in fact_header_v2_online_proof is installed before the first copied
row, only on empty fixed shadows. Header, identity and raw inserts must exactly
match their authoritative source row. Row updates/deletes and parent/direct-leaf
truncation are refused. Routing can only expand. Immutable partition metadata,
recorded leaf object identities and exact function/trigger/definition checks
preserve each committed page's proof. Existing source immutability, transactional
capture, exact page comparison and cursor/queue retirement establish coverage;
the saved row counts report that proof, rather than substitute for comparison.

At the final source/shadow fence, completed baselines and empty committed queues
close coverage. Bounded partition/catalog/physical checks replace rereading all
header, identity and raw rows. The raw verifier accepts only the live enclosing
header proof context. The existing SQL switch removes temporary guards in that
same transaction; a failed switch restores both guards and old serving source.
An unprotected older shadow keeps the original full verifier and cannot be
retroactively certified. Privileged operators must not disable protections or
edit progress; this is not protection against a malicious database owner.

The fixed archive catalogs now have an opt-in transactional insert queue and
separate finite baseline cursors. Each bounded archive page reuses the existing
checksum/fsync copy, expiry fence, resource watchdog and original capture
deadline, then commits cursor advancement and queue retirement together.
Out-of-order commits behind a baseline cursor remain queued. A failed page keeps
its SQL progress pending while retry reuses already published immutable files.
Existing retention expiry can skip legitimately expired source objects; it does
not authorize destination deletion. Root identity and catalog/trigger drift
refuse resume. Queue emptiness remains only an observation; publisher drain is a separate
host boundary.

Archive capture retirement is explicit and transaction-bound. Only the live
verified inventory context on the same connection/transaction can remove the
temporary catalog capture/change-rejection triggers and insert function.
Completed finite baselines and an empty committed queue are required in addition
to exact file inventory. The original capture state, progress and queue remain;
a terminal receipt retains their bindings and the inventory. A closed attempt
cannot prepare or copy again. The caller includes retirement in its final switch
transaction: abort restores all triggers and removes the uncommitted receipt,
allowing same-attempt catch-up; successful commit leaves the terminal receipt.
Saved reports and contexts from another connection are not permission. The
original deadline remains enforced, including retirement. This successful-handoff path does not perform expired-attempt cancellation,
publisher drain or host activation; terminal cancellation is separate below. The actual SQL switch invokes retirement when online
capture exists, and commit_handoff accepts the caller's still-live file proof.
The proof remains owned by that caller through commit/outcome reconciliation;
no host operator uses this mode yet.

An opt-in live file proof now acquires Linux read leases on destination files
before hashing them in the background. Existing writable descriptors or mappings
refuse admission; later write/truncate attempts permanently invalidate the live
context. The original file descriptors remain open. At the final catalog fence,
every required descriptor, unsymlinked path, inode and live lease is checked,
without rereading contents. All retained bindings are checked again after caller
work and before leaving that verification context. The full catalog/metadata
scan remains and its production cost must be measured. Leases do not protect
names, replace publisher drain, or authorize root activation.

Proof is deliberately process-local: a killed or closed controller loses it.
A restart must rehash bounded existing-object pages while collection continues;
it never trusts a saved checksum or advances/resets the original capture clock.
File count, byte count, original migration/resource watchers and proof lifetime
are bounded. The process must already have the requested descriptor capacity
plus headroom; the helper never increases limits. It owns SIGIO in its main
thread, targeting lease notifications there even when a watchdog thread exists.
Kernel descriptor/inode memory and production-cardinality final metadata time
are not yet admitted. The old full verifier remains the default.

This choice uses existing Linux leases rather than changing filesystem features.
An owned file on the actual HDD refused fs-verity with EOPNOTSUPP; no filesystem
setting was changed. The guarantees and limitations follow the
[Linux lease interface](https://man7.org/linux/man-pages/man2/F_SETLEASE.2const.html)
and [thread-directed signal ownership](https://man7.org/linux/man-pages/man2/F_SETOWN_EX.2const.html).
This is a controlled operator boundary, not protection against privileged kernel,
raw-device, clock or process tampering; no saved proof survives a process exit.

Reference preparation reuses the existing separate transactions for brief
NOT VALID installation, background VALIDATE and parent adoption. Disposable
SSD/HDD qualification combines those steps with the protected SQL shadow:
concurrent native v1 header/payload insertion commits while validation owns its
locks; a newly populated leaf before adoption prevents completion until it is
validated; a leaf created after parent adoption inherits both original and
staged references. The deployed v1 partition writer is frozen as a test fixture,
without the v2-only header provisioning step. Interrupted validation preserves
earlier committed constraints, and a failed final SQL switch restores the
protected proof and validated references. After the successful disposable switch, the matching runtime appends a
correction on the new day; current application reads select that revision while
frozen cold reads and retained archive bytes remain identical. This is correctness evidence
for small fixtures, not production-cardinality timing or a live collector
throughput measurement.

This slice does not install a production supervisor, expose a new user command,
remove the archive catalog/metadata scan or authorize the old host hold. Additional indexed
source lookups for protected inserts need measured throughput and collector/query
impact; earlier unchanged-copy projections do not qualify that new cost.
Complete reference/archive preparation and a measured end-to-end short switch
remain required. This is implementation work, not a new user approval request.

## Required evidence

Use disposable actual QT data with current/correction/frozen/archive readers,
concurrent publication, out-of-order transaction commits, controller/process
interruption, lost commit replies and same-attempt resume. Test proof invalidation,
bounded locks, nonconverging tails and abort before the final switch. Measure
concurrent collector progress and disk/WAL pressure separately from copy speed.
A complete rehearsal must include the matching runtime restart and encrypted
paired recovery publication.

The user-waived full-size logical restore remains deferred and unverified. It is
not reintroduced here as a gate. Preserve the completed backup, partial restore,
keys and historical failed/canceled receipts.

Archive capture preparation requires the raw-mapping shadow to be prepared
first: its foreign key adds native triggers to the raw manifest catalog.
Sealing the archive catalog binding before that DDL would correctly reject the
later trigger change. Existing bound captures are never rewritten to accept it.

Disposable combined handoff qualification retains one owning file-proof context
through an actual protected SQL switch. Killing the database connection after
the first rename restores archive capture and SQL guards. Retrying with a lost
commit reply keeps file leases through commit and resolves the durable outcome
using the existing bounded read-only inspection; frozen bindings remain exact.
Both full-content archive hashing and full-history SQL verification are disabled
by failing test hooks for this combined case. It does not measure host downtime
or qualify the persistent host controller, publisher drain or paired recovery.


### Terminal cancellation of abandoned capture

The internal fact_header_v2_cancel.cancel_attempt accepts the original capture
start identity and a caller-owned transaction. It can detach an intact expired
or abandoned v1 attempt under a separate cumulative limit of at most 30 seconds.
It shares the existing transaction, advisory-lock and per-statement bounds;
normal migration work still enforces the original capture deadline. Source,
raw and archive writer fences are nonwaiting. A busy source refuses cleanup.

Cancellation validates the exact source/capture and any prepared physical,
archive-root and reference bindings. It removes only staged shadow-identity
foreign keys before detaching identity mirroring and temporary capture triggers.
Native source references and immutable guards stay in force. Payload parent
removal covers inherited staged leaves, including leaves created during copying.
All original data, partial copies, queues, progress, functions and timestamps
remain, plus a terminal receipt. No data scan, copy, switch, deadline renewal or
automatic replacement attempt occurs. Normal preparation, copy and switch
entrypoints permanently refuse the canceled attempt.

A rollback restores all dependencies and capture; a lost commit reply requires
read-only receipt inspection. Cancellation is not a host abort or runtime
restart instruction. The persistent online host controller must invoke and
qualify this boundary explicitly; source collection continues on the old layout.
Production-cardinality lock/admission cost remains unmeasured.


### Reading legacy archives while their owners continue publishing

The installed SSD working root is owned by UID1000 and includes private-mode
archive files owned by root. A plain UID70 reader cannot access those files;
running the legacy ownership sweep beside live publishers is not acceptable.

The internal storage_online_worker.enter_source_read_identity boundary requires
a separately mounted read-only source with an exact device/inode binding. A
single-threaded Linux process starts with only DAC_READ_SEARCH, SETUID and
SETGID, clears supplementary groups, permanently drops all user/group IDs to70,
and retains only effective/permitted DAC_READ_SEARCH with no inherited/ambient
capability and no-new-privileges. It does this before application imports that
may create threads. Unexpected capabilities, writable source, identity drift or
a threaded entry refuse admission. A transition failure is fatal, with no root
or ownership-change fallback.

This capability permits reading all files visible to the worker, not only its
source directory. The host must confine its mount inventory, exclude keys and
unrelated host paths, and reject any writable alias of the source. The helper
alone does not prove that host mount inventory. It grants no write bypass:
ordinary UID70 permissions control destination writes; read-only source mount
enforcement remains separate. Executed child programs do not inherit read
bypass. File ownership, modes and contents are never changed by the transition.

The disposable native rehearsal uses private root/UID1000 files, exact archive
copying and live file proofs, rejects write bypass/root recovery/extra authority,
and exercises an independent UID1000 publisher through its own RW fixture mount.
It is a permission/ownership boundary proof, not a running QT collector, full
production inventory admission or final ownership/runtime activation proof.
No production entrypoint invokes this helper yet; persistent controller wiring
and final spool ownership remain required.


### Persistent online controller and private command channel

The internal storage_online_controller.OnlineController owns one live destination
file proof and a dedicated PostgreSQL session lock for an already prepared,
protected attempt. It binds the original capture record, fixed placement and
archive roots. Loss of the owning connection never transparently reconnects.
Existing page transactions retain their own migration/resource locks; an
in-flight bounded page may commit before ownership loss is observed, and its
durable progress is re-admitted on restart. Closing the context closes every
proof descriptor and invalidates the dedicated session rather than returning a
session-level lock to a pool.

The fixed pipe protocol permits status, bounded SQL copy, one rotating archive
page, one rotating background file-reproof page, terminal cancellation and close.
It admits no paths, limits, arbitrary SQL, shell commands or activation request.
A process-generated controller identity and strictly ordered sequence bind each
command. Only the identical last command can replay its bounded cached response
in that process. Requests are limited to 4 KiB, responses to 16 KiB, partial
frames and stalled responses to five seconds; poll supports descriptor numbers
above 1024 without raising limits. Cancellation retains its full database
receipt and returns only a bounded summary. EOF/error closes the caller-owned
context. It does not automatically cancel capture or restart any service.

A new controller starts with zero file proof regardless of durable copy cursors.
It can rehash existing objects in bounded pages while the source serves; tails,
saved responses and completed reproof cursors never confer switch authority.
An internal database-commit method retains the same proof through real commit
and explicit bounded outcome reconciliation. Any commit exception enters an
uncertain state; ordinary commands and blind switch retries are refused.
Read-only reconciliation remains available after proof expiry and never
authorizes runtime restart. The pipe deliberately does not expose this final
method until the host stop/drain/admission path is qualified.

This is an internal controller component, not a deployed host supervisor.
Production launcher/mount admission, entry through the source-read identity,
initial preparation and old-runtime resumption, short final publisher drain,
safe host abort/re-entry, final working-spool ownership and matching runtime/
recovery activation remain required. The small SSD/HDD fixtures exercise real QT
publication and database switching, not running collector performance or measured
production downtime. Original attempt clocks and file/resource limits remain;
online capacity and production-cardinality metadata costs are still unqualified.

### Explicit online worker launch boundary

`storage_online_launch.launched_online_worker` owns the deployment flock while
an already prepared controller serves its bounded pipe. It binds exact source
clients, candidate image/source hash, database identity, request/inventory hashes,
source directory device/inode, and the original persisted capture deadline.
An interrupted create can adopt only its exact admitted container; reentry does
not renew the attempt or reuse a dead process's file proof. A running/uncertain
worker requires reconciliation rather than blind reattachment. Exit stops only
the owned background worker when its channel does not finish; no source service
pause/restart, preparation, ownership change or switch is provided.

The worker entrypoint validates the sealed request and enters the restricted
read identity before importing application code. Lifecycle output goes to stderr;
stdout is reserved for protocol. Sole PG_DSN connects through the bound database
network namespace, and prepared placement must match the fixed SSD/HDD roots.
Descriptor and memory/swap limits are explicit inputs, not evidence of measured
production admission. No hidden limit increase occurs.

Physical verification requires PostgreSQL's PID namespace and follows its process
root. Therefore omitting recovery keys from the worker's own mounts alone does
NOT exclude them: the peer's mounts can remain reachable through /proc. This
launcher refuses a database with any mount other than the exact PGDATA and HDD
roots, including recovery configuration/key/socket mounts. It also refuses source
write aliases and control path overlaps. The existing recovery-prepared production
recipe does not meet this restriction. The next host integration must resolve
that boundary without exposing keys, weakening physical proof, or changing
permissions beneath live publishers. The launcher is not ready for that recipe.

Disposable native startup qualification admits the actual Docker contract,
rejects a peer recovery-secret mount, preserves legacy source ownership, and
reaches a deliberately unavailable database with empty protocol stdout. It does
not prove successful prepared-controller startup, collection performance, full
host recovery preparation or a short outage. Unit tests additionally cover
configuration drift and interruption after create before receipt persistence.
The complete initial/final host pauses and resource admission remain release
requirements. No final switch command is exposed on this channel.

The positive host-driven entrypoint fixture now prepares real QT v1 capture and
protected shadows on distinct SSD/HDD filesystems with canonical PGDATA, source
and history paths. It starts the actual root entrypoint through the admitted
Docker arguments, drops to the restricted identity before imports, and exercises
bounded copy/reproof commands while a separate real QT publisher adds archives.
Source/frozen records and original capture start remain unchanged. Restarting the
same exited worker container produces a new controller ID and zero live proof.
The fixture database has only PGDATA/HDD mounts and archiving stays off; it does
not qualify a recovery-key-bearing database, initial/final host pause, matching
runtime activation, full launcher-context lifecycle, or production throughput.
The synthetic UUID metadata and small application fixture establish only this
entrypoint boundary. Existing native UID1000 publication/permission evidence is
separate; this positive application fixture's source owner is UID70.


The separate opt-in scripts/ci/rehearse_online_prepared.py now drives the
complete launched_online_worker context against a fresh owned cluster. Its
application fixture uses that cluster's bound database instead of creating a
different database behind host admission; the existing disposable-database
prefix and isolation guards remain. Real QT archive publication overlaps
bounded copying and live file proof while synthetic service peers remain
running. The rehearsal checks the deployment lock, exact source container
identities, saved request/resource bindings, original deadline, normal exit,
same-container reentry with empty proof, and host-exception cleanup.

Run it only with owned test image IDs, a disposable output directory and a
scratch history parent on a distinct filesystem. It generates private test
credentials, uses an internal network, and removes only its owned containers,
network, volume and scratch children while retaining logs and receipts. This
manual fixture is not part of the ordinary database suite. Its source owner is
UID70; the earlier legacy UID1000/private-file proof remains separate. It starts
from prepared SQL state without an initial preparation receipt, so it does not
qualify the combined initial-resumption-to-launch transition, production guard
installation, a running collector's performance, recovery activation, or final
outage. No production algorithm or switch authority is changed by this fixture.

### Durable online intent exclusion

The initial preparation and online launcher retain storage-online-preparation.json,
storage-online-request.json and storage-online-worker.json in the deployment state directory. The sealed request
can exist before a worker receipt or container exists. Ordinary server mutation
dispatch, direct deployment, promotion and recovery refuse the presence of any
marker independently of the process-lifetime deployment lock. Partial JSON,
canceled/expired records, directories and dangling links are unresolved intent,
not permission to restart an old database recipe. Read-only release inspection
reports the exclusion without printing private receipt content.

A stopped controller or terminal SQL cancellation does not prove that a previous
runtime and its mounts can serve retained or relocated source data. Neither path
removes these markers. Exact terminal host reconciliation and receipt retention
must be qualified before any release mechanism is added; deleting evidence is
not reconciliation. The initial preparation operator persists intent before its first source stop.
Production admission must verify that the host's ordinary deployment entrypoint
enforces this interlock. The launcher itself accepts already prepared attempts;
the separate initial transition below does not initialize capture.
This guard does not authorize a production preparation, final switch or restart.


### Bounded key-free initial preparation

The internal storage_online_prepare.prepare_online_source records
storage-online-preparation.json before the first source stop. It binds exact
source container IDs, images, private configuration hashes, mounts/networks,
source-directory device/inode/owner/mode, database cluster, fixed recipe and a
600-second deadline. This deadline starts before pausing, caps every stop and the
existing database-preparation deadline, and is retained across interrupted
replacement and partial client resumption. No legacy clock is modified.

This path admits only an uncaptured source with no nondefault tablespaces,
disabled WAL archiving, no configured archive command/library, and a key-free
PGDATA/HDD recipe. It never disables existing archival or changes ownership.
After the preserving database mount transition, it restarts only the same previously running source
containers. An initializer already exited successfully remains stopped; its
identity and completed state stay bound and it must never be rerun to resume
collection. It records completion only while the original deadline remains and
the continuously serving clients are running with their declared health checks
healthy and the initializer retains its admitted lifecycle.
Completed reentry is read-only validation, never authorization to restart a
subsequently stopped client. Interruption before completion keeps the same
deadline and both receipts; drift or expiry refuses further starts.

The held preparation receipt remains. The online launcher may coexist with it
only after exact read-only admission of the completed source preparation.
Ordinary deployment still refuses the preparation/request/worker markers.
The old held cutover entrypoint retains its behavior; extracting its lock-owned
body permits the new initial transition to hold one deployment lock throughout.

This internal transition is not a production entrypoint or a complete online
migration. It does not initialize capture, move retained data, expose a final
switch command, add private recovery mounts, activate encrypted recovery, change
the database image or complete terminal host reconciliation. Host admission must
first bind the actual deployment guard. Full production inventory admission, actual intake/query performance and complete initial/final
outages remain to be qualified; running synthetic clients cannot prove those.


The disposable initial-transition rehearsal kills its host controller after the
first exact source client restarts, then re-enters under the original deadline.
It verifies preserved cluster/table/source-directory metadata, identical source
client identities, continued synthetic file intake, retained holds and ordinary
deployment exclusion. Completed reentry leaves the receipt unchanged. This is a
real Docker/PostgreSQL boundary proof using synthetic clients and UUID metadata
on disposable host directories. It is not a running QT collector, distinct-drive
admission, packaged complete online handoff or production outage measurement.

### Atomic online capture preparation

The internal fact_header_v2_online.prepare_attempt initializes the existing
header and raw shadows, protected exact SQL page guards, and transactional
archive capture in one bounded, resource-watched transaction. It checks the
fixed SSD/HDD placement, archive roots and recent-window policy before DDL.
Raw preparation precedes archive trigger binding. Protection is installed before
any baseline rows are copied; an older nonempty unprotected shadow is refused.

Preparation may briefly fence source/catalog writers with NOWAIT. It does not
stop clients, perform bulk copies, relocate retained/reference tables or grant
switch authority. Transaction failure removes all new capture/proof DDL.
A retry validates existing identities and preserves the original capture start
and saved duration, including after a lost COMMIT reply. A new requested duration
cannot extend an existing attempt. The preparation transaction has its own
maximum 60-second bound within the original attempt and resource ceiling.

Host admission and durable host intent must precede this internal call. Retained
relocation remains explicit and must commit and be observed before bulk copying.
Private identity/raw relocation, reference preparation/validation, combined
initial-host resumption and final recovery activation remain separate boundaries.
