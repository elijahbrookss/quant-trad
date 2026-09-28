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
  - tests/test_storage_online_runtime.py
  - scripts/automation/storage_online_runtime.py
  - scripts/provenance/source_tree_hash.py
  - portal/backend/Dockerfile
  - tests/test_storage_online_repositories.py
  - scripts/automation/storage_recovery_prepare.py
  - scripts/automation/storage_online_repositories.py
  - scripts/ci/online_guarded_source_fixture.py
  - src/core/storage_writer_fence.py
  - portal/backend/run_backend.py
  - portal/backend/workers/single_node_initializer.py
  - tests/test_market_data/test_storage_writer_fence.py
  - src/market_data/archive.py
  - src/core/settings.py
  - tests/test_market_data/test_archive_shared_ownership.py
  - scripts/automation/storage_host_boundary.py
  - scripts/automation/storage_online_drain.py
  - tests/test_storage_online_drain.py
  - tests/test_market_data/test_storage_online_collector_drain_db.py
  - tests/test_market_data/tiered_v1_ingestion.py
  - scripts/automation/storage_online_final.py
  - tests/test_storage_online_final.py
  - scripts/automation/storage_online_prepare.py
  - tests/test_storage_online_prepare.py
  - scripts/automation/server_deploy.sh
  - tests/test_server_promotion.py
  - scripts/automation/storage_online_controller.py
  - scripts/automation/storage_online_launch.py
  - scripts/ci/rehearse_online_launch.py
  - scripts/ci/rehearse_online_prepared.py
  - scripts/ci/online_start_reply_fixture.py
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

## Source runtime lifetime interlock

A deployed-source rehearsal demonstrated that closing target database logins can
still leave the old backend serving its API briefly. SQL login closure therefore
cannot establish filesystem publisher exclusion.

The three existing source entrypoints (backend, collector and initializer) now
support the internal operator input `QT_STORAGE_SOURCE_FENCE_ROOT`. When supplied,
it must name the existing canonical, co-located source archive/working directory.
They retain shared nonblocking directory-flock ownership before starting work.
The backend passes that same open file description to its supervised children;
parent exit does not unlock surviving children. Missing/replaced/aliased roots,
a split source layout, or an exclusive operator hold refuse startup. Without the
input, ordinary startup remains unchanged. No marker, source permission change,
new directory, public command or schedule is added.

This is a cooperating, image-qualified process boundary. A preserving preparatory
source release and exact image/entrypoint/environment/root admission are required
before relying on it. The internal final host retains the exclusive side using
`held_source_writers_locked` and the existing archive namespace boundary. It
binds the three fixed source commands, image, root and environment, then retains
the actual directory inode hold across its caller's transition. Its callback
rechecks original clocks and root metadata; it cannot be reused after exit.
The preparatory release and complete publisher/operator admission remain
unfinished. A saved observation is never lock authority.
Arbitrary tools, unqualified images, alternate entrypoints and privileged path
replacement are not excluded by this helper. Database gates, live proofs, original
deadlines, durable intent and uncertain-outcome handling remain separate required
boundaries. Kernel release after owner death never authorizes automatic recovery.


### One-shot held switch and fresh outcome

With the optional source fence configured, backend startup checks the existing
shared database readiness contract before spawning any API or worker. It uses
the existing worker startup timeout and fails nonzero when unavailable. This
closes the observed late API startup after the exclusive owner's death while
target logins remain closed. It does not introduce a second DSN or change
ordinary unconfigured startup. Collector and initializer keep their existing
database admission. This component evidence does not establish full outer-host
loss recovery or exclude alternate publishers.

`commit_online_handoff_locked` requires that live host source context, confirmed
login closure and job retirement, the same worker/session and the original final
deadline. It rechecks the source and gate and writes `commit_dispatching` into
the existing final receipt before sending the internal `commit_database`
command. The worker requires the retained SQL connection, fresh sequence and
original bound deadline. It checks source namespace exclusion before and during
the existing guarded switch. The host remains responsible for admission of all
publishers; a contended inode by itself cannot certify who owns it.

A fully received reply is followed by fresh `inspect_outcome` on that same
worker, connection and proof. A lost SQL COMMIT result is explicitly reported as
unknown, logged, consumed once and inspected without repeating COMMIT. Only a
fresh committed outcome advances the existing receipt to `committed`. An unread
or malformed pipe reply, failed inspection or uncommitted outcome retains the
unresolved dispatch intent; no replay, source restart or automatic reversal is
allowed. The original initial, capture, wall, boot and final monotonic ceilings
are never renewed. Both commands return no runtime or collection-start authority.

The disposable real SSD/HDD integration exercises actual gate closure, bounded
residual copy, guarded SQL COMMIT and fresh outcome, including a deliberately
lost real COMMIT result. Host Docker observations and the command exchange are
controlled adapters in that test; it does not qualify a complete Docker-pipe
operator, production publishers, source-image rollout or recovery activation.
Original capture and frozen records survive. Separate source-start tests cover
the actual guarded entrypoints after loss of the kernel owner with logins closed.

The connected Docker rehearsal now sends that same one-shot command over the
actual private attach pipe, observes the committed SQL state, closes the worker
and verifies its container stopped with PID0 before releasing the source hold.
Its three source peers are explicitly synthetic images using the actual lifetime
fence; an independent genuine QT publisher exercises copy/catch-up before final
pause. This proves the host/worker connection and retirement ordering, not the
production source fleet, post-switch recovery or a production pause allowance.
The fixture's closed-login restoration occurs only after verified retirement
and remains teardown, never production activation authority.

The existing launcher has a caller-locked internal context for this transition.
The ordinary `launched_online_worker` wrapper still acquires and retains the
same deployment lock through verified retirement. The complete host workflow
can hold that lock around `launched_online_worker_locked`, then retain it after
worker exit while separately admitting preserving recovery. Source and worker
identity checks, cleanup and original deadlines are shared unchanged; no second
launcher policy or public command is introduced.

Before recovery keys or repositories can be mounted, the read-capability worker
must still be independently verified stopped and reaped. A separately bounded
preserving transition from the already HDD-mounted database, matching runtime,
retained-WAL recovery and encrypted paired publication remains required. Neither
new final phase authorizes ordinary deployment or deletion of the retained marker.

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

## Host boundary ownership

Shared host admission, bounded Docker calls, the deployment flock and durable
receipt I/O live in `storage_host_boundary.py`. Online launch and final transition
no longer depend on private functions in the historical held-copy orchestration.
Initial preparation still reuses its preserving database preparation procedure.
Phase owners retain their existing receipt schemas, transitions and original
clocks; the shared module supplies no command or new switch/recovery authority.
The [storage component workflow map](../persistence/STORAGE_MANAGEMENT.md#one-online-migration-workflow-and-its-owners)
records the owners and the unfinished integration. This is a bounded dependency
refactor, not a new workflow framework or a second operator path.

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
page, one rotating background file-reproof page, fixed preparation steps, spool
observation, bounded final tails, read-only outcome inspection, terminal cancellation
and close. Explicit phase
and final deadlines can only shorten original admitted ceilings. It admits no
paths, arbitrary SQL, shell commands or activation request.
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
UID70; the earlier legacy UID1000/private-file proof remains separate. The default fixture mode starts
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
Private identity/raw relocation and reference preparation/validation use the
explicit steps below. Initial-host resumption and final recovery activation
remain separate boundaries.


### Initial preparation through background worker admission

The opt-in prepared-host rehearsal also accepts --prepare-source. This starts
with a fresh owned database exposing only PGDATA and synthetic serving clients.
It runs prepare_online_source, verifies the preserved cluster and original
600-second receipt, and resumes the same clients before invoking atomic SQL/raw/
archive capture preparation. The full launcher then admits the retained hold
through that completed source receipt. Receipt bytes remain unchanged across
background copy, concurrent QT archive publication, worker reentry and host
exception cleanup. The completed initializer stays stopped and synthetic file
intake continues.

Real QT publication runs in a separately owned application fixture; it is not
the synthetic collector service. Source directories use UID70, and disposable
history permissions allow both the host fixture user and database user to pass
the normal writability admission. UUID evidence is synthetic; the optional
distinct-device check independently verifies SSD/HDD separation. These facts
do not establish production permission admission, collector/query performance,
production-size outage duration or complete final recovery activation.

After atomic preparation, the --prepare-source fixture uses bounded SQL copy
passes and explicit preparation_step calls for private identity/raw relocation,
identity mirroring, individual reference preparation/validation and parent
adoption. Fixed retained/reference catalog moves remain separately invoked
through the existing catalog mover. Initial archive copying belongs to the live
controller. The default already-prepared fixture retains its older finite setup.

### Explicit online preparation steps

fact_header_v2_online.preparation_step admits one fixed operation per transaction.
The caller binds the original capture start, placement, automatic policy and
resource allowance. Protected shadows and the recent-window boundary are checked
before work. The committed header baseline precedes private identity relocation;
raw copying and relocation follow it, then identity mirroring and references.
Retained data must already be observed on history before these steps.

Each move or validation is explicit, with its own requested duration inside the
original cumulative attempt and resource ceiling. Long validation does not acquire
the source writer fence used for brief reference installation. Earlier committed
steps survive a later failed transaction; retries inspect actual placement and
constraint identities. A step report never grants switch authority. These internal
calls add no host pause, ownership change or wire activation command.

Disposable qualification inserts native v1 records while reference validation
holds its locks, kills a later validation backend, and confirms that earlier
validation commits, original capture time and frozen records survive. The combined
host fixture runs these steps after source resumption and before live controller
copy/reproof. These small fixtures do not measure production lock impact or
collector throughput. Production still requires admitted phase resources,
complete final stop/drain/switch, safe abort/reconciliation, and recovery activation
after the read-capability worker exits.


### Separately bounded final database transaction

OnlineController.commit_database requires an explicit absolute monotonic deadline
from the caller that owns final publisher drain and the short host pause. It does
not inherit the background page-command allowance. The deadline must still be in
the future and inside both the original admitted resource duration and live
attempt ceiling. Background commands retain their original short allowance.

The handoff receives the remaining resource duration and the same absolute
deadline; the latter only shortens its existing transaction/watch ceilings.
Time already spent stopping or draining clients is not granted again on entry.
All original capture, resource, source, lease and transaction guards remain.
An error after entering the switch remains commit_unknown and requires bounded
read-only reconciliation; an invalid deadline grants no switch authority.

This is an internal caller boundary, not a final wire command or measured
production pause allowance. The eventual host transition must retain its original
deadline across interruption and admit the whole stop/drain/delta/switch/runtime
sequence. The small database qualification uses a deliberate SQL delay to show
deadline rollback and a successful transaction longer than the page allowance;
that artificial duration is not a migration estimate. Full catalog cardinality,
source-drain and runtime/recovery timing still require measurement.


### Explicit preparation through the persistent worker

The background pipe accepts a named prepare_step with an exact step, optional
incoming-reference relation, and explicit maximum duration. It delegates to the
existing fixed preparation_step registry; it does not accept arbitrary SQL,
paths or relocation targets. The requested duration must fit the original
admitted resource allowance. Each existing phase still rechecks original capture
time, protected placement, policy, ordering and resources.

Private identity/raw relocation, mirroring and reference preparation/validation/
adoption are explicit commands. They do not widen short SQL/archive page or
status limits. A same-process identical reply can be replayed without another
mutation. A failed command closes further work; a replacement controller admits
durable progress and starts with no file proof. Source clients are not paused or
restarted by these commands. Retained/reference catalog movement remains a
separate operator boundary, and no final-switch/runtime command is exposed.

Disposable controller qualification traverses the finite header/raw phases,
restarts after committed identity relocation, publishes additional QT archives,
then prepares/validates/adopts references and catches archive intake. It checks
original capture/frozen preservation and refusal of excess phase duration.
This component qualification is not a complete packaged host phase driver,
production lock-impact measurement or final outage admission.


The optional owned host rehearsal adds --worker-phases with --prepare-source.
After initial source resumption and atomic capture, it sends explicit private
moves, identity mirroring and reference preparation/validation/adoption through
the launched worker's real pipe, alongside real QT publication and bounded copy
commands. The application fixture supplies its incoming-reference names only
after mirroring; this private diagnostic channel is not a production operator
discovery interface. Retained/reference catalog moves remain explicit external
fixture phases. Existing source receipts, empty-proof reentry and worker-only
exception cleanup are still checked. Synthetic services/UUID metadata/UID70
directories and small data do not establish production collector performance or
the final pause/recovery sequence.


### Rollback ownership across source-resumption admission

The internal OnlineController.rollback_source_fence keeps the existing migration
advisory lock and ACCESS SHARE locks on the original header/raw relations while
the host admits and resumes its exact old clients. A fresh negative handoff
inspection, unchanged original capture, protected SQL/raw layout, archive capture
and source root are required. A pending or committed switch refuses the fence.
Native source inserts can continue; another cooperating migration and relation
renames cannot race the enclosing transaction.

The caller supplies its original absolute final deadline inside the admitted
resource duration and calls the yielded live check before and after each bounded
host action. Connection loss, expiry, root drift and context exit invalidate that
check. The controller becomes terminal after abort, so it cannot later switch
under already resumed source clients. No serialized reply can recreate this
ownership. Abort inspection may outlive copy expiry but neither renews the
capture clock nor permits preparation, copying or switching.

This database seam does not authorize collection resumption by itself. Exact
host client/image/configuration/cluster admission, durable final intent/deadline,
interrupted stop/resume and supervision of an in-flight host action remain
necessary. There is no wire restart/switch command or production entrypoint.
Loss of the fence while a host action is running must be handled by that
qualified host transition; the database check alone cannot stop Docker actions.


### Durable final source-stop boundary

The internal storage_online_final.stop_online_source_locked runs under the
existing launcher's deployment lock after completed initial source resumption
and live worker admission. It persists a separate final intent before the first
stop. That intent binds the original source preparation, request/capture, exact
worker container/start/PID and controller greeting identity. Its explicit duration
must fit the original resource and capture ceilings; no production allowance is
selected automatically.

The original wall-clock and Linux boot-time deadlines survive re-entry. A changed
boot, backwards clock, expired window, changed bindings or requested duration
refuses further stops. All nested Docker observations/actions share the remaining
absolute host budget. Initial600-second preparation and original capture receipts
remain unchanged. The same live worker is retained; restarting it is not an
admissible continuation of this final intent.

Only exact previously serving source clients are stopped; the completed initializer
stays stopped and passive services remain running. Docker receives a graceful stop
with no forced-kill timeout. The host still times out at its original deadline:
a lost reply may leave a daemon stop in flight, so intent remains and no drain,
switch or resumption is claimed. This follows the documented
[Docker stop timeout behavior](https://docs.docker.com/reference/cli/docker/container/stop/#stop-container-with-timeout--t---timeout).
A completed stop can be re-inspected without rewriting its receipt; unexpected
client restart refuses. Ordinary deploy/recovery and background worker relaunch
independently refuse the final marker, including corrupt/partial markers.

This helper supplies no SQL switch, source resumption, recovery mount transition,
terminal marker removal or production entrypoint. The complete host flow must
still combine publisher/spool drain, same-worker delta/COMMIT/reconciliation,
the live rollback fence with supervised exact source restart, and matching
runtime/recovery activation. Host expiry or uncertainty remains held. The optional
owned --final-pause rehearsal exercises interrupted stop re-entry; it is not a
complete cutover, running production collector test or outage estimate.


### Read-only spool observation during the final hold

The source_drain pipe command observes the fixed source working-root spool through
its already admitted read-only mount. It walks by directory descriptors without
following symlinks, bounds depth/entries and checks an absolute deadline inside
the unchanged short command and live proof ceilings. Open/sealed WAL, partial
acknowledgements and unknown files remain pending, even beside an acknowledgement.
Only .ack.json sidecars are counted separately; their contents never authorize
WAL deletion. No file is opened for repair, modified or removed. Directory/path
replacement, nonregular entries, filesystem drift and exceeded bounds refuse.

The host observe_source_drain_locked boundary re-admits the exact paused source
and same live worker before and after the request under the original final
wall/boot window. It does not rewrite receipts. Observations require fresh
sequences and are not replayed as current. A clean result is momentary and always
reports publisher_drain_authorized and final_switch_authorized as false. It does
not prove no in-flight database transaction or unpublished object, perform final
catch-up, switch roots or authorize collector resumption. Pending WAL remains for
normal source recovery; forced cleanup is not an admission mechanism.

The owned final-pause rehearsal observes the real fixture spool, injects one
owned sealed diagnostic segment plus acknowledgement, verifies refusal to call
that spool empty and preservation of the bytes, and removes only its diagnostic
files. Synthetic peers and small fixture data do not qualify actual collector
shutdown, production spool cardinality or complete downtime.

### Collector finalization qualification

The disposable online-capture fixture runs ContinuousStreamRuntime with the real
Coinbase parser, trade projection, archive publication and PostgreSQL repository.
Only the transport supplies scripted frames. The preserved v1 source uses its
frozen deployed ingestion method, v1 partition provisioning and v1 read clause;
the candidate v2 writer cannot serve that schema before the switch.

A blocked canonical acknowledgement keeps the runtime stop pending and the sealed
WAL present after archive publication. A failed acknowledgement must propagate
failure and retain the segment. Normal runtime startup recovers it before opening
a new transport session, with one canonical fact, manifest and raw mapping.
Preexisting unrelated pending WAL and original capture/frozen bindings remain.
This tests the runtime finalizer boundary; Docker SIGTERM, supervisor lifecycle,
all production stream projections, complete publisher exclusion and host
stop/delta/COMMIT/recovery remain separate qualification requirements. A completed
runtime or a momentarily empty spool does not authorize a storage switch.

The supervisor now preserves finalizer exceptions/cancellation, timed-out drain,
and unresolved task failure during restart backoff; stop() raises after a failed
thread or drain instead of reporting clean shutdown. A successful replacement
collector that drains clears its earlier failure. Actual Linux SIGTERM tests
exercise the worker's existing exit0/exit5 paths with isolated infrastructure.
The SSD/HDD fixture additionally runs the real supervisor around the real QT
collector/repository, with controlled discovery/safety metadata and scripted
transport. It proves drain error propagation and retained-WAL recovery, not
production safety registration, Docker stop or a complete held-host switch.
The legacy source image predates this fix: its exit0 must not be treated as
proof of publisher drain. Spool preservation and exact source admission remain
required, and the source image is not changed by these tests.

The optional prepared-host rehearsal --worker-shutdown path replaces the synthetic
collector shell with the actual worker and supervisor process. Docker delivers
SIGTERM through the bound final-stop helper while the same migration worker stays
alive. Discovery, adapter, lifecycle and heartbeat effects are controlled fixture
inputs; separate database tests qualify real publication and WAL recovery. The
failure case retains an owned pending segment and reports exit5. The existing
strict inventory guard refuses paused admission and retains stopping intent; the
source is not restarted. Even clean paused state supplies no publisher-drain,
switch or source-resumption authority. Initial
preparation uses a clean stop and resumes the same source process container before
the final failure is injected. No production runtime or migration algorithm changes.

Internal final_delta rounds admit completed SQL and archive baselines plus the
same controller's background file reproof before any tail copying. Each round
copies at most two alternating SQL tail pages and one page per fixed archive
family. Per-page tail-only checks refuse bulk fallback. All work shares an
absolute caller deadline capped by the unchanged short command allowance,
original admitted resources and original capture lifetime. SQL and archive
transactions only shorten existing timeout/watch ceilings. A process binds its
first final deadline; later rounds must retain it and commit/rollback cannot
widen it. Background copy/preparation requests are refused after entry.

A failed round closes ordinary work. Already committed pages remain durable;
an interrupted archive transaction retains its queue. Replacement controllers
start with zero file proof and must reprove while source admission permits it.
The fixed final_delta pipe command accepts only its caller's absolute deadline.
The internal host helper admits the exact paused source and same live worker
before and after each bounded round under the persisted original wall/boot
window. It validates the controller and all three archive-family replies; it
never modifies the final receipt or grants a new deadline on retry. Same-process
sequence replay retains the existing framing rules.

This tail-copy path provides no publisher-drain, host stop/restart,
switch-entered receipt or runtime authority. Pending spool files remain intact.
Late publisher commits can be copied, but tail emptiness is only an observation;
publisher exclusion and the complete exact verifier remain required at COMMIT.
Database commit is not exposed on the pipe. Full outer-controller loss and safe
source resumption still require separate host reconciliation.


### Durable entry before possible database switching

The internal record_switch_entry_locked checkpoint validates the exact paused
source and same live worker, including the controller's already bound final
deadline and fresh ordered status. It durably changes the existing final receipt
to switch_entered before any future dispatch. The initial preparation, capture
and final wall/boot clocks remain unchanged. An acknowledgement lost after save
leaves that phase intact; ordinary stop, spool and tail-copy reentry refuse.

This checkpoint records uncertainty conservatively. It dispatches no database
command and returns database_switch_authorized=False. It does not establish
publisher exclusion or turn saved worker status into proof. Reentry cannot replay
or remove it, even when no COMMIT was actually sent. Qualified outcome inspection,
supervised source resumption, worker reaping and recovery/runtime activation
remain required. The internal one-shot held switch is described above; no
complete production operator is enabled.


### Fresh outcome inspection through the held worker

The inspect_outcome command reads the existing authoritative SQL handoff result
under a read-only transaction and migration ownership. A busy migration returns
pending explicitly, never an absent-certificate rollback conclusion. The bounded
response distinguishes committed, uncommitted and pending; it exposes neither
the full database receipt nor restart/runtime authority. Every observation needs
a new sequence; cached replies cannot stand in for a fresh inspection. The pipe
can remain open for inspection/close in an uncertain or terminal database state,
retaining the same live file proof until explicit close or channel failure.

The host admits the existing switch_entered receipt, exact paused source and
same worker before and after inspection. Its at-most-five-second call only
shortens the original final deadline and page ceiling. Every outcome leaves the
host receipt unchanged and source held. An uncommitted response is not the live
rollback fence; a committed response does not permit recovery mounts while the
read-capability worker survives. COMMIT dispatch, publisher admission, supervised
source resumption and terminal host reconciliation remain separate requirements.


### Real publication during Docker shutdown rehearsal

The explicit `--real-worker-publication` host rehearsal combines Docker SIGTERM
to the actual worker/supervisor with ContinuousStreamRuntime, the Coinbase parser,
trade projection, PostgreSQL archive publication and canonical acknowledgement.
Transport is scripted. The owned v1 fixture binds the frozen v1 ingestion and
partition/read clauses; it does not run the candidate v2 bootstrap against v1.
The same admitted migration worker and file proof remain alive during shutdown.

A diagnostic acknowledgement delay checks the intermediate state: a durable raw
manifest/mapping and sealed WAL exist before the canonical fact commits. Normal
completion must produce exactly one fact/manifest/mapping, retire only that
segment and preserve unrelated fixture WAL. The held tail-copy path then catches
publication that completed during stopping. Discovery, lifecycle and heartbeat
remain controlled, as do UUID metadata and UID70 fixture directories. This is not
full deployed-image readiness, all-projector drain, real collector performance,
production downtime, COMMIT dispatch or source resumption authority.


### Retained rollback fence on the private worker channel

The fixed rollback_fence_begin/check/end commands retain the existing read-only
SQL rollback fence across ordered requests to the same live controller. They
require its already bound absolute final deadline without shortening/replacing
that binding or renewing the original capture. Begin performs authoritative
negative inspection and retains migration ownership plus original relation locks;
check verifies the same connection. Source publication remains possible while a
competing migration or destructive DDL is refused. End releases the fence and
terminalizes the controller; it does not enable copying or switching again.

Each observation requires a fresh sequence. A cached reply cannot represent
current ownership, including a retry of begin. While fenced, ordinary work and
outcome inspection are refused; only fresh fence checks, end or close are
accepted. The idle pipe loop checks the fence at most every 100ms between bounded
operations. EOF, malformed framing, lost connection and context exit unwind the
live transaction. No serialized field can re-create it, and all replies continue
to deny collection resumption and runtime activation.

This supplies the live database half of a future supervised host abort. It starts
no Docker client, edits no final receipt and supplies no host restart authority.
The host still must bind original source images/configuration/cluster/roots,
persist its intent, supervise in-flight starts when ownership or time is lost,
and reconcile its terminal state. A lost fence cannot itself stop a Docker action.
Full host/worker death, recovery mounts and matching runtime remain separate
requirements; the production final command is still unavailable.


### Supervised original-source abort resumption

The internal resume_online_source_locked helper accepts only existing
switch_entered intent under the same launcher lock, live worker and private
pipe. It obtains the retained authoritative rollback fence, re-admits exact
original clients, images/configuration, cluster and roots, and persists
source_resuming before starting any client. Every exact Docker start has an
in-flight journal entry saved before dispatch. Only originally running clients
are started; a completed initializer remains stopped.

The host checks the live fence and source binding while each Docker CLI is in
flight, sharing the original final wall/boot and already bound monotonic
deadlines. Loss, expiry, drift or a failed reply stops further dispatch. Killing
and reaping the local CLI does not cancel the daemon request: a client may still
start later. The retained journal therefore records unresolved state rather than
claiming all clients remain stopped. Neither saved negative observations nor
re-entry can replay starts. The durable final marker continues to block managed
switching, ordinary launch, deployment and recovery.

After all exact original clients are healthy, the helper ends the live fence,
terminalizes that controller and records source_resumed. Lost end/save replies
retain uncertainty. Even successful resumption retains the final marker; it
cannot authorize marker removal, a replacement controller, candidate runtime or
recovery mount activation. Terminal reconciliation, full outer-process loss and
production readiness remain separate qualification requirements. The optional
owned --abort-resume rehearsal exercises this internal host path; it is not a
production entrypoint or a production outage measurement.


The optional --abort-resume-fence-loss disposable rehearsal uses a private Unix
HTTP proxy limited to ping and one exact owned container start. The real Docker
engine completes that start while its reply to the actual CLI is withheld. The
fixture then terminates only the owned database migration-fence backend. It
checks that host supervision refuses further starts, reaps the local CLI and
preserves source_resuming with the exact in-flight action and original clocks.
One source client is already running: the result is explicitly unresolved, not
an assertion that the source remains stopped. This controlled reply fault does
not qualify a daemon start completing after ownership loss or full host death.
The proxy is confined to the one child CLI environment and removed at fixture
exit; production Docker configuration and migration behavior are unchanged.


The optional --abort-resume-late-start mode accepts that one exact start at the
private proxy but does not forward it yet. With all source clients stopped, it
terminates the owned SQL fence and waits for the real local Docker CLI to be
reaped. Only then does the proxy forward the already accepted request to Docker.
The actual engine starts the client and returns 204. Monotonic observations
record this ordering; no deadline is renewed and no extra start is permitted.

The retained source_resuming journal still has the exact in-flight container and
an empty completed list. Replay and ordinary relaunch refuse, while one original
client is running. Normal launcher cleanup retires the read-capability worker.
This demonstrates a queued intermediary request surviving local cancellation.
It does not prove Docker-internal queue behavior, full outer-host death, or a
safe terminal reconciliation of incomplete starts. No production transition or
additional resumption authority is introduced by this fixture.


### Reconcile a completed resumption on the same live worker

After rollback fence end, the aborted controller now remains available only for
fresh read-only outcome inspection and close, until the original final deadline.
It cannot prepare, copy, switch, cancel or begin another rollback fence. Explicit
close, EOF, proof/ownership loss and expiry still release the read capability.
The host must reap it before any recovery secret mounts become visible.

The internal reconcile_source_resumed_locked helper can finish the existing
source_resuming receipt after a lost terminal acknowledgement. It requires every
originally running client to have a durable completed start entry, no in-flight
start, the same live worker/controller and exact healthy original source before
and after fresh authoritative inspection. Only an aborted controller reporting
an uncommitted database outcome is accepted. The original wall/boot/final deadline
and a short inspection ceiling bound all work; no start is issued or replayed.
Success records source_resumed and retains the final marker and all exclusions.

This covers a fully received end reply discarded by the host or an unrecorded
completion while that exact channel remains usable. Unread/partial framing,
worker/outer-host death, partial starts, delayed daemon completion, reboot and
expiry remain unresolved; there is no fence recreation or saved-negative restart
shortcut. The optional owned --abort-resume-lost-end rehearsal qualifies this
limited terminal path. It is not COMMIT dispatch, recovery activation, ordinary
deployment authorization or a production downtime measurement.


### Verify worker retirement independently of its attach CLI

Launcher teardown now inspects the exact admitted container after reaping the
local Docker attachment. CLI exit alone cannot prove that the worker holding
DAC_READ_SEARCH has exited. If that same admitted worker is still running,
teardown stops only it, then requires a non-running, non-paused, non-restarting,
non-dead container with PID zero and exited/created status. Contract drift,
lost stop replies, remaining processes or expiry fail retirement explicitly.
The launcher lock remains held throughout and source clients are not changed.

The existing 10+15+10 second cleanup allowances now share one absolute35-second
ceiling; final local CLI reaping has its own bounded one-second cleanup wait.
These are worker cleanup bounds, not a renewed copy, source stop, COMMIT, runtime
or recovery activation allowance. A cleanup failure remains unresolved and
must prevent any later recovery mount transition. No receipt/marker is removed.

The optional --worker-attach-loss owned rehearsal SIGSTOPs only the admitted
migration Python process, then kills/reaps its actual Docker attach CLI. It
checks that Docker still reports the worker running before normal launcher
cleanup retires it. It then exercises a fresh controller with zero proof under
the original capture deadline while source clients keep serving. This proves
cleanup of a live outer launcher; full outer-process death and recovery-mount
activation remain separate requirements.


### Retained collector WAL across the database switch

The disposable collector-drain fixture now also preserves a sealed trade segment
across the actual guarded v1-to-v2 database and archive-catalog switch. It covers
failure before canonical publication and failure after that publication commits
but before acknowledgement. The source supervisor joins before the internal
switch; candidate recovery uses the original SSD working/spool root and the HDD
archive root, with the candidate v2 ingestion and range reader restored.

Recovery must produce exactly one fact, manifest and raw mapping before another
frame is accepted. Fresh post-switch intake must publish a second record into
the HDD archive root. Original source archive bytes and the unrelated spool
files, owners and modes stay unchanged; only normal runtime acknowledgement
retires the recovered segment. Frozen records and the original capture survive.
No migration helper deletes WAL or changes its ownership to obtain readiness.

This small scripted Coinbase trade fixture does not establish host publisher
exclusion, source-image admission, all-projector recovery, legacy UID1000 spool
permissions, encrypted pairing, production performance or COMMIT dispatch
permission. Pending WAL remains a recovery obligation; its presence cannot be
ignored on the strength of this fixture alone.


### Refuse unadmitted Docker mount writers during final source admission

The internal final host observation now supplements the exact project/network
inventory with two bounded Docker-wide mount/state snapshots under the existing
absolute deadline. A live, paused, restarting or nonzero-PID peer outside the
admitted source/worker set refuses admission when a writable reported source
mount equals, contains or is contained by a database/collector source mount.
Read-only and fully stopped peers are observed without mutation. Changed peer
inventory/start identity/mounts, malformed paths, missing admitted identities or
bounds exceeded fail closed. Docker mount ordering is normalized. Exact source
client lifecycle changes during journaled stops/starts remain governed by existing
source admission; their mount descriptors still must remain unchanged. Worker and
unadmitted peer runtime changes refuse. The fixed observation caps are 256 containers and
64 mounts each, within the existing Docker output and caller time limits.

This check neither stops nor alters an unexpected container. It reads no mount
contents or container environment, persists no new receipt, and does not log
source paths. Existing final intent/deadline and source identities remain the
only host transition bindings. The check runs on final stop, held tail/outcome
observation and supervised resumption through their shared source admission.

The optional host rehearsal previously left its independent diagnostic publisher
alive with writable source mounts outside the project's admitted service set.
The new check refuses that configuration. The fixture now publishes its late
tail while source peers serve, completes its assertions and exits before final
entry; there is no diagnostic allowlist exception. The same migration worker
then catches that residual tail. Post-transition SQL observation still compares
frozen datasets and the original capture. Earlier late-publication-after-stop
proof remains historical evidence, not publisher-exclusion authority.

Docker mount/state snapshots do not exclude future daemon starts, host processes,
SQL-only publishers or filesystem aliases outside the reported path hierarchy.
They are an additional refusal boundary, not complete publisher exclusion or
COMMIT dispatch permission. Stopped peers remain preserved, including retained
backup/restore objects; this check grants no permission to restart them.


The added admission work exceeded the older rehearsal's extra 25-second
subwindow during terminal reconciliation; that attempt remains failed and is not
resumed. A new fixture binds its final monotonic deadline once from the already
persisted original 60-second wall/boot window, leaving one second of margin.
Neither the total fixture ceiling nor any production allowance increases. This
fixture budget change is explicit and cannot be interpreted as production
pause admission or renewal of an expired final attempt.


Final source admission also resolves Docker network/PID namespace aliases within
those same bounded global snapshots. A live, paused, restarting or nonzero-PID
unadmitted peer sharing either namespace with the database or collector refuses,
even when it has no writable mounts and is absent from the project's normal
network inventory. IDs, unique ID prefixes and container names resolve against
the observed inventory; missing, ambiguous or cyclic aliases fail closed.
Source namespace/name changes between observations also refuse. Fully stopped
peers remain preserved. No unexpected peer is stopped or changed.

This is an additional refusal check, not continuous exclusion: future Docker
starts, host processes, remote SQL clients and filesystem aliases still require
separate admission. It grants no COMMIT, source-resumption or recovery authority.
The existing caller deadline and inventory/output bounds are unchanged. The
read-capability worker's launch confinement remains a separate boundary.


The internal online-controller database handoff requires a fresh SQL-session
refusal check before verification and again on the same switching transaction
immediately before COMMIT. Only the actual switching connection and the separately
checked live controller ownership connection are excluded. Other database client sessions,
including idle sessions, and prepared transactions refuse; application names or
addresses cannot establish migration ownership. The check uses the handoff's
already shortened absolute deadline, and the existing transaction/resource fences
remain through commit. A late refusal rolls back the real rename and preserves
source/capture/frozen data. No client is terminated or database login setting changed.

These are necessary refusal conditions, not continuous SQL or host publisher
exclusion. A client can arrive after an observation. The host still must establish
exact publisher/source-image/spool admission and retain it through handoff. No
COMMIT pipe command or production final-pause allowance is added by these checks.

PostgreSQL's server-owned `backend_type` identifies client sessions; a supplied
application name is never used for exclusion. Internal PostgreSQL/TimescaleDB
workers are outside this client check and still require separate admission.
An idle TimescaleDB scheduler is not proof that its jobs cannot run. This check
must not be presented as admission of extension jobs or all SQL publishers.

### Retain the final SQL session across a separately owned login gate

The controller may bind one internal final SQL session before a host-managed
login gate. The handoff accepts this caller-owned connection without closing it;
the same session supplies commit and fresh uncertain-outcome inspection. Closing
or invalidating it never falls back to another connection. Entry pins the
original final deadline before admission SQL; capture-clock shrink can refuse
entry. Background work and reentry are forbidden after this boundary; confirmed job
retirement admits only residual tails on the retained session.
Context exit discards the session. This adds no public command or gate authority.

A disposable fixture uses its existing cluster's maintenance database to close
new logins to only the owned test database. PostgreSQL refuses closing logins
from inside that target database. While closed, the real switch commits, a
received commit result is deliberately lost, and the retained session observes
committed state without granting runtime or source-resumption permission. A
separate lost-session case refuses reconnection. Fixture cleanup restores the
original open-login setting; this is not production gate reconciliation.

This resolves connection continuity, not continuous exclusion of all publishers.
Existing sessions, extension/replication jobs, archive writers, durable host
intent, gate restoration after outer loss and the recovery transition remain
separate integration requirements. No production login configuration changes.

### Record the host login-gate mutation in the existing final receipt

The final host boundary now records `login_closing` before asking PostgreSQL to
close new target-database logins, and `login_closed` after fresh same-worker and
source admission. Both phases retain original intent/clocks and reject retry or
source resumption. The host owns the setting mutation; the worker owns its live
retained SQL session and capture/file proof. Session begin/check pipe operations
add no COMMIT or host-start authority. No independent gate CLI or second receipt
is introduced.

The fixed maintenance query uses the database container's existing credentials
and target name, with the caller's bounded Docker deadline. Exact cluster and
database OID/name must agree with the retained worker before mutation. Catalog
quoting selects only that target; drift or uncertain replies retain the hold.
Source admission can inspect cluster identity through maintenance while the
worker freshly inspects the target capture through its retained session.

Closing new logins does not drain existing sessions or prove background/archive
publisher exclusion. Gate restoration after interruption and complete switch /
recovery integration remain required. There is deliberately no automatic reopen
or source restart after a negative observation or failed gate acknowledgement.

### Stop scheduled database publishers under the existing gate intent

Use the already retained target session and host `login_closing` intent to stop
pinned Timescale 2.14.2 background workers. Do not infer authority from a mutable
scheduler/job name or introduce a second operator. The same original deadline
bounds the nontransactional stop and fresh all-target-backend drain; uncertain
replies cannot be replayed. The completed receipt explicitly records job stop.
The original job definitions remain unchanged and fingerprinted with fixed
metadata bounds. Unknown extension/preload or replication environments refuse.

Once this stop has been requested, internal COMMIT requires confirmed stop, the
closed gate, the same admitted extension/replication environment, unchanged job
definitions, no other target backends and no prepared transactions. Existing file
proof, source/capture clocks, SQL locks and outcome reconciliation remain owners
of their existing boundaries. Host/archive/spool exclusion remains separate.

Stopping a running job can interrupt its current transaction while preserving
work it committed earlier. Timescale's normal crash retry can delay the next
execution; this is an operational consequence to measure, not a reason to alter
job schedules or extend the final switch budget. Disposable scheduler restoration
is separate from production gate/job recovery authority, which remains unfinished.

The supported operator admits only the two built-in jobs observed on the serving
cluster: Timescale 2.14.2 telemetry (job 1) and job-error retention (job 2).
Before serving commands and again before entering the retained final session,
the existing controller checks their exact enabled schedules, retry limits,
configuration, check function and lack of hypertable association. It also pins
the retention/check SQL bodies and execution attributes to the qualified public
installation SQL; names or extension version alone do not establish identity.
Unknown/custom jobs, NULL retention configuration and modified functions refuse
before host login closure. Configuration remains inside PostgreSQL.

The same worker retains the complete job-definition fingerprint and rechecks it
before stopping jobs, on fresh final-session observations and before internal
COMMIT. The host requires the live admission result before changing access;
a saved result grants no replay or switch authority. This deliberately avoids
arbitrary custom-job interruption/replay support. The built-ins perform telemetry
and job-error cleanup rather than QT fact publication, but their normal retry
behavior still does not promise exactly-once execution. The previous generic
scheduler fixture remains lower-level lifecycle evidence only. Source-image,
archive/spool publisher exclusion and the complete host switch remain separate
unfinished release requirements.


### Residual catch-up after job retirement

The existing final-delta operation also runs on the retained connection after
confirmed job retirement behind the closed login gate. It reuses the existing
header/raw/archive page machinery, separate page commits and live resource/file
checks. It cannot start a baseline, relocate data, open another connection or
extend the final/capture window. The host accepts this route only from its
existing `login_closed` intent with fresh same-worker and source admission.
No new receipt, command surface or state owner is introduced.

A disposable late QT publication proof covers nonempty residual queues, the
same-backend internal switch and preserved frozen/capture state. Interrupted
archive transactions preserve earlier SQL pages and leave archive progress
pending; a reopened gate refuses catch-up. The host rehearsal covers the same
route with an already-converged tail. These combined component results are not
a production outage measurement, scheduled QT job semantics or complete host
publisher exclusion. Exact verification, host COMMIT and preserving recovery
remain release requirements.


### Preserve a known original source when abandoning confirmed gating

Reuse the existing host resumption owner and live negative-outcome fence for a
confirmed `login_closed` abort. Retain gate evidence in the final receipt and
journal access restoration, job restart request and each exact client start
before dispatch. The same retained SQL connection protects original relation
identity while a bounded host maintenance action reopens the original database;
a separate target transaction requests the existing Timescale scheduler restart.
No job definition or schedule is changed. A restart request is not proof of job
completion or generic exactly-once execution.

Shared bounded Docker supervision retains fresh fence/source checks while each
local CLI runs. Loss leaves an unresolved action that may already have completed;
it cannot authorize a retry, more starts, marker removal or candidate activation.
Confirmed restoration followed by exact healthy source admission terminalizes
the controller under its original deadline. Uncertain gate closure, committed
outcomes and dead/lost owners refuse this route. Full outer-loss reconciliation
and successful migration recovery remain unfinished release obligations.

## Preserve application ownership across the online transition

Disposable use of the exact legacy collector exposed incompatible private file
owners: the disposable legacy producer ran as UID1000, while the online reader
creates HDD objects as UID70. This was not an attestation of production child
ownership. A later production observation found root-running collectors and
root-owned0600 pending WAL beneath a UID1000 working root. The candidate UID1000
runtime therefore additionally needs a bounded preserving recovery copy; original
source permissions and bytes must remain intact until normal qualified recovery.
The internal spool boundary now supports a bounded preserving copy into a new
candidate-owned SSD root, reusing its guarded traversal and normal QT replay.
Failed copies remain unactivated; the final operator still must bind this step
into its original deadline and source-exclusion interval. The group-publication
contract alone does not implement that ownership transition. Running every service as70 breaks retained spool
recovery; switching every service to1000 breaks copied-object access and the
PostgreSQL physical-maintenance boundary.

Preserve original source ownership and give candidate application/database maintenance an
explicit shared group for immutable HDD objects. Keep PostgreSQL files, recovery
keys and repositories private to their existing owners. Do not recursively
change source permissions or give ordinary runtimes filesystem-bypass capability.
The object store implements opt-in group-readable publication before its existing
atomic link; existing private paths refuse rather than being repaired. Private
publication remains the default. The exact directory/object contract is owned by
[Storage Management](../persistence/STORAGE_MANAGEMENT.md#application-and-database-file-ownership).

The opt-in internal maintenance process now hosts the existing supervisor at the
database-owned process boundary, retaining one lifecycle scheduler and its
status/recovery guarantees. No separate policy or generic workflow framework is
introduced. This ownership seam is tested, but deployment wiring, actual
retained-WAL database recovery, complete encrypted pairing and the final operator
remain unqualified. Existing server/runtime guards and production stay unchanged.


The explicit owner setting omits the collector's supervisor in dedicated mode;
its compatibility default remains unchanged. Storage status consumes the dedicated
worker's existing heartbeat contract, while collector health excludes that worker.
This preserves one maintenance schedule and makes a missing or competing owner
visible. Deployment recipes, mounts and all-publisher group membership still need
preserving integration; the process seam alone does not complete the ownership
transition or authorize a production switch.


The fixed overlay now expresses the split accounts and explicit group membership,
including bot readers. Maintenance alone receives the PostgreSQL process namespace
and recovery-key mounts. Backend retains the registered SSD mount for capacity
observation but has no permission to read private PostgreSQL files. The existing
legacy runtime validator still rejects this new service topology. Qualifying and
binding the new composition belongs to the unfinished online final operator;
changing the declarative recipe grants no activation or recovery authority.


The confined online worker's existing private request may explicitly bind
`archive_shared_group_id=70`. The launcher pins that setting in its existing
request/environment hashes; omission explicitly selects private publication.
The worker admits only its unchanged primary GID70, with no supplemental group,
new capability or identity transition. It checks central settings and the already
prepared group2770 archive root before opening SQL. Existing capture/root identity,
live file proof and object-store checks continue to protect copy/reuse. Incompatible
private paths refuse without repair. Other shared groups are unsupported by this
fixed worker. This connects the migration publisher to the declared runtime
archive contract; it grants no final switch or runtime activation authority.

### Hold archive namespace ownership through the final transaction

Content leases do not prevent another process from unlinking a verified pathname.
Use the existing object-store mutation boundary to take a shared Linux directory
flock for publication/deletion, with an exclusive lock retained by the online
controller through internal COMMIT and outcome reconciliation. Contention refuses
immediately, so it cannot renew a pause budget. Ordinary publishers remain
concurrent. No lock file, receipt, command surface or permissions repair is added.

This supplements content proof and SQL fences. It only excludes cooperating store
implementations; exact legacy/source/host admission remains necessary. A dead
worker releasing its lock cannot authorize source restart or recovery activation.


### Add recovery mounts only after the read-capability worker is retired

Keep the existing final-state owner and continuous deployment/source holds across
the committed switch and reader retirement. A separate fixed recreation phase may
then add the already prepared key directory and independent PostgreSQL socket to
the already HDD-mounted database. It preserves the initial recipe, PGDATA, HDD,
image and settings; it does not reinterpret initial preparation or renew its clock.
The phase has its own shorter bound inside the original final/capture window.
Journal each daemon action before dispatch in the existing final receipt, refuse
uncertain replay, and verify the original cluster with target logins still closed.
This is a real privilege transition, not a generic workflow abstraction or another
operator. Mount readiness is distinct from repositories, WAL delivery, application
recovery and complete encrypted-pair publication; those must still be composed and
qualified before release.


### Repository preparation and native WAL continuation

After `recovery_database_ready`, the existing final-state owner can continue under
that same live source/deployment hold through `storage_online_repositories`.
It independently rechecks migration-reader retirement, the committed binding,
original cluster, private recipe, filesystem identity and stopped original clients.
The original final/capture deadline bounds the separately limited phase; helper
startup cannot renew it. Docker mount-list ordering carries no identity meaning;
all complete mount records are compared after sorting by destination.

The same final receipt records ordered intent before restoring database logins,
creating/running one fixed UID70 repository preparer, changing native archive
settings, stopping/restarting that same database container and switching WAL.
The preparer reuses `storage_recovery_prepare`, existing independent keys and
repository code. Its fixed mounts include PostgreSQL data, HDD, recovery keys,
socket and inventory, but never the private application spool. Only this preparer
writes the existing archiver configuration; ordinary applications receive no keys
or physical database access. The exact stopped preparer and configuration digest
remain bound after it exits. No key generation, backup policy or recurring full
backup is introduced.

`recovery_wal_ready` requires the original cluster, exact native archive settings
without a pending restart, successful repository preparation and observed native
WAL delivery. A lost action/reply leaves its intent unresolved and permits no
replay or automatic reversal. This state grants no application-start authority.
Private spool recovery, matching runtime activation, fresh collection and complete
encrypted-pair publication still require connected qualification. Production
pause and capacity admission remain unfinished. The runtime package explicitly
includes the existing online worker/controller dependency closure, and those files
participate in source attestation.


### Connect private pending-WAL preparation after native WAL readiness

The final-state owner may enter `storage_online_runtime.prepare_spool` only from
`recovery_wal_ready`, inside the same live source/deployment hold and original
final/capture window. This internal application-handover boundary begins by
preparing pending WAL; it does not yet start the application or certify recovery.
The original source root remains read-only. An already prepared empty private
UID1000 destination must be on the same SSD, separate from the source hierarchy.

One fixed, isolated filesystem helper reuses `prepare_recovery_spool`. It has only
source/read-only and destination/read-write mounts, no network, no shared peer PID
namespace, no database or keys, and only CHOWN/DAC_OVERRIDE/FOWNER capabilities
needed for the existing private ownership transition. Ordinary runtime privileges
are unchanged. Host checks retain original source identities, database bindings,
repository-helper retirement and the source hold; the child independently enforces
the original deadline and SSD reserve before bounded one-MiB writes. The helper
must actually exit cleanly before its copy result is accepted. Local CLI loss never
means its daemon work was canceled.

The existing final receipt journals create/copy intent before dispatch. A private,
bounded `.qt-recovery-copy.json` in the new working root contains copied-file hashes;
the final receipt stores its digest and compact counts, avoiding an unbounded
control receipt. Original files, acknowledgements and permissions stay untouched.
Any failed or uncertain copy retains the unactivated destination and unresolved
intent, with no reuse or replay. `recovery_spool_ready` certifies only preserving
copy preparation. The matching application must still recover and acknowledge WAL
through normal QT database/archive processing before collection and encrypted-pair
outcomes can complete the supported operator.


### Matching application startup after spool preparation

The same final owner has an internal `activate_online_runtime_locked` continuation
from `recovery_spool_ready`. Its private rendered recipe contains the unchanged
prepared database and the existing initializer, backend, collector and dedicated
maintenance entrypoints. It binds the candidate image, shared archive group,
private new SSD working root, inventory and maintenance limits. Applications keep
UID1000; only maintenance uses UID70, the database PID namespace and recovery keys.
The private recipe resolves maintenance's PostgreSQL PID namespace to the exact
confirmed replacement container before computing its Compose hash; this avoids
Compose changing the service hash during service-name resolution. The public
overlay retains `service:tsdb`. The candidate environment must remove the old
source-fence input, including any image default. No source file ownership is changed.

Each original application removal, candidate creation and start has durable intent
in the existing final receipt before dispatch. A failed or uncertain action is not
replayed. Created candidates are checked against the admitted recipe before they
start; earlier starts must remain healthy while later actions proceed. The original
source/deployment hold, final clocks, retired migration reader, retired preparation
helpers and private copy manifest remain checked throughout. The collector starts
before maintenance; startup does not wait for a long physical baseline. Infrastructure
and UI services are not replaced by this application-recovery step.

`recovery_runtime_ready` records application health only. Normal pending-WAL recovery,
fresh collection, current/frozen reads, complete paired recovery and the final
operator/release outcome still require connected evidence. The final marker remains,
and ordinary launch/deploy has no new bypass. This continuation is implemented;
its native integration qualification is in progress, not a production approval.
