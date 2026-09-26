---
component: storage-management
subsystem: persistence
layer: service
doc_type: architecture
status: active
tags:
  - storage
  - postgres
  - recovery
code_paths:
  - scripts/db/fact_header_v2_cancel.py
  - scripts/db/fact_header_v2_online.py
  - scripts/db/fact_header_v2_online_proof.py
  - scripts/db/archive_root_v2_online.py
  - scripts/db/archive_file_v2_proof.py
  - cli/main.py
  - src/core/storage_targets.py
  - src/core/storage_header_placement.py
  - src/core/storage_move_budget.py
  - portal/backend/service/storage/header_catalog.py
  - portal/backend/service/storage/header_filesystem.py
  - portal/backend/service/storage/header_journal.py
  - portal/backend/service/storage/header_inspection.py
  - portal/backend/service/storage/header_movement.py
  - portal/backend/service/storage/header_resources.py
  - portal/backend/service/storage/header_resource_claims.py
  - portal/backend/service/storage/recovery_copies.py
  - portal/backend/service/storage/recovery_maintenance.py
  - portal/backend/service/storage/history_maintenance.py
  - portal/backend/service/storage/maintenance_status.py
  - portal/backend/service/storage/maintenance_runtime.py
  - portal/backend/service/storage/history_policy.py
  - portal/backend/Dockerfile
  - scripts/provenance/source_tree_hash.py
  - docker/test/storage-demo.compose.yml
  - portal/backend/service/storage/header_admission.py
  - portal/backend/service/storage/header_destinations.py
  - src/core/storage_inventory.py
  - portal/backend/db/storage_target_models.py
  - portal/backend/controller/storage_management.py
  - portal/backend/service/storage_management.py
  - portal/frontend/src/adapters/storage.adapter.js
  - portal/frontend/src/v2/rooms/StorageRoom.jsx
  - portal/frontend/src/v2/rooms/storage.css
  - scripts/automation/storage_device_audit.py
  - scripts/automation/storage_host_prepare.py
  - scripts/ci/test_storage_runtime_directories.py
---
# Storage Management

The Storage settings page enrolls host-prepared drives and reviews configuration.
After an explicit operator cutover has installed an applied policy, Apply can
save settings for that same layout. Initial setup and role changes remain
blocked: a UI confirmation cannot prepare a database or prove a migration.
The existing collector loop performs history movement and local recovery copies
under its deployment gates, saved policy and explicit operating limits. The
settings page reports observed outcomes separately from saving configuration.

## From a prepared drive to a reviewed change

The read-only host inventory at QT_STORAGE_INVENTORY_PATH lists at most 32
targets. Each has target_id, label, filesystem_uuid, root, medium (ssd or hdd),
and eligible roles (recent, history, archives, backups). Its schema_version is
qt.storage_inventory.v1. Missing inventory means no prepared drives; malformed
inventory is an error. The deployment must bind this file read-only and expose
the target roots and host udev identity metadata at stable container paths.

POST /api/storage/targets accepts only target_id. The service verifies the
filesystem UUID and writability, then records the identity under the shared
storage advisory lock. Repeated identical enrollment is harmless. Repointing a
target ID or registering the same filesystem twice is rejected.

GET /api/storage reports enrollment, observed filesystem capacity, policy
revision, recent plans and prepared candidates. Filesystem availability alone
does not prove that movement or backups are working. The existing collector
worker heartbeat carries each configured maintenance phase's in-progress state
and latest outcome. Storage reads that persisted evidence; it neither runs
maintenance nor reads private files to manufacture success.

For an enabled phase, the status requires exactly one live configured worker.
Expired/stopped workers, missing configuration, ambiguous ownership, malformed
timestamps and an observation older than one maintenance interval plus its
heartbeat TTL cannot appear successful. A fresh process heartbeat does not
refresh an old maintenance result. Successful history and backup outcomes must
match both the saved policy revision and its content hash. A changed policy
therefore remains unconfirmed until a corresponding maintenance observation.
Backup due times are exposed separately from the last completed copy.

Blocked plans, stale/failed maintenance and degraded lifecycle reports make the
overall status needs_attention even when both filesystems are mounted. Running
work is reported separately from completion. Disabled policy phases remain
explicitly disabled. The compact page always shows the current backup state;
a last-copy timestamp cannot replace and hide a newer failure. This is observed
worker execution evidence, not a new scheduler, readiness certificate or
substitute for a restore test.

POST /api/storage/plans validates a complete policy against its base revision.
The durable request ID prevents retries from creating different plans.
Assignments and advanced settings appear in the impact preview. The browser
does not calculate readiness or silently activate a draft. A fresh review is
required after another policy revision. GET /api/storage/plans/{id} retrieves
the same server-owned plan.

POST /api/storage/plans/{id}/apply saves settings only when a non-null policy
with a positive revision already exists and all drive assignments remain
unchanged. Initial policy installation belongs to the explicit, verified operator
cutover; this API cannot perform it. Apply rechecks mount identity/writability,
review hash, policy revision and exclusive change ownership. Policy revision and
the completed settings plan commit atomically; a retry returns the same result.
Its completion means settings saved, never physical movement completed.

The current planner requires recent groups on SSD. Increasing recent_days could
include groups already moved to HDD, so this settings path rejects increases
until placement has been reviewed by an operator. Shortening the window, pausing
or resuming maintenance, and changing reserve/recovery settings do not relocate
data in the request. An outstanding move must finish or reconcile before settings
can be saved. The worker retains all existing per-operation resource, deployment,
revision and physical-admission checks. A saved policy is not proof of worker
readiness, migration qualification or a successful recovery copy.

The allocator subtracts the free-space reserve and in-flight reservations,
then chooses the eligible filesystem with the most headroom. Its caller must
serialize allocation and persist a reservation atomically. It never finds old
objects by applying today's allocation policy. StorageLocation resolves an
explicit target and safe relative key, checking mount identity and containment.

## Historical header placement preview

`core.storage_header_placement.plan_header_placement` is a pure planning
boundary for dated header partitions. It accepts a typed catalog snapshot and
observed filesystem capacity; it does not inspect disks, run SQL, save
reservations, or connect to the Storage API yet. The snapshot must come from a
catalog and filesystem adapters that prove parent attachment, daily bounds, complete
ordinary-index ownership, TOAST colocation, and physical relation-to-target
bindings. Caller-provided flags are not a substitute for that adapter.

Each proposed group contains its table and every ordinary index. Heap bytes
include TOAST and its internal indexes; ordinary indexes are counted
separately. PostgreSQL table tablespace changes do not move ordinary indexes,
so a future executor must move and verify the complete group transactionally.
The plan records OIDs, physical file identifiers, target UUIDs, sizes and a
deterministic evidence hash. Relation names are descriptive labels, not SQL.

The planner handles at most 4,096 daily groups and 32 registered targets;
a smaller caller budget is rejected before sorting or planning if exceeded.
Incomplete inventory, unverified source filesystems, or insufficient
destination capacity blocks the whole proposal. No partial move list or
additional reservation is returned on a blocked plan. Read-only evidence is
not admitted for this movement preview; this does not prohibit historical
reads from a read-only filesystem.

Days strictly before the database UTC day minus `recent_days` are historical.
Recent groups must already be on the selected recent target; moving active
groups requires a separate cutover. Historical groups keep an eligible existing
heap location if the rest of the group fits. Otherwise, allocation selects the
eligible target with most remaining headroom, with target ID breaking ties.
Adding an HDD therefore does not redistribute already valid history.

Copy reservations accumulate across the proposed batch, subtract existing
reservations and the policy reserve, and never credit space expected to be
freed by another move. They cover only measured files that change target.
The minimum reservation for an empty moving group is one byte; it is not a
filesystem-allocation or WAL estimate. WAL, temporary files, concurrent growth,
recovery copies and operational margin still require separate budgeting.

`planning_complete` means only that this limited preview has no blockers.
`execution_available` and `activation_ready` remain false, including when
the proposed policy enables movement. Global identity and raw-mapping growth,
payload/archive placement, the physical executor, recovery and performance
acceptance remain explicitly uncovered. Before any future execution, a worker
must recheck the catalog, policy revision, mount identities and capacity, then
reserve space under the shared lock. This preview cannot authorize a cutover
or establish a whole-system storage forecast.

### Catalog adapter

`portal.backend.service.storage.header_catalog.read_header_catalog` now
implements the read-only PostgreSQL half of the inventory boundary. It has
disposable-database coverage and is not called by the Storage API.
It accepts the existing PG_DSN-backed engine, opens a read-only repeatable-read
transaction, and applies a decreasing statement-time budget. Access-share
locks protect admitted tables against destructive rewrites while permitting
ordinary collection. Partition and index counts are bounded before accepting
a complete result. Concurrent file growth and index maintenance still require
fresh checks before execution.

The reader checks the registered daily partitions against their attached
relations, schema, persistence and exact daily bounds. It observes table,
ordinary-index and TOAST sizes/placement, rejects invalid ordinary indexes,
and reports whether TOAST and its internal indexes are colocated. PostgreSQL's
effective database-default tablespace is resolved when a relation stores
tablespace OID zero.

Its result contains an unbound `HeaderPlacementSnapshot` plus PostgreSQL
tablespace OIDs, locations and relative file paths. Every relation target ID
remains null and `filesystem_bindings_verified` remains false. Paths describe
the database server's namespace, not necessarily the API container's
filesystem. An empty built-in tablespace location does not mean HDD storage.
A host-side UUID/device verification and explicit path binding are still
required before these observations can produce a usable placement plan.
The reader requires access to the header catalog and PostgreSQL cluster
identity; it does not load application settings, create a second DSN, bootstrap
schema, or perform disk operations.

### Filesystem binding

`header_filesystem.verify_header_filesystem` connects the unbound catalog
snapshot to registered targets through read-only file probes. The future worker
must share the database server's PID namespace and database storage paths; running it
against similar-looking paths in the API container is insufficient. Its
absolute `pg_controldata` executable is trusted worker configuration, not a
portal setting. The executable runs with a small explicit environment and
without a shell or inherited database credentials.

The verifier compares the PostgreSQL 15 control-data cluster identifier with
the SQL inventory. It also checks the observed postmaster start time, PID file
directory and running process executable. It compares the device and inode of
`global/pg_control` in the worker's mounted data directory with that file
accessed through the postmaster's `/proc/PID/root`. The latter uses a
kernel filesystem lookup, not a userspace resolution of the proc root link.
This permits different worker/server binary locations without accepting a
different mounted copy of the database. The configured utility must still be
PostgreSQL 15. A backup copy's cluster identifier
alone cannot establish that it is active database storage. The catalog reader
now includes the server data directory and postmaster start time for this
purpose.

Each reported ordinary table/index file must match its database OID, physical
file identifier and expected PostgreSQL default/tablespace path. Tablespace
directory links are resolved and compared with the server's declared location.
Individual relation-file symlinks, missing files and paths outside a unique
registered root are refused. Target roots must pass UUID, device and writable
filesystem checks. TOAST placement remains the catalog reader's tablespace
colocation observation, not a separate file-by-file TOAST audit.

Process identity, mount identity and relation-file inode/device evidence are
checked again before returning bound target IDs and fresh capacity observations.
The function has a bounded inventory and elapsed-time checks between probes;
this is not a hard interrupt for a kernel filesystem call that hangs. The
future worker needs an outer operation timeout as well as retry/recovery
controls. None of these checks reserve capacity or prevent later file growth,
so a movement worker must obtain fresh evidence before executing.

Temporary-file tests cover default and linked tablespaces, stale identities,
wrong UUIDs/devices, malformed paths, missing files and mid-check file/mount
changes. The PostgreSQL utility is mocked in those tests. A real
PostgreSQL/worker-namespace fixture in
`tests/test_market_data/test_header_namespace_db.py` has passed locally and in CI.
It starts PostgreSQL 15 as a non-root OS user, with TCP disabled and a private
Unix socket. It probes real control data, process identity and relation files;
the udev UUID entry is synthetic. It exercises a table-only tablespace change,
the subsequent index move, transaction rollback after table-only and whole-group
movement, preserved row hashes, and rejection of copied
identity, stale start time and a wrong UUID. Cleanup is restricted to its
generated temporary cluster. This fixture does not qualify physical HDD
performance or a production migration. Runtime wiring and movement remain
pending; these helpers do not enable Apply.

## Database records and cutover work

portal_storage_targets, portal_storage_policy, portal_storage_plans, and
portal_storage_object_locations are clean-schema ORM models under the existing
Base and PG_DSN. The current bootstrap can create their missing clean tables.
A deployment cutover still needs explicit schema validation, grants, host
mounts, and a worker. These models do not alter market.fact_versions, its global
identity constraints, raw mappings, or existing archive manifest locations.

The next physical design must move historical headers, mappings and indexes as
well as payloads, bound SSD growth, preserve exact known-at and frozen-dataset
results, and survive interrupted movement. It must account for global identity
index costs on HDD rather than describe that cost as a small SSD directory.

[ADR 0069](../decisions/0069-bind-storage-objects-to-registered-targets.md) explains
why target identity is independent of placement policy. The
[implementation and benchmark runbook](../../engineering/storage-tiering-validation.md)
records the remaining proof and cutover gates.

## CLI

The same API is available through qt storage status, qt storage enroll TARGET,
qt storage review --policy-file FILE --base-revision N --request-id ID,
qt storage plan ID, and qt storage apply ID --policy-hash HASH. Apply preserves
the same settings-only and physical-cutover guards as the page. CLI success from review means a plan was saved,
not that data moved. The existing CLI audit records these requests.

The clean-schema dated-header foundation is now described in
[ADR 0070](../decisions/0070-separate-global-fact-identity-from-dated-headers.md).
It preserves global identity while allowing detail partitions to move later.
It does not enable Apply or prove physical tiering, and cannot yet be deployed
over an existing v1 layout.

## Durable header intent and reservations

The internal header journal saves one immutable batch per reviewed storage plan,
with at most 4,096 daily groups and 64 ordinary indexes per group. Each group
retains its original OIDs, file identifiers, source target IDs, destination
target UUID and copy-byte requirement. A batch binds these to the database
identity, policy revision, proposal hash and observation timestamps. Evidence
per group is limited to 64 KiB; the journal does not grow a per-file array in
the plan's UI progress JSON. These limits bound each batch, not lifetime
journal retention; terminal-ledger retention and its SSD budget must be included
in whole-system capacity accounting before activation.

Reservation requires an already queued/running plan with movement enabled,
the current policy revision, and filesystem-adapter observations no more than
60 seconds old according to database time. The repository recomputes the pure
proposal from registered targets and current aggregate reservations. Changed
capacity, placement evidence or reservations require a fresh review. It uses
the same storage advisory-lock key as policy management, refuses contention
immediately, and requires READ COMMITTED transactions. The database identity
must match the observed PostgreSQL cluster and database.

Saving the entire batch and adding its copy reservations happens in the
caller's transaction. Structured lifecycle logs identify these writes as staged;
a returned receipt or staged log does not claim that the caller committed. A partial unique index prevents two active intents from
owning the same database heap. An identical retry returns its durable receipt
without reserving again, even if the original observation has since expired.
That receipt is not fresh physical evidence and cannot authorize execution.
A changed review hash for an existing batch is rejected.

Cancellation releases capacity only when every group remains reserved and
unstarted. It is atomic, preserves reservations belonging to other work and
cannot reactivate a cancelled batch on retry. Running, blocked, completed,
mixed or incomplete groups require reconciliation; a client-side timeout must
never automatically release their space. There is no completion or running
transition exposed by this repository yet.

These are canonical clean-schema models, with no runtime backfill or live
migration. The journal remains internal and unconnected to the public queue
endpoint. Destination enrollment/worker wiring, locked pre-execution
checks, physical table/index DDL, crash reconciliation, execution logging and
full capacity coverage still belong to the future worker. No reservation
activates policy or moves bytes. Existing installations will require explicit,
reviewed schema preparation as part of the later operator cutover.


## Prepared PostgreSQL destination evidence

The catalog reader can observe at most 32 explicitly requested destination
tablespace OIDs in the same read-only inventory. It records the current name,
location and CREATE privilege, plus PostgreSQL's catalog version. Missing,
duplicate, malformed or global-tablespace requests are refused. This reads
already prepared tablespaces; it never creates directories or tablespaces.

The filesystem verifier optionally accepts a target-to-tablespace assignment.
Its requested OIDs must exactly match those catalog observations. It verifies
the existing PG15 catalog-version directory for a custom tablespace, or the
database directory under pg_default. An empty custom tablespace is admissible
before PostgreSQL has created a per-database subdirectory. No probe file or
database directory is created. The destination must belong to the exact
registered UUID/device/root, permit the history role, and have CREATE privilege.
The directory must belong to the postmaster's OS user, allow that owner full
access, prohibit group/other writes, and be on a writable mount in the server's
own filesystem view. Identity and permissions are rechecked before returning.

Sharing pg_control does not by itself prove that other mounted files are shared.
Source relations and destinations are now checked through file descriptors
opened from /proc/PID/root. Every later path component uses O_NOFOLLOW; an
absolute symlink cannot escape into the worker's root and falsely prove a match.
Custom tablespace links are read from both process views. Verification follows
the server's catalog path, not an unrelated path obtained by resolving a
worker-only alias. Device/inode equality
must hold for the actual source files and destination directories, and
server-side mount flags are read from the destination descriptor. This requires
Linux O_PATH support, visibility of the postmaster's PID namespace, and suitable
permissions; a permission or namespace mismatch is a refusal.

Returned destination evidence includes database identity, the observed configured target root, target UUID/device,
tablespace OID/name/location, worker and server directory paths, the verified
directory inode and catalog version.
It remains an observation until the internal registration and reservation
boundaries below accept it. The future worker must repeat the checks while
holding execution locks before moving any table or index. Neither these
observations nor an existing reservation enables Apply.


## Registered destinations and bound movement reviews

The internal registration boundary saves one prepared tablespace per database
identity and target under the shared storage-management lock. It accepts only
fresh verified observations for the current database and enrolled active history
targets. The configured root recorded by the verifier must match current
enrollment, preventing reuse of an observation from an old target root. Existing registrations are immutable: changing the OID, name, location,
directory paths, catalog version, target root or filesystem UUID is refused.
A tablespace cannot belong to two targets in the same database. Registration is
idempotent and transactional; it creates no PostgreSQL tablespace or directory
and is not yet connected to a public API or operator command.

Stable registration excludes the current device number and directory inode.
A remount or physical restore can change those observations without changing
the registered identity. This is not automatic restore approval: the filesystem
verifier must supply fresh matching evidence, every move review binds the
current observations, and running/uncertain work still requires reconciliation.
Changed database identity or stable destination details require an explicit,
reviewed recovery/cutover; they are never silently repointed.

The destination-bound review wraps the pure placement plan with the exact
verified destinations used by its moves. Its hash includes both the placement
evidence and destination OID/name/location, target UUID, current device and
directory inode. Missing destination proof blocks the entire move batch and
clears all additional copy reservations. The pure placement hash alone is no
longer an admissible journal review.

Before reserving, the journal recomputes this review and checks each destination
against immutable registration. Each durable move stores its destination
evidence and has a foreign key to its registration. A new hash does not
override a changed registration. Registration/review only record intent and
capacity ownership; the physical executor, runtime wiring, crash recovery and
policy activation remain absent. These canonical model additions require the
later explicit deployment cutover, with no runtime backfill of old intentions.


## Locked single-group observations

The internal read_locked_header_group boundary observes one registered daily
heap and its complete ordinary-index group on the caller's existing READ COMMITTED
connection. It takes an ACCESS EXCLUSIVE lock on that exact date-derived child,
checks the expected heap OID and attachment/bounds, and holds a key-share lock
on its registry row. It never opens a second connection or commits the caller's
transaction. Unrelated children are neither inventoried nor locked by this
boundary. The caller must roll back on any error.

It shares catalog validation and file/TOAST accounting with the complete reader.
The operation uses a declining statement budget, honors a shorter caller
statement timeout, and restores that setting after success. A subsequent call
on the same transaction sees the caller's table/index DDL. This avoids checking
a moved group on a second connection that would block on the worker's own lock.

Its inventory explicitly identifies the selected storage day and remains
inventory_complete=false. The filesystem adapter can verify that one group's
files and prepared destinations while retaining this partial status. Global
placement/review refuses partial inventories and cannot create reservations
from them. A partial observation is not a capacity reservation or proof of
policy eligibility, storage-management ownership, successful commit or recovery.

This observer alone does not acquire policy/intent ownership, authorize DDL,
change rows or enable Apply. The inspection and atomic movement boundaries below
compose those responsibilities. Runtime execution still requires whole-system
copy/WAL/temp/growth admission and supervision.


## Inspecting a reserved move

inspect_reserved_header_move retains the storage-management advisory lock and
uses the same caller transaction for a fresh locked-group catalog observation
and filesystem verification. It accepts a move ID and its saved review hash;
it does not accept a caller-provided physical certificate. The plan must remain
queued/running at the admitted policy revision, the batch must be complete and
uncancelled, and the selected move must still be reserved. Running, blocked and
terminal moves are refused by inspection. The atomic primitive below separately
reconciles completed moves against their durable physical evidence.

The inspection compares database identity, historical eligibility, immutable
tablespace registration and the exact destination evidence saved in the review,
including device/inode observations. The source heap and ordinary-index membership
must retain their original OIDs, file identifiers, schema/names and target IDs.
Current bytes may shrink or grow within the original reservation; an overgrown
copy requires a new reviewed reservation. Malformed or internally inconsistent
stored intent is refused.

The returned moving-members list excludes relations already on the selected
target, preserving their existing tablespaces and avoiding unreserved extra
copies. Capacity must still cover the current copy, other aggregate claims and
the policy reserve, subtracting this move's own reservation exactly once. Known
active claims within its bounded batch cannot exceed aggregate reservations.
This does not certify lifetime ledger accounting across every batch.

Inspection changes no journal state, capacity reservation or physical location.
A competing cancellation receives the same busy-owner response until the
transaction ends. Logs identify inspection and its copy-only capacity scope.
The result explicitly reports execution unavailable: WAL, temporary files,
ingest growth, physical execution/reconciliation and recovery/performance
qualification remain uncovered by inspection alone. Initial activation stays blocked.


## Internal atomic movement primitive

stage_header_move is an internal primitive for disposable qualification and a
future budgeted worker; no API, CLI or scheduler calls it. It uses the caller's
READ COMMITTED transaction and wraps its own operations in a savepoint. This
rolls back native DDL and journal changes on Python failures as well as SQL
errors, while preserving work the caller performed before the savepoint.
The outer caller remains responsible for commit/rollback.

After reserved-move inspection, it moves only the listed heap and ordinary
indexes to the verified tablespace, with dialect-quoted catalog identifiers and
a declining statement timeout. The heap carries its TOAST data. Post-copy
catalog/filesystem verification uses the same connection and retained locks.
Logical membership and names must remain unchanged, copied members must be in
the intended tablespace, and retained members must keep their exact physical
identity. Byte growth alone is not part of completion identity.

Completion evidence and the completed state, together with release of this
move's reservation, are staged in the same transaction as the DDL. The bounded
64 KiB evidence records database/day/heap identity and each member's OID, file
identifier, target UUID, device, inode, tablespace and server-path hash.
State/evidence constraints reject completed records without a bounded v1 proof
and reject proofs attached to other states. This is a canonical model addition;
existing installations containing older draft tables require an explicit
reviewed schema cutover, never an implicit startup alteration.

A retry of a completed move re-observes registered files under ownership and
group locks, compares the saved completion identity, and performs no DDL or
second release. Changed files, a changed destination, or incomplete proof
requires reconciliation. If a prior backend is still running, ownership is busy;
a client timeout is not evidence that its transaction rolled back.
Lifecycle logs and returned receipts remain provisional until caller commit.

This does not make automatic execution ready. Verified WAL/temp/ingest-growth
headroom, outer worker supervision, full current/frozen query qualification,
historical read latency during actual copies, and data-preserving deployment
migration remain required before runtime wiring or Apply activation.

### Supervised execution of an existing reservation

The internal execute_reserved_header_move boundary owns one transaction and
commits the existing atomic primitive. It creates no plan, new reservation,
schedule or policy activation. An unfinished move must already hold a valid
resource claim. Fresh admission verifies its resource paths, both drive
identities, current policy/intent and sufficient copy/WAL/temp/growth headroom.

While the statement runs, a watcher checks shutdown, the admitted time budget
and current filesystem availability. It protects the policy reserve and other
jobs' claims, and refuses net consumption beyond this move's saved copy and
auxiliary allowance. Cancellation targets only the current psycopg2 backend.
The outer connection stays checked out through commit/rollback and watcher
shutdown; if the watcher cannot stop within its grace period, that connection
is invalidated before it can return to the pool.

Completion and reservation release remain in the DDL transaction. Cancellation
before commit rolls them back together. A failure around commit has an uncertain
client outcome; retry uses the existing completion reconciliation instead of
assuming rollback or repeating the copy. A completed retry verifies the saved
physical result without moving files or releasing space twice.

This is a sampled net-space guard, not per-backend WAL/temp attribution or an
instantaneous limit on every filesystem allocation. Qualified headroom and the
cancellation margin must cover concurrent producers and observation delay.
The actual HDD workload and preserving deployment remain unqualified. The
settings-only Apply path does not perform or certify that initial cutover.

### PostgreSQL process identity during movement

The PostgreSQL 15 catalog observer reads the first 4096 bytes of the fixed
`postmaster.pid` file through `pg_read_file`. Its first three lines (PID, data
directory, process start time) must exactly match the local file before physical
placement can be verified. The observer therefore requires permission to read
that server file; missing permission fails closed. This internal worker
requirement does not grant file access to the portal or to a UI-selected path.

PostgreSQL records `MyStartTime` in that file and samples `PgStartTime` later;
`pg_postmaster_start_time()` is not required to equal the file timestamp.
The SQL timestamp must fall between the process start and the observation.
Cluster ID, process executable, shared control-file identity, filesystem UUIDs,
and relation/destination identity checks still apply and are repeated at the
end of verification. See PostgreSQL 15
[process startup](https://github.com/postgres/postgres/blob/REL_15_STABLE/src/backend/postmaster/postmaster.c)
and [PID-file creation](https://github.com/postgres/postgres/blob/REL_15_STABLE/src/backend/utils/init/miscinit.c).


## Movement resource envelope

The pure assess_header_move_resources boundary evaluates declared headroom for
a single historical move across every supplied registered filesystem. It
requires fresh, timezone-aware capacity observations (at most thirty seconds
old), complete per-target copy claims, temporary and maintenance allowances,
and non-WAL/non-temporary growth rates. The move timeout is bounded to one hour;
an explicit one-to-sixty-second cancellation grace also counts toward growth.
Elapsed observation age is rounded up and added to that growth window, so space
consumed since the free-space reading is not assumed available.
WAL allowance is explicit and includes all expected additional retained WAL
from observation through that window; max_wal_size alone does not establish it.

Each filesystem is counted once by target UUID and current device identity.
Roles sharing a drive add their copy, WAL, temporary, maintenance and growth
requirements before comparison with available bytes and the policy reserve.
The selected move's own copy claim is removed from the aggregate exactly once
and replaced with its current copy requirement; competing claims remain.
Copies exceeding their reservation refuse assessment. Already occupied space
is reflected in free capacity, and no future source deletion is credited.

Incomplete maps, unknown targets, stale/future capacity, invalid or duplicate
filesystem identity and numeric overflow refuse assessment. Read-only storage
and insufficient headroom produce explicit blockers. Zero allowances must be
specified rather than inferred from missing values.

A sufficient result is conditional on the declared limits. This calculation
does not inspect WAL/temp paths, prove or enforce the limits, reserve auxiliary
capacity, supervise a worker or enable Apply. Those runtime responsibilities
remain required before composing it with the atomic movement primitive. No new
portal controls are introduced by this internal calculation.


## PostgreSQL resource-path observations

observe_header_resources observes PGDATA, WAL and the caller session's new
temporary-file and temporary-relation locations on its active READ COMMITTED
connection. It shares binary/process/cluster identity checks with header-file
verification, honors a tighter caller statement timeout, restores that setting
on success and requires caller rollback on failure. It creates no files,
tablespaces, directories or additional database connections.

The observed database default tablespace is always included as a temporary-file
fallback. Quoted temporary tablespace identifiers (including embedded quotes and
commas) are split within a bounded list and normalized by PostgreSQL parse_ident.
Missing or inaccessible named spaces refuse observation rather than silently
assuming a fallback. This is conservative relative to PostgreSQL's own fallback
behavior. Catalog, process identity, directories and filesystem bindings are
rechecked before returning evidence.

Every resource directory must be writable by the database OS owner and belong
to one registered filesystem UUID/device. Directory identity must match through
the postmaster's process root. A relocated pg_wal link must agree in worker and
server namespaces; its target is verified against registration. Tablespace
links must likewise agree with catalog locations. Unexpected directory links,
unregistered paths and namespace disagreement refuse observation.

For a not-yet-created pgsql_tmp or per-database temporary-relation directory,
the observer proves the existing immediate parent and absence in both
namespaces. It records that distinction and never creates a placeholder.
Existing children are checked directly, including ownership and writability.

This evidence covers the caller's allocation settings, not every producer's
existing temporary objects or allocations on other sessions. It does not
qualify peak rates, enforce resource limits, reserve capacity or enable Apply.
The reserved-move resource inspection now repeats these observations under
movement ownership and composes them with the resource-envelope calculation.
Accounting for other producers remains required before execution.

The PostgreSQL 15 behavior is defined by
[temporary tablespace selection](https://github.com/postgres/postgres/blob/REL_15_STABLE/src/backend/commands/tablespace.c)
and [temporary file paths and fallback](https://github.com/postgres/postgres/blob/REL_15_STABLE/src/backend/storage/file/fd.c).


## Reserved-move resource inspection

The inspect_reserved_header_move_resources boundary combines the saved move,
current physical group, PostgreSQL resource paths and declared headroom on one
caller transaction and connection. It retains storage ownership and the selected
daily table lock, derives the WAL and temporary targets from verified bindings,
and requires both observations to identify the same enrolled filesystems.
Database identity, backend PID, observation ordering, freshness, policy revision
and saved review must still agree. Caller maps are copied before probes begin.

The budget uses fresh available capacity and current aggregate copy reservations.
The selected copy replaces its own claim once; it does not reserve new capacity,
move files, update the journal, commit or activate policy. Shortages report the
affected drive, including a source drive whose growth or WAL would exhaust it.

Inspection phases share a declining deadline, clamped to a tighter caller
statement timeout. A late result is refused, and the original timeout is restored
on success. Caller rollback remains required on failure. This bounds SQL and
rejects stale completion; cancellation of a stalled filesystem call still needs
worker supervision.

Even a sufficient result has execution disabled. Declared limits are estimates,
not enforced ceilings. Other producers and existing temporary objects, durable
auxiliary reservations, resource-limit qualification and worker supervision
remain uncovered. This internal preview adds no portal control or execution
entrypoint.


## Durable auxiliary reservations

A reserved move may now hold an immutable resource claim alongside its copy
intent. The claim records reviewed limits, their hash, the verified WAL target,
observation growth window and amounts bound to registered filesystem UUIDs.
Each target keeps separate aggregate copy and auxiliary counters; status reports
their combined reserved capacity. New copy admission counts both. Combined
resource inspection replaces this move's own auxiliary claim once while retaining
other claims, so retrying a preview neither double-counts nor forgets demand.

Acquisition uses current real resource inspection under storage ownership and
a caller-owned savepoint. A duplicate request acknowledges the original durable
claim; changed limits refuse reuse. The receipt is not fresh path evidence or
execution permission. Numeric, UUID and hash consistency are checked when the
claim is reused or released, and missing aggregate ownership refuses release.
Claims do not expire or disappear merely because a client times out.

Verified atomic completion releases auxiliary and copy capacity with file
movement and completion evidence in the same transaction. Unstarted whole-batch
cancellation releases both under the same ownership lock. Rollback preserves
both claims; completed retries do not release again. Terminal records retain the
claim as bounded audit evidence. No new public executor is enabled.

These are clean-model columns, not a runtime migration. Existing installations
require an explicit reviewed schema cutover. The claimed allowances remain
declared estimates. The supervised execution boundary above checks net space
and cancellation; qualified producer rates, application performance and the
existing-data migration remain required.


## Bounded local recovery generations

The internal recovery_copies operation writes one PostgreSQL custom dump and
the snapshot's referenced archive objects to a private directory on the existing
HDD. It admits only a repeatable snapshot holding the actual shared archive-expiry
fence. Raw/checkpoint objects already recorded expired are excluded; canonical
archive pages remain required. Collection may continue outside the snapshot.

The configured drive UUID is rechecked during work. Byte, object, elapsed-time
and free-space limits stop the attempt without publishing a completed copy.
Object lengths and SHA-256 are checked; the dump subprocess is terminated and
reaped on failure. The explicit object budget may be set up to ten million;
the default remains one million. September 2026 measurements already observed
about 380,000 raw objects and roughly 11,000 new objects per day, so an unchanged
one-million ceiling is insufficient for the requested retention horizon.
Duplicate archive keys are rejected by exclusive creation in the private
generation, without retaining every key in process memory. Catalog reads remain
paged at 256 rows. Raising this ceiling does not qualify duration, byte capacity
or retention throughput: those operating budgets still require measurement. Completion is published only after files and directories are
synced. A failed attempt retains earlier completed copies. Retry removes only
marked private partial copies belonging to this database and drive.

Rotation starts only after a new copy is durable. Old generations are renamed
out of the completed set before retirement; their ownership marker is removed
last, allowing interrupted retirement to resume. Unowned trees, symlinks, foreign
filesystems and special files are refused. Local copies share the HDD failure
domain with history; they are not off-server protection.

This low-level copy operation does not itself schedule work or handle Apply.
The due-copy service and configured worker described below acquire its snapshot
fence, exclude competing storage jobs, supply the PostgreSQL utility/connection
and report operational health. Those connections are implemented and locally
tested. The remaining activation blockers are the complete server filesystem
and ownership rehearsal, measured operating limits and preserving cutover.
The copy's own filesystem limits remain local guards. No new placement controls
are added.

The restore rehearsal covers a preserving v1 handoff followed by new v2
observations, corrections and raw archive publication. Recovery must restore the
active snapshot and its archive objects, admit the active layout and accept new
collection. Retained v1 tables are historical evidence; they cannot substitute
for that snapshot after new writes resume. Migration progress copied by a logical
dump is not permission to resume an incomplete migration in a different database.
The fixed migration's database/OID/physical admission remains required.

Every new completed receipt records the storage-layout version and a hash of the
layout certificate visible in the same repeatable database snapshot as the dump.
The copy completion time alone cannot prove which side of a migration the dump
contains. The scheduler skips an interval only when the latest completed copy
matches the current layout certificate. A legacy receipt without this evidence,
or one for a different layout, requires a new bounded copy even if recent.
Existing copies remain available until normal successful rotation. A lost
completion response reuses the published matching generation on retry; it does
not create another generation solely because the earlier caller lost its reply.
This evidence does not replace restore testing or physical storage admission.

### Due-copy maintenance admission

The internal run_due_local_recovery service reads the existing saved interval and
copy count. It uses the existing storage-management transaction lock to exclude
movement and new reservations while making a recovery copy; current reservations
remain unavailable. A busy owner skips this attempt. It verifies PGDATA, WAL and
temporary allocation roots with the existing PostgreSQL namespace observer.
The configured recent SSD must actually serve PGDATA, default database files and
WAL. The archive root must belong to a registered archive target.

Before starting, it admits the explicit copy byte budget plus the policy reserve,
existing reservations and caller-declared growth/allocation headroom. During work
it rechecks both drives, capacity and the still-held database ownership. The
primitive checks shutdown cancellation between chunks and while polling pg_dump,
then kills and reaps that child if cancelled. The archive-expiry snapshot uses
nonwaiting fence admission so an ongoing expiry cannot make shutdown wait for
an unbounded lock.

This service supplies a due decision and copy execution, not a second scheduler.
Its budgets must come from the measured deployment plan. The existing lifecycle
supervisor accepts optional history and recovery runners after retention releases
its transaction. Each phase has an independent outcome and shutdown cancellation;
a failed history phase does not suppress the recovery attempt.
The production entrypoint accepts explicitly configured operating limits through
the runtime connection below. The default server composition does not provide
the verified namespace; settings-only Apply does not provide it. Deployment
composition and final measured limits remain necessary; portal status consumes
worker evidence only when those runners are actually configured.

### Saved-policy historical maintenance

The internal run_history_maintenance service moves at most one eligible day per
pass using the saved policy, complete catalog, registered destinations and the
existing plan, move and resource-reservation journal. The bounded planner still
validates the entire catalog. Later eligible days are explicitly deferred and
receive no reservation in this pass. Already placed history remains on its
allowed HDD; adding a target does not trigger rebalancing. The existing unlimited
review and its hash remain unchanged.

The service requires recent SSD and historical HDD assignments, prepared
registered tablespaces, verified physical paths and explicit measured resource
limits. It creates neither tablespaces nor a new scheduler. A new intent and all
capacity claims commit together; failed admission leaves neither behind.
Another storage owner causes a busy result. A reserved intent is retried before
new work is planned. A lost completion response is reconciled against the durable
move and re-observed physical files before the plan is marked complete, without
moving the day or releasing capacity twice.

A changed/disabled policy or changed resource configuration cancels only a
wholly unstarted one-group intent and releases its claims. Committed placement
is preserved. Execution failures remain visible as blocked plans; they do not
turn into successful or silently skipped work. A complete pass releases its
ownership before recovery is considered on the existing lifecycle thread.

This is an internal execution seam, not public activation. The production
entrypoint accepts explicit limits through the connection below, but the server
still needs the verified PostgreSQL filesystem namespace and qualified budgets.
Portal health projects its persisted phase evidence. Initial policy installation
still requires the operator cutover; settings-only Apply cannot bypass it.

### Explicit runtime operating limits

The collector entrypoint supplies history and local-recovery runners to its
existing lifecycle supervisor when storage.maintenance_limits_path is configured.
QT_STORAGE_MAINTENANCE_LIMITS_PATH is the environment override. Its default is
null: existing deployments do not acquire new movement or backup behavior merely
by loading this code. The saved database policy still controls eligibility,
enablement, backup interval and rotation count. The limits file does not contain
placement policy, credentials, another DSN or executable paths.

The file is read once at worker startup from an absolute administrator-provided
path, bounded to 128 KiB. Missing configured files, duplicate JSON fields,
unknown fields, invalid versions and invalid limits fail before runners are
created. Changing limits requires a worker restart. The exact top-level fields
are schema_version (qt.storage_maintenance_limits.v1), history and recovery.

| Section | Required limits |
| --- | --- |
| history | wal_bytes; temporary_bytes, growth_bytes_per_second and maintenance_bytes keyed by registered target ID; movement_timeout_seconds; cancellation_grace_seconds |
| recovery | max_bytes; timeout_seconds; headroom_bytes keyed by the recent and backup target IDs; max_objects |

Both operations retain their existing transaction, filesystem, capacity,
cancellation and policy checks at execution. Runners share the existing database
object and archive root; PostgreSQL 15 utilities use the fixed /usr/lib/postgresql/15/bin
paths used by the qualified disposable topology. Constructing the runners does
not inspect drives, perform schema changes, create recovery copies or activate
policy. The same supervisor and heartbeat own schedule and status.

Valid configuration proves only that limits have the required shape, not that
they are measured or sufficient. Production values require the workload and
capacity evidence in the release checklist. The runtime image includes
PostgreSQL 15 control/dump utilities without creating a distribution-managed
database cluster. The default server mounts
still do not provide the matching PostgreSQL namespace. Configure the
runtime only as part of the explicit preserving cutover after those deployment
prerequisites and the bounded preserving migration are qualified. No live
configuration or deployment is implied by this local implementation.

### Runtime-image storage rehearsal

The existing storage-demo topology now builds the backend Dockerfile's
storage-test target. It inherits the deployable runtime code, locked Python
dependencies and PostgreSQL 15 utilities, adding only test inputs. The default
production target inherits the same runtime without those storage test inputs.
The topology still uses isolated PostgreSQL and two disposable filesystems,
UID 70 and a shared PostgreSQL PID namespace; it introduces no live host mounts.

This closes the test-image dependency difference when the demonstration passes.
It does not qualify current server file ownership, change server Compose mounts,
or authorize activation. The real-process rehearsal runs the backend supervisor
and its indicator/research workers alongside collector maintenance as UID 70.
The initializer installs code-owned instruments with all provider enrollment
disabled. The Storage API must read both drive identities, and a fresh process
using the production repository must return the same frozen record identities
and hashes before and after history movement. The fixture retains an internal-only
network and matches the server's PostgreSQL extension preload.

These checks target common file-owner compatibility. They do not grant the
backend a real Docker socket, convert root-owned retained files, exercise full
provider load, or qualify the final server Compose mount arrangement. Those
remain explicit prerequisites for the preserving deployment, regardless of
whether the local process rehearsal passes.

Archive object manifests use logical market-archive keys resolved beneath the
configured object root. A verified root relocation need not rewrite those keys.
Preservation still requires copying/verifying every referenced object and
handling retained acquisition spool paths explicitly during the cutover.

### One saved recent-data window

When explicit maintenance limits configure the existing worker, its canonical
payload retention service reads the same saved Storage policy as header movement.
The saved recent-days setting replaces the legacy canonical hot window and
per-Fact-type overrides. Missing policy or disabled movement prevents canonical
archival execution; saving a policy never enables disabled deployment execution
gates. Unconfigured deployments and their manual lifecycle behavior remain
unchanged. Existing raw-object compaction and expiration settings are unchanged.

Planning checks the assigned HDD archive root and its filesystem UUID. The
first release retains one archive root; adding configured history targets does
not force that existing archive root to move or initiate rebalancing. The
effective free-space floor includes the saved reserve percentage, existing
reservations and any stricter canonical operating limit. Each archive/reclaim
transaction takes the existing management lock and rechecks the saved policy
revision/hash, movement enablement and destination. A changed policy rejects the
old plan; it does not authorize a step under stale settings. The reclaimer repeats
this guard at its final exclusive handoff. Committed archive pages remain the
existing resume authority.

The disposable real-process scenario changes the saved recent window, pauses
movement, resumes payload archival and header movement, and reads the same frozen
results. A separate injected pause after planning must prevent sealing. These
checks qualify the policy connection only on the disposable layout, not actual
HDD performance, existing-server permissions or the full preserving migration.

### Preserve recent intake on SSD

Live stream spool and raw encoding scratch have an explicit optional working
root, separate from the HDD archive root. Preserve the current SSD directory as
MARKET_STRUCTURE_WORKING_ROOT and configure its
QT_MARKET_DATA_WORKING_EXPECTED_UUID when preparing the two-drive release.
MARKET_STRUCTURE_STORAGE_ROOT can then identify the verified HDD archive path
without rewriting existing spool paths. Both mount identities are checked before
use, including before repairing a retained incomplete spool.

No default path changes or new portal controls are introduced. The existing
archive configuration remains the fallback when no working root is configured.
Raw compaction and canonical archive staging retain their archive-side behavior;
streaming scratch uses the working root. Qualification must include retained
files, common writer ownership, source-disk scratch peaks and failure recovery.
The server Compose mount/user integration remains an explicit release blocker;
this code does not activate or move live storage.

### Runtime access to archived history

The server backend receives the existing host QT_MARKET_DATA_ROOT assignment
and gives spawned bot readers a read-only archive bind at the configured
container root. The runtime receives MARKET_STRUCTURE_STORAGE_ROOT explicitly.
Dedicated filesystem mode requires the host mapping and read-only udev metadata
for the same UUID checks. This closes the launch-time archive visibility gap;
it does not by itself qualify all server permissions or rehearse the preserving
production migration. No advanced placement or rebalancing setting is added.


### Fixed server runtime packaging and layout

The optional server storage overlay preserves the current SSD working/spool and
PostgreSQL paths, exposes the prepared HDD consistently at `/qt-history`, and
uses UID/GID 70 for application archive writers. Only the collector shares the
PostgreSQL PID namespace for physical maintenance observation. Inventory and
maintenance limits are read-only prepared configuration; the existing saved
policy remains the placement/schedule authority. The normal deployment helper
does not select this overlay automatically; activation belongs to the preserving
cutover under its persistent host hold.

The production backend image now includes only the exact preserving-operator
Python dependencies, covered by the ordinary runtime source-tree fingerprint.
No manual SQL bundle or implicit startup migration is added. UID-owned image
scratch/report directories support the non-root services without modifying host
file ownership. See ADR 0068 and the server deployment guide for the fixed
layout inputs and disposable actual-core rehearsal scope. Real host permissions,
Docker socket group, full cutover/recovery and HDD performance remain separate.


## Fixed runtime directories on the prepared HDD

The existing storage_host_prepare helper has a separate
--prepare-runtime-directories action. It admits the already mounted ext4 device
by the reviewed serial, size, UUID and mountpoint, then creates or verifies only
data and data/archives under that mount. It never formats, mounts, edits fstab,
moves database files or recursively changes existing files.

Those roots use the pinned application/PostgreSQL UID 70 and the named host
operator's primary group, with mode 0770. Both the runtime and deployment operator
can pass their root-directory write checks. PostgreSQL tablespace directories and
private archive files retain their own stricter ownership/permissions. Use the
returned history_root for QT_STORAGE_HDD_ROOT and archive_root for
QT_MARKET_DATA_ROOT when preparing the later reviewed runtime configuration.

Preparation uses directory descriptors without following symlinks. Children must
remain on the admitted writable HDD filesystem. Re-entry preserves correctly
prepared directories and their contents; it can finish only an empty interrupted
root-owned or runtime-owned directory. Foreign ownership, nonempty inconsistent
permissions, symlinks, nested foreign filesystems and read-only mounts refuse.

The disposable native rehearsal uses real process UIDs, filesystem permissions
and tmpfs mounts; only hardware audit/findmnt observations are synthetic. This
does not qualify the live disk or transfer ownership of existing SSD files. That
existing-data permission admission and the complete host cutover remain required.


### Preserving database process boundary

The packaged fact_header_v2_handoff module accepts a bounded internal stdin
request for the existing checked destination preparation and database sequence.
It requires UID 70, matching image provenance, the expected database identity,
exact existing roots and one fixed SSD/HDD policy. It does not use application
startup to bootstrap or migrate a schema. The host must retain the publisher
pause and durable hold through runtime activation; successful database completion
explicitly does not authorize collection to resume. PG_DSN remains the sole
connection setting. Errors expose a guard code/type, never SQL values or DSNs.


### Fixed service activation after preserving migration

The internal held runtime handoff consumes the same image-pinned candidate recipe
and database request. It keeps the existing deployment lock and durable hold
while starting only the fixed application services; the prepared PostgreSQL
container and its existing volume are retained. Candidate images, environment,
commands, mounts, network and health are checked against the private snapshot.

Before retiring the hold, inspection inside the collector's PostgreSQL namespace
reconciles the committed layout and current policy, checks its live maintenance
heartbeat, and verifies an actual completed recovery generation for that exact
snapshot layout. The existing collector makes that copy. A pending or degraded
maintenance phase retains the hold; a heartbeat claiming success without a
matching published copy does not qualify. Inspection does not migrate, change
policy or create a recovery generation.

A private fixed activation record precedes service changes. Interrupted startup
reuses the matching candidate and refuses changed configuration. Verified release
bytes are recorded before the hold is removed, allowing lost completion replies
to reconcile without reverting to old software. The initial release records
ssd-hdd-v1 and leaves previous_revision empty: the pre-migration image is not an
automatic rollback for new-layout writes. Its revision remains in migration
evidence. The original bounded attempt deadline is not extended on retry.

This implementation still requires the combined real-service rehearsal and
physical-drive qualification before deployment or a public activation control.

### Preserving-copy throughput

The fixed header migration reuses its code-defined SQLAlchemy table declarations
between pages. This caches no database observations: source definitions, capture
state, copied content and physical placement are still admitted on every page.
A physical-drive profile identified repeated reconstruction of the entire model
graph as a significant part of the copy time. The optimization does not change
transaction boundaries, the original persisted deadline or recovery. The internal
operator admits up to 4,096 rows per page, matching the existing header and raw
copy primitives; the default remains 128. Measured requests may select the larger
bounded batch while retaining the same per-step duration and resource limits.
Archive copying and final object inventory keep their separate 256-object page
ceiling and byte budgets.


### Recent ingestion during historical movement

Hot-payload validation and the deferred identity/header check execute their
existing parameterized header lookup with a plan made for the concrete storage
day. A cached generic parent plan can acquire locks on historical partitions
even when execution later prunes them; that blocked new collection during an
exclusive history move. Replanning these two checks preserves every identity and
relationship condition while keeping unrelated historical locks out of recent
ingestion. Full-model ingestion is exercised with both automatic and forced
generic planning while another connection holds a historical partition lock.
Clean bootstrap and the explicit v1-to-v2 cutover install the corrected bodies;
startup refuses mismatched installed bodies instead of silently replacing them.

## Encrypted incremental recovery candidate — September 24

The user authorized replacing repeated logical full copies with encrypted
incremental recovery. The existing logical-copy implementation and completed
receipts remain intact while its replacement is qualified. The disposable
physical/database-plus-archive experiment is described in
[ADR 0072](../decisions/0072-use-encrypted-incremental-recovery-sets.md).
It does not activate a new backup scheduler, alter saved retention semantics or
qualify a production recovery. Report this candidate separately from the
currently implemented local-copy maintenance path above.

### Encrypted recovery runtime (explicit opt-in, not yet activated)

Maintenance-limits schema qt.storage_maintenance_limits.v2 adds a required
recovery.incremental object containing pinned tool paths, the verified physical
PostgreSQL directory/socket, private database/archive key paths and a bounded
max_chain_backups. All other saved policy and operating-budget semantics remain
unchanged. v1 configuration continues to select logical recovery copies.

EncryptedRecoveryCopies publishes a physical backup label and encrypted restic
snapshot as one recovery point under the existing storage ownership and archive
fence. Repository preparation is explicit; missing identity/key/repository state
fails closed. Legacy generations are preserved in their original directory.
Retirement keeps every physical dependency needed by the retained usable points;
interrupted cleanup resumes before reporting a point as not_due. Native tools
inherit no ambient credentials or configuration, and their child processes are
bounded by the existing cancellation, deadline and filesystem reserve checks.

Production images contain pinned tools. Image availability does not enable WAL
archiving, provision keys, qualify a restore or activate a backup policy. Those
operator steps and full QT application recovery remain pending under ADR0072.

The encrypted recovery deployment uses the fixed storage overlay's private SSD
recovery-key bind (PostgreSQL and collector only) and shared database socket.
It does not activate archiving implicitly. Native WAL expiry retains consistency
logs for all saved points while bounding obsolete between-backup history;
arbitrary later PITR is outside the paired archive contract. The existing
maintenance failure surface reports disabled or failing PostgreSQL archiving
even when the next backup is not due.

Release validation runs the fixed SSD/HDD core recreation and actual held
preserving-cutover fixture in the existing deployment-contract CI job. The latter
exercises the explicitly admitted recovery key/socket mounts across database
preparation and interrupted activation. The separate actual encrypted recovery
fixture verifies the configured receipt format and stale-generation rejection.
These disposable tests do not replace physical-drive performance qualification.

The fixed preserving staging sequence relocates the completed identity table and
indexes to the HDD before starting the raw lookup copy. Their full temporary
SSD allocations therefore do not overlap. Relocation and its recorded placement
commit together; an interruption before raw staging retains the source and the
original attempt clock. Retry verifies the HDD placement, catches up intervening
source inserts and reuses the committed relocation. Identity mirroring remains
a later bounded source-fence step. This ordering does not authorize source
deletion or relax filesystem reserves.

The preserving operator also relocates the immutable retained v1 rollback table
to the HDD before allocating the new header and raw lookup copies, when that
legacy table exists. This addresses measured SSD staging pressure without deleting
the rollback data or changing retention. The existing fixed-table mover admits
only the named, write-fenced ordinary table with no incoming reference/view or
inheritance dependency. It preserves heap, TOAST, indexes, relation identity,
logical definitions and guards in one transaction, verifies physical placement,
and reuses committed placement after interruption. The existing capacity/WAL
watch and original migration deadline apply. A clean installation with no legacy
rollback table has nothing to relocate; no generic placement control is added.

### Initial migration duration

The explicit initial operator accepts at most 96 hours, justified as a bounded
prospective allowance by the 47.90-hour partial copy/verification projection.
It is not a whole-migration ETA or proof of acceptable collector downtime.
At first preparation, capture persists the requested duration with prepared_at.
Every phase and retry uses that original clock. Existing captures without the
new column retain 24 hours and are never altered or revived. Direct capture/copy
primitives default to 24 hours. The earlier host deadline and per-step statement
timeouts still apply. Routine automatic movement remains limited to one hour;
longer initial admission must count its entire growth window. See ADR0070.


### Migration and routine budgets at activation

The initial migration request retains its original multi-day attempt budget.
The prepared routine maintenance file is separate: activation validates it with
the pinned worker image's existing strict parser, including the one-hour movement
ceiling and exact SSD/HDD target IDs for both history and recovery. The host does
not require the two budgets to be equal, because collection resumes with different
growth allowances and routine work must not inherit a multi-day timeout.

The validation container has no network, database mount, keys or Docker socket;
only the limits file is mounted read-only. The host checks the file again after
validation and binds its hash in the existing activation receipt. Changes during
validation or an interrupted activation remain refused. This changes no saved
placement/retention policy, capture deadline or automatic restart behavior.


### Online migration preparation

[ADR 0073](../decisions/0073-prepare-storage-migrations-with-live-collection.md)
records the replacement for the rejected whole-migration collector hold.
The internal fact_header_v2_online.copy_pass commits bounded verified pages
while the existing v1 source continues serving. It requires the exact prepared
placement, preserves the original capture deadline and enforces existing
resource guards. Finite baselines precede fairly alternating catch-up passes.
Private relocation is a separate phase; source data is never deleted.

An empty tail observation is not switch readiness. Newly empty shadows may opt
into protected exact page proofs before copying: source-equal insert guards,
immutable targets, monotone routing, direct-leaf truncate guards and persistent
leaf identities keep committed page verification valid. The final SQL fence
closes capture and checks exact guards, definitions, partition catalog and
physical placement without scanning every header/identity/raw row. Guard removal
and the existing SQL switch commit or roll back together. Older unprotected
shadows retain the full verifier; proof cannot be retrofitted after copying.

Archive preparation also has durable capture for the three fixed immutable
manifest families. Each finite baseline or captured-tail page uses the existing
guarded exact file copier and commits its cursor/queue changes in the same
transaction. Late lower-ID commits remain visible through capture. Failed pages
reuse published files without losing pending work. Expiry semantics, root
identity, source preservation and the original attempt deadline remain enforced.
An empty archive tail does not certify the files or authorize root activation.

Existing reference prevalidation is qualified with the protected shadow and
concurrent native v1 header/payload inserts. The final inventory catches new
payload leaves before adoption; newly created leaves after parent adoption
inherit validated references. Validation interruption preserves earlier committed
work, while failed SQL switching restores guards and reference state. This uses
the deployed v1 partition writer as a frozen fixture and does not establish
production-scale validation time or running collector throughput.

An optional live archive proof hashes copied files while holding Linux read
leases. Existing writers refuse protection; later write/truncate attempts
invalidate the proof. The final fence still enumerates the complete catalog and
checks every required path, inode and lease, then rechecks retained bindings
before leaving. It avoids rereading contents only while that exact controller
retains its proof. Restart requires bounded background re-verification; saved
hashes alone cannot substitute. File descriptors and proof bytes/time are
bounded without changing host limits. Descriptor/kernel-memory capacity and the
remaining metadata scan require measured admission. The default final verifier
still hashes all files. Publisher drain, invocation of terminal cancellation and runtime activation
remain separate unfinished host integration.

The complete online operator, archive reconciliation and measured short final
switch remain unfinished. The held host operator cannot be advertised as a
short-outage path. Additional source lookups in protected inserts require
throughput/collector-impact qualification. The old held-growth capacity
sensitivity must be replaced by live-ingestion and capture-backlog admission.

### Online archive capture retirement

The internal archive_root_v2_online.retire_capture helper is admitted only by
the live exact inventory context on the same transaction and matching roots.
It additionally requires complete baselines and an empty committed queue.
It removes only temporary catalog triggers and their insert function, retaining
original capture/progress/queue tables and a terminal inventory receipt.
The caller must include it in the final switch transaction. Rollback restores
capture for same-attempt catch-up; committed closure refuses prepare/copy reuse.
No files, source rows or saved attempt timestamps are changed. Expired attempts
remain refused, and a saved report cannot authorize retirement. The SQL switch automatically requires this live retirement when capture exists;
commit_handoff accepts the caller-owned live file proof through commit. Host
integration, publisher drain and activation remain unqualified. Expired-attempt
cancellation uses the separate terminal boundary below.

Archive capture preparation requires the raw-mapping shadow to be prepared
first: its foreign key adds native triggers to the raw manifest catalog.
Sealing the archive catalog binding before that DDL would correctly reject the
later trigger change. Existing bound captures are never rewritten to accept it.


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
