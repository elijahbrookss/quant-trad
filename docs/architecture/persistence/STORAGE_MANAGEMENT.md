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
  - cli/main.py
  - src/core/storage_targets.py
  - src/core/storage_header_placement.py
  - portal/backend/service/storage/header_catalog.py
  - portal/backend/service/storage/header_filesystem.py
  - portal/backend/service/storage/header_journal.py
  - src/core/storage_inventory.py
  - portal/backend/db/storage_target_models.py
  - portal/backend/controller/storage_management.py
  - portal/backend/service/storage_management.py
  - portal/frontend/src/adapters/storage.adapter.js
  - portal/frontend/src/v2/rooms/StorageRoom.jsx
  - portal/frontend/src/v2/rooms/storage.css
  - scripts/automation/storage_device_audit.py
  - scripts/automation/storage_host_prepare.py
---
# Storage Management

The Storage settings page enrolls host-prepared drives and reviews role
assignments. It is being implemented alongside the existing storage layout.
It does not yet move database records, publish archives across a drive pool,
schedule backups, or activate policy. Apply returns an explicit conflict while
that executor is absent. Existing collection and retention settings remain
authoritative until an explicit tested cutover replaces them.

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
does not prove that movement or backups are working. Their status is explicitly
unknown or unconfigured until authoritative execution evidence exists.

POST /api/storage/plans validates a complete policy against its base revision.
The durable request ID prevents retries from creating different plans.
Assignments and advanced settings appear in the impact preview. The browser
does not calculate readiness or silently activate a draft. A fresh review is
required after another policy revision. GET /api/storage/plans/{id} retrieves
the same server-owned plan.

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
the server execution blocker. CLI success from review means a plan was saved,
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
endpoint. Registered destination tablespace identity, locked pre-execution
checks, physical table/index DDL, crash reconciliation, execution logging and
full capacity coverage still belong to the future worker. No reservation
activates policy or moves bytes. Existing installations will require explicit,
reviewed schema preparation as part of the later operator cutover.
