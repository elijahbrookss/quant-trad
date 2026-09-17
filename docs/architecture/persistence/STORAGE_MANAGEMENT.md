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
future catalog adapter that proves parent attachment, daily bounds, complete
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
