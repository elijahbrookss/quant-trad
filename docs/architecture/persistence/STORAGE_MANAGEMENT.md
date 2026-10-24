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
  - scripts/db/manual_retire_promoted_identity_proof_v1.sql
  - scripts/db/raw_mapping_v2_placement.py
  - cli/storage_placement.py
  - scripts/automation/storage_online_keys.py
  - scripts/automation/storage_online_forward_worker.py
  - scripts/automation/storage_online_reschedule.py
  - scripts/automation/storage_online_forward.py
  - scripts/db/fact_header_forward_keys.py
  - scripts/db/fact_header_forward_adoption.py
  - scripts/db/fact_header_forward_placement.py
  - scripts/db/fact_header_v2_references.py
  - scripts/automation/storage_online_terminal.py
  - portal/backend/db/fact_header_legacy_schema.py
  - scripts/automation/storage_online_deadline.py
  - scripts/db/fact_header_v2_deadline.py
  - scripts/automation/storage_online_release.py
  - scripts/automation/storage_online_operation.py
  - scripts/ci/online_operation_fixture.py
  - scripts/automation/storage_online_runtime.py
  - scripts/automation/storage_recovery_prepare.py
  - scripts/automation/storage_online_repositories.py
  - scripts/ci/online_guarded_source_fixture.py
  - src/core/storage_writer_fence.py
  - portal/backend/run_backend.py
  - portal/backend/workers/single_node_initializer.py
  - docker/docker-compose.storage-server.yml
  - portal/backend/workers/market_data_collector_health.py
  - portal/backend/service/bots/runner.py
  - scripts/ci/test_server_core_recreation.py
  - portal/backend/workers/storage_maintenance.py
  - portal/backend/workers/market_data_collector.py
  - portal/backend/service/market/collector_operations_service.py
  - src/market_data/archive.py
  - src/market_data/archive_namespace.py
  - src/core/settings.py
  - scripts/automation/storage_host_boundary.py
  - scripts/automation/storage_handoff_pause.py
  - scripts/automation/storage_online_drain.py
  - scripts/automation/storage_online_recovery.py
  - scripts/automation/storage_online_final.py
  - scripts/automation/storage_online_prepare.py
  - scripts/automation/server_deploy.sh
  - scripts/automation/storage_online_controller.py
  - scripts/automation/storage_online_launch.py
  - scripts/ci/rehearse_online_launch.py
  - scripts/ci/rehearse_online_prepared.py
  - scripts/ci/online_start_reply_fixture.py
  - scripts/db/fact_header_v2_cancel.py
  - scripts/automation/storage_online_worker.py
  - scripts/ci/rehearse_online_worker.py
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
  - portal/backend/service/storage/retention_observability.py
  - portal/backend/service/storage/repos/query_diagnostic.py
  - portal/backend/service/market/canonical_retention.py
  - portal/backend/service/market/market_storage_lifecycle.py
  - portal/frontend/src/v2/rooms/StorageRoom.test.jsx
  - docker/grafana/provisioning/dashboards/capacity-database-growth.json
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
  - scripts/reporting/canonical_storage_query_diagnostic.py
---
# Storage Management

The Storage settings page enrolls host-prepared drives and reviews configuration.
After an explicit operator cutover has installed an applied policy, Apply can
save settings for that same layout. Initial setup and role changes remain
blocked: a UI confirmation cannot prepare a database or prove a migration.
The existing storage lifecycle supervisor performs history movement and local recovery
copies under its deployment gates, saved policy and explicit operating limits. The
settings page reports observed outcomes separately from saving configuration.

## Reading this storage manual

Use the capacity and observation sections to understand measured space and
progress. Use [the migration ownership map](#one-online-migration-workflow-and-its-owners)
before selecting a preserving/forward operator. Physical migration, application
activation, repository preparation, native WAL delivery and complete recovery
publication are separate outcomes. A saved policy, completed SQL transaction or
healthy process cannot substitute for the next outcome.

The lower sections document explicit operator/recovery seams and their refusal,
retry and ownership rules. Their presence in source does not attest that a
particular deployment has executed or qualified them. Installation identities,
private receipts and current job status belong to operator-owned records.

## Qualification and operation scope

This manual describes source-supported operations and their admission contracts,
not a live installation status or a history of passing fixtures. Commands that
mutate storage require explicit authorization for the exact release, operation,
source fleet and physical layout. Inspection, preparation, COMMIT, runtime
activation, complete paired recovery and deployment publication remain distinct
outcomes; no earlier receipt silently grants the next outcome.

| Evidence | What it can support | What still needs independent admission |
| --- | --- | --- |
| Unit/fault-injection checks | Exact input/refusal rules, journal preimages and uncertain-outcome handling | Real process/file ownership, SQL locks and physical cost |
| Disposable native PostgreSQL and Docker qualification | Transaction/constraint behavior, actual worker transport, interruption, rollback and restore for the tested layout | Production cardinality, physical-drive capacity, collector load and pause duration |
| Source attestation and prepared-runtime inspection | Matching immutable package, source, mounts, processes and configuration at observation | A completed operation or future resource availability |
| Durable operation receipt | The specific committed transition and its pinned identity/clocks | Fresh fleet health, collection continuity or a matching current recovery pair |
| Encrypted paired recovery/restore proof | Exact selected database/archive recovery point and supported restore behavior | Off-server protection or an unmeasured recovery-time guarantee |

Before a mutation, measure remaining copy/index/attachment work, growth, queue
occupancy, WAL/temp/recovery allocation and rollback time above reserve floors.
Account for the actual collector finalizer and pending-segment limits, not only
free disk. Longer query, batch or handoff allowances change permitted disruption;
they are not throughput or downtime measurements.

Every phase retains its original durable clock and owning session/fence.
Inspection never renews time. An absent reply or exited attach CLI is not proof
that SQL or a Docker request failed. Reconcile exact durable/native outcome and
independently retire the admitted worker before recovery or secret mounts.
When uncertainty prevents reconciliation, preserve source, target, WAL and
journals and use the separately admitted terminal/recovery owner. Ordinary
reentry never invents permission to redispatch, reclaim or delete evidence.

Validation tools and fixtures belong under scripts/ci and the test topology.
Their flags are not alternative operator commands. Use the normal validation
matrix and release workflow for the exact source. Repeated qualification caveats
below are intentionally expressed once here; the per-operation sections retain
the distinct schemas, clocks, locks, refusal cases and recovery actions.

## Storage capacity and workload admission

[ADR 0078](../decisions/0078-evolve-research-within-existing-data-boundaries.md)
records the migration-scope decision. Current procedures and limits belong to
the storage and release boundaries; dated qualification records are evidence
for their recorded source and workload, not instructions to replay operations.
Migration design and qualification sections below retain historical context;
their older status statements do not establish the current deployment state.

Before admitting affected storage work, account for each physical disk's current
occupancy once and the additional allocations that coexist: source/target and
rollback copies, indexes, identity/catalog growth, spool, WAL, temporary files,
archives, recovery chains and replacement overlap. Overlapping policy reserves
and operation floors are not additive: use the larger applicable requirement
after accounting for demands and competing claims. Do not credit predicted
reclamation before verified publication and physical observation.

Capacity claims require a measured growth horizon and sustained verification and
reclamation under continuing intake. Copy counts, healthy-service snapshots and
small correctness fixtures do not establish sustainable capacity or a speedup.
Do not remove indexes or retained recovery/source material based on size alone.

The selected 14-day recent-data placement policy is not a research lookback or
data-expiration limit. Affected workflows must qualify uncached historical reads,
including initialization, restart, gap catch-up and warmup beyond that window,
against their existing latency/resource limits and competing collection I/O.
Observe-only paper support does not establish live readiness. Passing storage
checks does not start research or clear an operational hold.

## Bounded lifecycle observation

`GET /api/storage` includes `qt.storage_retention_observability.v1`. The
projection is read-only and metadata-only: it projects at most 2,048 retention
partition rows after reading at most 2,049, and projects at most 10,000 archive
manifest rows after reading at most 10,001. The manifest page is bounded before
verification expansion. Verification versions for those selected manifests use
their primary-key scope plus the five-second statement timeout as the backstop;
they do not have a separate per-manifest row cap. A 250 ms lock timeout also
applies. The projection does not call the retention planner, read Fact payloads,
open archive objects, hydrate history or acquire lifecycle ownership. A failed
or timed-out projection becomes `unknown`; storage settings remain available.

The projection identifies the sampled runtime source, saved policy revision and
fingerprint, default effective hot window, scan bounds, completeness and any
truncation. The maintenance runtime's saved-policy adapter replaces the base hot
window and clears base fact-family overrides; the projection reports both that
effective empty override map and the inactive configured base values. An eligible
open partition has no sealed source-byte estimate, so its presence makes the
result `partial`.

Physical hot source bytes, compressed archive bytes, estimated rows and
cumulative reclaimed bytes are separate measures. The 6-hour, 24-hour and
7-day windows also keep four event domains separate: source bytes first measured
at partition sealing (an intake estimate), compressed bytes acknowledged to the
archive, compressed bytes verified and physical hot bytes reclaimed. A single
sample cannot establish filesystem deltas or permanent metadata growth. The API
therefore reports those fields as unavailable, and it does not provide an ETA
until aligned physical intake and reclaim samples demonstrate positive net
drain.

The maintenance supervisor reports the current phase, its start and boundary
progress, plus completed phase durations. Canonical progress is observed only at
the existing plan/action/result boundaries. Observer failure is caught and
cannot change retention, recovery, scheduling, admission or retry behavior.
Last durable advancement in the API is reconstructed from committed partition,
manifest and verification timestamps rather than from process-local callbacks.

The Storage room presents this bounded evidence with the explanation beside the
numbers. The Capacity and Database Growth Grafana dashboard retains four named
panels as explicit unavailable placeholders. Its PostgreSQL datasource cannot
establish the required statement and lock deadlines before a metadata statement,
so those panels do not query the retention catalog. Operators use the bounded
Storage API/UI projection for the pipeline domains, oldest days and saved default
eligibility window whose runtime adapter clears family overrides. Neither surface
grants execution authority.

## Canonical archive stage attribution

Each canonical archive stage records its existing phase duration and catalog
batch counts. Raw-lineage lookups additionally record the lookup mode, exact
number of distinct requested keys, witness count, optional bound-manifest
cardinality, result cardinality, elapsed time and outcome. They never log record
identities, bound values, payloads or object paths.

One small PostgreSQL statistics snapshot is read after page admission and a
second immediately before commit. The paired receipt names the archive
backend PID and PostgreSQL version so an operator can correlate a separately
bounded `pg_stat_activity` wait sample. It reports changes in PostgreSQL 15's
cluster-wide WAL and background-writer counters as `scope=cluster_overlap`.
Those changes include other concurrent database activity and are not
page-exclusive WAL, statement cost or device attribution. A statistics reset or
rolled-back page reports the delta as unavailable. `track_io_timing` is recorded,
but PostgreSQL 15 does not expose `pg_stat_io`; this instrumentation therefore
does not claim a relation, index or physical device caused a wait. The two
catalog-view reads run inside a SQL savepoint, do not scan application rows and
fail open as unavailable observations; they cannot alter archive admission,
timeouts, atomicity or recovery behavior.

## Canonical query diagnostic

`scripts/reporting/canonical_storage_query_diagnostic.py` is an inert,
server-only operator command for a closed inventory of canonical retention SQL
builders. It is not an API, MCP tool, scheduler task or maintenance trigger. A
run names one already verified UTC storage day and writes one private JSON
receipt:

```bash
python scripts/reporting/canonical_storage_query_diagnostic.py \
  --storage-day 2026-09-02 \
  --receipt /private/operator-evidence/canonical-query-diagnostic.json
```

The command refuses an active lifecycle owner, a non-verified partition, a
missing committed page, over-limit evidence or an unattested image.
It opens an isolated connection whose server-side default is read-only before
the first statement, so it never enters the application's schema-bootstrap
session path. Every transaction is repeatable-read and read-only. Statement and
lock bounds remain 5 seconds and 250 milliseconds. The admitted work budget is
60 seconds, including scope selection: connection and pool waits are clamped to
one second, cases retain cleanup time, and an elapsed overrun can never produce
a passing receipt.
Representative read-only cases may use `EXPLAIN ANALYZE`; maximum supported
shapes are plan-only unless explicitly admitted. The receipt contains SQL
fingerprints and summarized plan, buffer, WAL and sampled wait evidence without
retaining bound record values or arbitrary query text.

Coverage is explicit. The inventory includes retention admission, archive page
selection, exact record-ID and receive-position lineage, bounded verification
catalogs, dependency expiry, relation identity/size and frozen-Dataset pin
admission. Production catalog writes, file decoding, the exclusive handoff and
the partition drop are reported as untested or unsupported in live mode. Their
write, duplicate/FK, timeout, cancellation and full-rollback contracts remain
in the ordinary disposable PostgreSQL CI suite. A diagnostic success is neither
maintenance authority nor a worst-case cache guarantee.

## Forward-only transition

The forward operator retains an existing header heap as one sealed range below
the canonical dated parent, then writes new days to daily partitions. The
[canonical forward operation](#canonical-forward-operation-and-utc-cutover-window)
owns its publication, adoption, final proof and UTC window. Retained history on
SSD is an intermediate placement; it does not satisfy the historical-capacity
or matching-recovery goal.

The same STABLE range reader includes the sealed legacy relation. Clean layouts
use an empty binding catalog; the full-copy handoff installs that empty catalog
without changing saved copy fingerprints. Missing or changed retained history
refuses admission. Physical inventory includes heap, TOAST and ordinary indexes.
The constant-boundary helper avoids locking the legacy heap for new-day
provisioning; reads of retained history still take its read lock. Inventory alone
never grants attachment, movement or final-switch authority.

## One online migration workflow and its owners

The release workflow is: prepare the fixed SSD/HDD destination, copy and catch up
while the original source serves, admit a short final pause, switch once, then
activate the matching runtime and publish a complete encrypted recovery pair.
This is the workflow required by [ADR 0073](../decisions/0073-prepare-storage-migrations-with-live-collection.md).
Runtime readiness is an intermediate result. Internal phase helpers are not
alternative release commands; complete recovery and deployment publication
remain separately admitted outcomes.
The historical operator that holds clients throughout copying is not this
release's production path.

Each boundary owns a different kind of truth:

| Boundary | Owns | Cannot establish by itself |
| --- | --- | --- |
| `storage_host_boundary` | Bounded Docker I/O and observations, fixed service inventory, deployment flock, private durable receipt I/O. | Receipt meaning, publisher exclusion, phase completion or permission to resume/switch. |
| `storage_online_prepare` | Initial preparation receipt, original 600-second window, original source identity and preserving initial resumption. | Final switch or a later recovery-mount transition. It still reuses the existing database preparation procedure. |
| `storage_online_launch` | Exact migration-worker configuration, launcher lock lifetime, verified worker retirement and original capture binding. | Completion of a SQL switch or permission to expose recovery keys to a live read-capability worker. |
| `storage_online_final` | Final wall/boot window, durable switch intent, source-stop/start journal and same-worker host coordination. | SQL commit truth from an exit code, stale receipt, or observed empty queue. |
| `storage_online_recovery` | Fixed preserving recreation of the already HDD-mounted database after committed reconciliation and verified reader retirement, journaled in the existing final receipt. | Repository readiness, restored access, application startup or a complete encrypted pair. |
| `storage_online_controller` and bounded copy/proof components | Live controller ownership, current proof, original attempt/command deadlines and bounded preparation progress. | Host publisher exclusion or runtime activation. Process-local proof cannot be restored from a saved status reply. |
| `fact_header_v2_handoff` | Verified SQL transaction and authoritative outcome inspection; separately admitted policy/runtime checks. | A safe host pause or source restart. The host final-state owner supplies the held COMMIT transition. |
| Recovery preparation and maintenance | Repository identity, native WAL delivery and publication/retention of complete encrypted database/archive pairs. | Permission to add secret mounts before exact committed reconciliation and verified read-worker retirement. |

The shared host boundary has no CLI or migration state machine. Both historical
and online phase modules call its named functions directly; there are no parallel
copies or private compatibility forwarding functions in the historical module.
Its single nested Docker deadline can only shorten the caller's existing window.
Receipt serialization, file permissions, error codes, service admission and
locking retain their prior behavior. Schema validation and transition decisions
stay with the phase that owns each receipt.

The single operation connects these owners, including publisher exclusion and
the post-switch recovery transition. Remaining release work must qualify terminal
release, actual source/fleet admission and production operating impact.
It must not accumulate another set of competing receipt meanings or public phase
commands. An uncertain switch requires fresh authoritative inspection; an
in-flight or partially completed Docker start remains unresolved and cannot be
replayed from a saved negative SQL result. The current final marker continues to
block ordinary deployment/recovery entry until an explicitly qualified terminal
transition exists. No refactor grants that missing authority.

## Archive names during the final switch

Every local object-store publication and deletion takes a shared, nonblocking
Linux flock on the archive-root directory inode. Ordinary publishers remain
concurrent. After its last copy page, the existing online controller takes the
exclusive side before internal COMMIT and retains it through uncertain-outcome
inspection until controller retirement. A late cooperating store refuses before
creating directories, publishing a name or deleting a file; reads remain available.
There is no lock file, source permission change, new deadline or saved lock token.
Root identity checks reject replacement. The existing live file leases still
protect file contents and exact verification still checks required names.

This is a cooperating-store boundary. Legacy images, arbitrary filesystem tools,
subdirectory roots and processes bypassing this object store are not restrained
by flock. Exact image/mount/publisher admission and host lifecycle exclusion remain
required before the host may dispatch a switch. Kernel release on process death
is not restart or recovery authority. The original final marker and unresolved
outcome rules continue to block automatic activation.

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
The preparatory release and complete publisher/operator admission must be
qualified together before use. A saved observation is never lock authority.
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

The planner handles at most 4,096 groups (including the optional legacy range)
and 32 registered targets;
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

The optional fixed legacy group covers `MINVALUE` through its exclusive
`legacy_end_day`. Its journal `storage_day` is the latest possible day in that
range, never a fabricated daily partition bound. Thus the whole group remains
recent until that day is strictly outside the recent window. Legacy and daily
ranges cannot overlap. Plans, saved source intent and physical completion
receipts bind the exclusive end day; dropping or changing it refuses execution.
Daily plans omit the optional field, preserving existing hashes and receipts.
No released source bytes receive advance or duplicate capacity credit.

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
relations, schema, persistence and exact daily bounds. The one fixed legacy
child instead requires the immutable OID/range binding, native index and foreign
key integrity and mutation seals used by startup. All checks use the catalogue's
existing decreasing SQL budget. A selected-group observation locks only that
child, so a daily move does not wait for an unrelated legacy heap lock. It observes table,
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
snapshot to registered targets through read-only file probes. The physical worker
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
physical worker requires an outer operation timeout as well as retry/recovery
controls. None of these checks reserve capacity or prevent later file growth,
so a movement worker must obtain fresh evidence before executing.

## Database records and cutover work

portal_storage_targets, portal_storage_policy, portal_storage_plans, and
portal_storage_object_locations are clean-schema ORM models under the existing
Base and PG_DSN. The current bootstrap can create their missing clean tables.
A deployment cutover still needs explicit schema validation, grants, host
mounts, and a worker. Before the initial service pause, the online operator
requires all seven canonical storage-control tables to exist. A missing table
fails with its name; migration never bootstraps or repairs the schema. The
operator also checks the prepared archive destination as UID70 with a read-only
HDD bind, no network or capabilities, using the existing archive group contract.
The destination device, inode, owner, group and mode join the configuration
binding. This detects an unprepared private directory before the worker starts;
it does not change existing source or destination permissions.

These models do not alter market.fact_versions, its global
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

The clean-schema dated-header foundation is described in
[ADR 0070](../decisions/0070-separate-global-fact-identity-from-dated-headers.md).
It preserves global identity while allowing detail partitions to move later.
It does not enable Apply or prove physical placement; existing v1 installations
require the explicit preserving or forward cutover.

The identity-registration guard uses an explicit Fact-ID equality lookup before
checking the full immutable projection. A row-wise null-safe comparison alone
can scan the whole registry on each insertion; moving its indexes to SSD does
not repair that query shape. The [layout notes](../../engineering/fact-header-layout-v2.md)
describe the installed-function plan regression and explicit upgrade requirement.

For newly bound private migration copies, `CopyPlacement.recent_lookup_indexes`
keeps only the identity ID, identity/day and raw-mapping primary-key indexes on
the verified recent SSD. The existing copy owner still places their heaps,
TOAST and other indexes on HDD and verifies every member. The absent/default
choice preserves the original all-history receipts. This placement option does not
move already retained copies, reopen retired adoption, activate runtime or
change the recent-data policy. Its durable SSD cost and recovery implications
are described in the [placement preview](#historical-header-placement-preview).

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
migration. The journal is internal. Registration, locked pre-execution checks, physical DDL,
crash reconciliation and resource coverage belong to the operator/movement
owners below. A reservation never activates policy or moves bytes; existing
installations require explicit reviewed schema preparation.


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
boundaries below accept it. The executing worker must repeat the checks while
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
and remains an internal boundary composed by the admitted operator.

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
the default remains one million. Size the bound from the actual retained object
inventory and growth rate; a default ceiling does not establish sufficient
capacity for a requested retention horizon.
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

Recovery qualification must include new observations, corrections and raw
archive publication after a preserving layout change. Restore the
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

The frozen-Dataset pin audit has its own bounded PostgreSQL statement timeout,
`pinned_ranges_statement_timeout_ms`, expressed in milliseconds. Its default is
1,000 ms and the environment override is
`QT_MARKET_DATA_LIFECYCLE_CANONICAL_PINNED_RANGES_STATEMENT_TIMEOUT_MS`. The
effective value is the minimum of that setting, the general canonical execution
statement timeout and the remaining maintenance-run budget. Plans expose the
configured policy value; reclamation results and structured start/finish logs
expose the effective value. The audit materializes exact Dataset binding envelopes,
deduplicates equal search envelopes, and stops each indexed series/time seek at
the first header satisfying the original range and frozen-commit bounds. The
final count still preserves every distinct Dataset binding. Changing this timeout
does not weaken pin checks or expand the handoff/run budget.

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

### Disposable SSD history cache admission

The canonical reader may retain immutable HDD objects in a bounded SSD cache.
The data boundary documents its copy/eviction contract. `history_policy.py`
composes it with existing saved targets, reserves and storage-management locking;
there is no new database schema or independent capacity authority. A cache fill
is optional and cannot proceed through a conflicting maintenance transaction.
The fill holds that lock for at most its configured 30-second cooperative work
budget and checks actual free space/ownership during copying. Ordinary cache
hits keep only an active-file lock. OS I/O stalls remain subject to host-level
qualification, not a hard real-time guarantee from Python checkpoints.

Cache admission reads the registered SSD target for identity and capacity; that
target can be the backend's read-only PostgreSQL bind. The actual cache working
directory must be writable and on the same UUID and device. An unavailable
target or working mount bypasses this optional cache and keeps the authoritative
archive reader available. No PostgreSQL directory permissions are expanded.

The fixed server overlay enables global research serialization and one research
worker. It leaves cache size zero until capacity qualification supplies an
explicit cache byte budget, minimum-free floor and private runtime-owned
`history-read-cache` directory beneath the existing SSD working root. Count
cache occupancy and in-progress fills in SSD space; do not grant recovery or
migration reclamation credit for copies that have not actually been evicted.
The 14-day recent-data policy does not authorize deleting unarchived collection.

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

## Recovery adapters and encrypted release requirements

Encrypted database-plus-archive recovery is a release requirement for the
preserving storage composition. That requirement is separate from which adapter
an admitted configuration actually constructs:

| Configuration | Adapter | What completion establishes |
| --- | --- | --- |
| Legacy v1 maintenance limits without incremental settings | LocalRecoveryCopies | Bounded logical dump plus referenced archive-object generation; not encrypted recovery |
| Explicit v2 incremental settings | EncryptedRecoveryCopies | Paired physical backup and encrypted archive snapshot under the pinned repository/key contract |

The selection is explicit in
portal/backend/service/storage/recovery_maintenance.py. The legacy adapter
remains for compatibility and retained generations; its success must not satisfy
an encrypted-pair release gate. Changing configuration does not provision keys,
enable PostgreSQL WAL archiving or qualify restore. See
[ADR 0072](../decisions/0072-use-encrypted-incremental-recovery-sets.md).
Both local adapters share the HDD failure domain; neither supplies off-server
protection by itself.

### Encrypted recovery runtime and admission

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

Restic runs with JSON output and quiet progress: native summaries and errors
remain available, while periodic status messages cannot consume the bounded
subprocess output buffer during long backups. Ordinary result and error output
retain their 8 MiB caps; an overflow identifies the tool and stream without
exposing contents. Nonzero native exits still fail the pair.

Native snapshot listings include every original file path. The same recovery
owner uses pinned `ijson` parsing to consume that listing incrementally and keep
only complete snapshot IDs, hosts and tags, still capped at 8 MiB of metadata.
This avoids both retaining an archive-sized JSON response and inventing a JSON
parser. Truncated, ambiguous or malformed metadata refuses retirement; paths
remain in the native snapshot and encrypted inventory. Cancellation, deadline
and process-group cleanup apply while streaming. This preserves bounded allocation, cancellation and child cleanup without
changing backup formats or the paired-publication requirement.

Production images contain pinned tools. Image availability does not enable WAL
archiving, provision keys, qualify a restore or activate a backup policy. Those
operator steps and full QT application recovery require separate exact-source
and installation qualification under ADR0072.

The encrypted recovery deployment uses the fixed storage overlay's private SSD
recovery-key bind (PostgreSQL and collector only) and shared database socket.
It does not activate archiving implicitly. Native WAL expiry retains consistency
logs for all saved points while bounding obsolete between-backup history;
arbitrary later PITR is outside the paired archive contract. The existing
maintenance failure surface reports disabled or failing PostgreSQL archiving
even when the next backup is not due.

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
still hashes all files. These proof components alone do not perform publisher
drain, terminal cancellation or runtime activation; use the connected operator
described below. Its exact release still needs measured archive reconciliation,
final-pause, live-ingestion, capture-backlog and collector-impact qualification.
The historical held host operator is not a short-outage guarantee.

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
read-only receipt inspection. The bound cancellation form takes an exact JSON
capture preimage plus the host's durable intent SHA256 and records a v2 terminal
receipt; the original internal call retains its v1 receipt. Both execution and
inspection also exclude a live controller's session lock, including its idle
periods. `inspect_cancellation` compares the intent, untouched capture, actual
source/database/queue identity, original triggers and native references. It
allows newly provisioned payload days only after validating their references.
Missing or foreign receipts cannot authorize redispatch. Read-only inspection
neither renews a capture nor removes data or temporary dependencies. Cancellation is not a host abort or runtime
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


### Persistent online controller and private command channel

The internal storage_online_controller.OnlineController owns one live destination
file proof and a dedicated PostgreSQL session lock for an already prepared,
protected attempt. It binds the original capture record, fixed placement and
archive roots. Loss of the owning connection never transparently reconnects.
Existing page transactions retain their own migration/resource locks; an
in-flight bounded page may commit before ownership loss is observed, and its
durable progress is re-admitted on restart. Closing the context closes every
proof descriptor. Healthy owner cleanup rolls back any unfinished transaction
and explicitly acknowledges release of its controller lock before invalidating
the dedicated session. The unlock has a five-second statement ceiling; failure
still discards the connection and is surfaced. A lost or invalidated session
never reconnects for cleanup. No session-level lock returns to a pool, and a
fresh controller does not depend on asynchronous disconnect processing.

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
recovery activation are owned by the canonical operation. Original attempt
clocks and file/resource limits apply throughout.

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
enforces this interlock. The launcher accepts prepared attempts or the explicitly
bound initial capture described below;
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

### Atomic online capture preparation

fact_header_v2_online.prepare_attempt combines the existing header/raw shadow,
exact SQL protection and archive capture installation in one resource-watched
transaction. Fixed roots/placement/recent-window policy are admitted before DDL;
raw preparation precedes archive trigger binding. NOWAIT writer fences and a
maximum 60-second transaction bound limit this preparation. Failure rolls back
all newly installed capture/proof state; retries retain the original attempt
clock and reject changed bindings or an unprotected populated shadow.

This internal API does not stop clients, move bulk data or grant final-switch
authority. The host must already have admitted the source runtime, mounts and
durable intent. Retained-table movement stays explicit before bulk copying.
The separately committed identity/raw/reference operations use the explicit
steps below. Complete production host-to-recovery orchestration remains required.


### Initial preparation through background worker admission

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
restarted by these commands. The same worker also delegates `catalog_history` to the existing fixed catalog
mover. Only the retained rollback table and the two archive reference catalogs
are admitted; arbitrary relations remain refused. These commands grant no final
switch or runtime authority.

Disposable controller qualification traverses the finite header/raw phases,
restarts after committed identity relocation, publishes additional QT archives,
then prepares/validates/adopts references and catches archive intake. It checks
original capture/frozen preservation and refusal of excess phase duration.
This component qualification is not a complete packaged host phase driver,
production lock-impact measurement or final outage admission.


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
runtime/recovery activation. Host expiry or uncertainty remains held.


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

### Collector finalization qualification

Archive publication can precede canonical acknowledgement. A blocked
acknowledgement keeps shutdown pending and sealed WAL present; failure must
propagate while preserving the segment. Normal startup recovers it before a new
transport session. Unrelated pending WAL and capture/frozen bindings remain.
A completed runtime or momentarily empty spool does not authorize switching.

Supervisor shutdown preserves finalizer exceptions/cancellation, drain timeout
and unresolved failure during restart backoff. stop() raises for a failed thread
or drain. A replacement collector that drains successfully can clear its earlier
failure. A legacy source image may predate these semantics: exit zero is not
publisher-drain proof. Preserve spool and admit the exact source image independently.

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
recovery mount activation. Partial starts, outer-process loss and uncertain Docker completion remain
held until the separately admitted reconciliation owner establishes outcome.


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
shortcut. This is a bounded resumption reconciliation, not COMMIT dispatch,
recovery activation or deployment authority.


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


Final online source admission also checks bounded Docker-wide mount/state
snapshots. An unadmitted live peer with a writable mount overlapping database or
collector storage refuses before further host work; no unexpected peer is stopped
or edited. Read-only/stopped peers remain preserved. The original deadline bounds
the observation and drift refuses. This supplements strict project/network
inventory; it is not host-process, future-start, SQL-publisher or filesystem-alias
exclusion and does not create COMMIT authority.


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


The internal `OnlineController.final_database_session` retains one database
connection for the already bounded final window. After background/tail work,
commit, fresh outcome inspection and direct rollback admission can use that same
session without opening a new login. Its backend identity is fixed; closure,
invalidation, identity drift or an unexpected transaction refuses replacement.
Context exit discards the connection, and the controller cannot reenter this
session or return to baseline work. Confirmed job retirement permits only bounded
residual tails on this same session. The original final and capture deadlines remain.

This retained session alone is not login-gate authority. It changes no database login setting, host receipt, pipe command
or restart authority. Durable host gating, existing/background publisher
admission and post-switch recovery still require qualified integration.

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


### Host-owned final login gate

`storage_online_final.close_database_logins_locked` now owns the internal gate
transition in the existing final receipt. It requires switch intent, exact held
source and the same live worker. The worker opens its retained final session
through ordered `final_session_begin` and fresh `final_session_check` operations;
these report identity/capture and deny switch, restart and runtime authority.
No new operator entrypoint or COMMIT command is exposed.

The host compares worker and maintenance-session cluster/database identity,
records `login_closing` with the original open-login setting BEFORE dispatch,
and closes only that target database's new logins. It records `login_closed`
only after the same worker and source are freshly admitted through the closed
gate and the worker confirms the bounded database-job stop below. SQL control uses the existing database recipe via the cluster-local
maintenance database; there is no second application DSN. Original capture,
initial preparation and final wall/boot/monotonic deadlines are unchanged.

An interrupted request can already have closed access. Both gate phases block
ordinary stop/restart/relaunch and gate replay, retain the marker and grant no
COMMIT or automatic reopen. Confirmed `login_closed` has the separately guarded
internal abort path described below; `login_closing` remains unresolved. Prepared transactions still refuse the switch;
archive/spool publishers and terminal recovery still require admission.
Gate restoration requires the admitted retained-session abort/recovery owner;
worker teardown alone never grants access restoration.

The same host intent now covers `final_session_quiesce`: the retained worker calls
Timescale's existing stop-background-workers function after proving the target
login gate is closed. This operation is pinned to Timescale 2.14.2, plpgsql 1.0,
optional pgcrypto 1.3 / pg_buffercache 1.3 / pg_stat_statements 1.10, and Timescale with optional
pg_stat_statements preload. Other extensions/preloads, standby mode, subscriptions,
replication slots or connected replicas refuse. This narrow admitted environment
is not a configurable job framework or a general extension compatibility claim.

The request is marked in memory before the nontransactional stop; it cannot be
replayed. Under the same original final deadline, the worker waits for every
other target backend to leave, excluding only its actual retained session and
live owner. It preserves a bounded definition fingerprint (at most 256 jobs /
1 MiB of catalog metadata); job configuration stays inside PostgreSQL. It changes
no job definitions, schedules, enabled flags or restoring settings. Interrupted
jobs can retain already committed work and enter Timescale's normal crash retry
backoff. No automatic job restart is provided by this transition.

`login_closed` now explicitly records `database_jobs_stopped=true`; uncertainty
retains `login_closing` with false. Older receipts lacking this field refuse
reentry and remain evidence. Subsequent internal COMMIT checks require the gate
and pinned environment to remain admitted, definitions unchanged and ALL other
target backends absent, as well as the existing prepared-transaction check. No
scheduler name or application name is accepted as an exemption. These controls
do not establish host/archive/spool exclusion or expose a COMMIT wire command.
A qualified preserving job/gate restoration is still required before production.

The supported operator admits only the two built-in jobs observed on the serving
cluster: Timescale 2.14.2 telemetry (job 1) and job-error retention (job 2).
Before serving commands and again before entering the retained final session,
the existing controller checks their exact enabled schedules, retry limits,
configuration, check function and lack of hypertable association. It also pins
the retention/check SQL bodies and execution attributes to the qualified public
installation SQL; names or extension version alone do not establish identity.
Unknown/custom jobs, NULL retention configuration and modified functions refuse
before host login closure. The worker
also encodes its private archive override as YAML `null`, matching the existing
central settings contract; an empty or malformed group remains invalid. Configuration remains inside PostgreSQL.

The same worker retains the complete job-definition fingerprint and rechecks it
before stopping jobs, on fresh final-session observations and before internal
COMMIT. The host requires the live admission result before changing access;
a saved result grants no replay or switch authority. This deliberately avoids
arbitrary custom-job interruption/replay support. The built-ins perform telemetry
and job-error cleanup rather than QT fact publication, but their normal retry
behavior still does not promise exactly-once execution. Source-image and archive/spool publisher exclusion remain separate final-switch
requirements.

Jobs can publish changes between the last tail pass and their retirement. The
existing `copy_final_delta_locked` now also accepts confirmed `login_closed` and
keeps its exact original switch deadline. Fresh same-worker session observations
admit the closed database identity and original capture before and after each
bounded round; source bindings, stopped clients and receipt clocks are rechecked.
The final receipt remains unchanged and the report grants no switch authority.

On this route, `OnlineController.final_delta` requires confirmed job retirement,
checks the closed gate, pinned environment, job definitions and absence of other
SQL publishers, and reuses the retained backend for existing header/raw/archive
page transactions. Each completed page commits separately with the same resource
watch, cancellation, file proof and original capture deadline. Caller-owned page
connections must be idle, live and belong to the same engine; page helpers never
close or replace them. An interrupted page rolls back its SQL progress while
preserving earlier committed pages and reusable verified files. A failed round
terminalizes this controller; it is not permission to reconnect or resume it.

Background bulk work remains forbidden in this window. Empty tail observations
still do not authorize COMMIT; exact final verification and host/archive/spool
publisher admission remain mandatory. Actual scheduled QT job semantics,
restoration after uncertain/outer loss and the complete host switch require
separate connected qualification.


### Returning to the original source after confirmed gating

The existing `resume_online_source_locked` also accepts confirmed `login_closed`.
It freshly admits the same retained worker/session, then acquires the original
negative-outcome migration fence and source relation locks. A committed outcome,
missing job-stop confirmation, changed definitions/environment or lost session
refuses. The fence stays live through access restoration and every source start;
its observations preserve original capture/database identity. No saved negative
outcome or stopped-client observation supplies this authority.

The existing `source_resuming` journal retains `login_gate` evidence and records
`resume.gate_restore` with ordered `logins` then `jobs` actions. Each is persisted
as in flight before dispatch. The first restores the exact original open-login
setting through the existing cluster maintenance path. The second requests the
pinned Timescale background workers to restart using a separate, bounded target
transaction; the migration fence remains on the retained worker connection.
Definitions, schedules and enabled flags are unchanged. Acceptance of this
request does not prove every job has run or provide exactly-once job semantics.

The shared host boundary supervises these bounded Docker/SQL requests using the
same loop already used for exact source starts, with bounded SQL input and
original deadlines. It checks live ownership while the local CLI is pending.
Killing/reaping that CLI does not cancel a daemon or SQL operation. A lost reply,
identity/ownership loss or expiry leaves the action unresolved and prevents
further actions. Access or a background job may already be active; uncertainty
must never be described as all writers stopped. No automatic retry or reversal.

Only after both restoration actions complete does the existing procedure start
originally running source clients, admit their exact healthy state and end the
fence. It records `source_resumed`, retaining the final marker and gate history.
The completed initializer stays stopped. Ordinary deployment/relaunch/recovery
still refuses that marker. This is an internal preserving abort, not candidate
runtime activation, successful migration or recovery-mount authority.

Disposable qualification covers this full controlled-source abort and a fully
received restoration reply discarded after actual access reopening. Outer host
loss, unread framing, late SQL completion and production job/source/spool
admission remain separate limits. The original preparation, capture and final
windows are never renewed. This abort component does not qualify the connected
operator for a production source or workload.

## Application and database file ownership

The candidate application runs as UID1000, while PostgreSQL and physical
maintenance run as UID70. Source ownership must be observed independently:
a source may contain root-owned0600 pending WAL beneath a UID1000 working
root. A UID1000-only recovery fixture cannot qualify access to root-private
files. Preserve original source bytes and
metadata; a separately bounded preserving recovery-copy transition is required
before candidate activation. Do not infer child ownership from the mount root. The online reader can copy those archives without changing the
source, but its UID70 private copies are not readable by UID1000. The former
all-UID70 runtime recipe therefore cannot be reused for the preserving online
transition. Ordinary application processes must not gain root or DAC_READ_SEARCH
as a workaround, and PostgreSQL's private ownership guards remain unchanged.

The existing immutable archive object store now supports an explicit
`QT_ARCHIVE_SHARED_GROUP_ID` setting (positive numeric group, default unset).
When set, it requires membership in that group and an already prepared objects
root with that group and mode2770. New object directories inherit that group and
are2770; new object inodes become0640 before atomic publication. Shared members
can publish/retire names in the directories but cannot edit another owner's
object inode through group write. There is no world access. This is a trusted
application/maintenance sharing group, not isolation between its members.

Only newly created paths are prepared. Existing incompatible roots, directories,
objects or symlink aliases refuse; runtime never broadens their permissions or
changes their owners. An interrupted directory preparation can require explicit
operator reconciliation; retry does not repair an existing private directory.
With the setting absent, publication remains private0600. The setting does not
change the spool, temporary encoders, PGDATA, keys, repositories or backup files.

The publication seam and filesystem ownership must be qualified with the
source and target identities actually admitted for the installation. The
composition hosts the existing maintenance supervisor at the database-owned
process boundary, retaining one lifecycle scheduler, status and recovery engine.
The public overlay expresses this composition; it does not authorize activation. Actual source ownership admission, preserving recovery-copy
preparation, host switch, read-worker retirement and post-switch activation still
need integration and measured final-pause qualification. The legacy runtime
validator continues to refuse the new topology. Existing prepared production directories are not
silently converted to the new group contract.


### Database-owned maintenance composition

`storage.maintenance_owner` selects exactly one composition: `collector` remains
its compatibility default; `dedicated` makes the collector omit construction of
the lifecycle supervisor. The internal `portal.backend.workers.storage_maintenance`
process then hosts that same supervisor and its existing history/recovery runners.
It requires the pinned PostgreSQL OS user70, an explicit shared archive group,
an already prepared archive root, and encrypted incremental maintenance limits.
It has no acquisition adapters or provider intake, alternate policy, scheduling
loop for individual phases, or additional application DSN. Existing phase
cancellation, storage fences, retention ordering and paired recovery stay with
the existing supervisor/runners. A missing dedicated process is unavailable
maintenance; the collector does not silently take it over.

The worker uses the existing worker-state repository with role
`market_storage_maintenance`. Its heartbeat carries the actual supervisor snapshot;
Storage status reads that role and the compatibility collector role through the
same freshness, policy and outcome checks. Two live configured owners remain
ambiguous; expired or failed work cannot appear healthy. Collection health
excludes maintenance-role rows so a live maintenance process cannot impersonate
a collector. Shutdown joins the existing supervisor; failed start, heartbeat,
drain or retirement exits nonzero. No worker table, status engine or user-facing
operator is added.

Normal Storage status reads database evidence and filesystem capacity; it does
not require backend access to PostgreSQL private files. Physical inspection stays
inside the database-owned maintenance boundary. Its existing temporary archive
work is on the configured archive root; the private application SSD spool is not
a maintenance input. Application access to legacy private SSD files is preserved.

This is an opt-in process seam, not an activated server recipe. The fixed overlay
and bot reader now express it as described below; the old runtime/mount validator
still refuses that topology. Installing and admitting one dedicated owner, exact
legacy-WAL recovery and fresh intake, physical maintenance and a complete encrypted
recovery pair remain integration requirements.


### Preserving runtime recipe

The existing fixed storage overlay now selects UID1000:1000 for backend,
initializer and collection, preserving the deployed private SSD spool owner.
Each joins the explicitly configured archive group. The dedicated maintenance
service alone shares the PostgreSQL PID namespace and receives recovery-key,
socket and limits mounts; it runs as UID70 and has no collector-spool mount.
Its temporary archive work stays on the HDD. All four processes drop Linux
capabilities and set no-new-privileges; backend retains its existing Docker socket
role. Image-local logs/reports are prepared for the two accounts at build time;
no host files are repaired, chowned or widened by startup.

Backend retains the registered SSD/PGDATA mount for filesystem capacity reporting.
Its UID1000 cannot traverse private UID70 PostgreSQL directories. This is capacity
observation, not physical inspection; PG_VERSION remains inaccessible to that UID. The initializer and collector do not mount PostgreSQL data.
Bot containers receive the same explicit archive-group setting, run as UID1000
with that supplemental group, drop capabilities, and retain only the existing
read-only archive/udev mounts. Unconfigured bot launches keep their prior behavior.

The existing bounded read-only heartbeat probe also recognizes maintenance's
host-scoped worker prefix and running/degraded lifecycle. It does not bootstrap
schema or turn liveness into proof of a completed backup. Storage continues to
show actual phase outcomes through its existing projection.


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


### Preparing a private recovery copy of retained spool data

The existing online spool boundary can prepare a new private SSD working root
from a separately held, read-only original. It reuses the same bounded metadata
walk, copies only pending `.open`/`.sealed` WAL, verifies each copy, and assigns
only newly created destination paths to the candidate UID1000. Original files,
permissions and acknowledgement projections remain untouched. An old local
acknowledgement is not a database certificate and is not copied as authority.
The normal QT runtime must recover and acknowledge the copied WAL through the
existing database/archive path before retiring its copy.

This is an internal key-free filesystem preparation step, not a second replay
engine or an operator entrypoint. It requires an empty prepared destination on
the source device, explicit byte/entry bounds, an original caller deadline and
continuous caller-owned writer exclusion. Unknown pending files, source changes,
symlinks, deadline exhaustion and reused destinations refuse. Failed partial
copies are retained and cannot be silently adopted or repaired. A returned copy
report never grants switch, rollback, recovery-mount or runtime activation
authority; the final-state owner must compose those transitions.


### Preserving recovery mounts after the committed switch

The final-state owner exposes an internal recovery transition only inside the
same live source hold that admitted COMMIT. It requires a freshly confirmed
committed outcome and independently rechecks the exact stopped migration reader,
its original start identity and reaped attach process before exposing recovery
mounts. A persisted committed receipt alone cannot enter the transition.

The fixed recreation code is in `storage_online_recovery`. It derives a separate
private recipe from the unchanged initial database recipe, adding only the existing
private recovery-key directory and an independently prepared local socket volume.
The database image, environment, command, network, original PGDATA and existing HDD
mount remain bound. Keys, source files and existing volumes are neither generated,
removed nor changed. The initial recipe and its original 600-second receipt remain
unchanged; the separately bounded phase can only shorten the original final
wall/boot/monotonic and capture deadlines.

The existing final receipt records `recovery_preparing`, ordered stop/remove/create/
start intent before each daemon request, and `recovery_database_ready` only after
fresh original-cluster, closed-login and absent-target-backend/prepared-transaction
checks. Only a cleanly stopped original container may be removed, without volumes.
The same source hold and host deployment lock stay live. A lost request/reply,
changed binding or expired deadline leaves the action unresolved; local CLI reaping
does not cancel the daemon request and this transition has no replay/reentry path.

A quiescence refusal retains the actual target-backend and prepared-transaction
counts from one maintenance query, plus at most sixteen backend identities and
an explicit truncation flag. The bounded sample includes backend type, state,
application name and lifecycle/wait timestamps; it excludes SQL text, users,
transaction GIDs and connection secrets. This evidence does not grant a retry,
terminate sessions or relax the zero-backend/zero-prepared-transaction guard.

Database mount readiness grants no collection or runtime activation authority.
The internal continuation below prepares repositories and confirms native WAL
delivery. Preserving private spool-copy recovery, matching runtime startup and a
complete encrypted pair remain required steps of the connected operator.
Production pause and resource admission still require the integrated measured operation.


### Repository preparation and native WAL continuation

Online preflight requires typed incremental recovery settings before source stop.
It also executes `pgbackrest version` as UID70 in the retained database container,
requiring the pinned 2.59.1 tooling. Tools in the application image do not prove
the separately retained database image can archive WAL. The probe reads no keys
or data and performs no installation.

Incremental settings come from the admitted maintenance limits; the helper uses
a private tmpfs input and does not require duplicate configuration beside keys.
For the confirmed pre-preparation missing-config failure after commit, the
existing recovery owner provides an explicit, bounded continuation. Its failed
receipt and helper remain preserved, and ordinary migration replay stays closed.
Invoke the existing `qt storage migrate --operation-file <private-plan>
--recover-repositories-file <private-request>` inspection first; only explicit
`--execute` dispatches recovery. The strict v1 request uses
`schema_version: qt.storage_repository_continuation.v1`, exact original-byte
`operation_sha256` and `final_sha256`, and integer `duration_seconds` from 1
through 600. It admits only the confirmed missing-config exception before
repository preparation mutated state, with the original failed helper and
retired migration reader independently identified and stopped.

A separately retained database image may lack the pinned native archiver after
preparation completes. Only this recognized missing-archiver failure admits
`qt.storage_repository_continuation.v2`; its request adds
`previous_continuation_sha256` over the exact expired v1 continuation bytes and
`database_image` as an immutable `sha256:` image identity. Completed preparation,
helper retirement, unchanged archive configuration and the missing binary must
be verified rather than inferred from a timeout.

The at-most-600-second allowance is an explicit recovery-only extension, never
a renewed migration/capture attempt or permission to replay SQL. Preserve prior
final/continuation bytes, original start anchors and downtime evidence. Reboot,
changed clocks/bindings, restarted source clients, changed helpers/readers,
unknown in-flight actions or unresolved prior outcomes refuse. Retain the
deployment/source holds; inspect authoritative state after a lost reply rather
than redispatching automatically. The implementation and exact refusal checks
belong to `scripts/automation/storage_online_recovery.py`.

The image-correction path preserves cluster, volumes, keys and
runtime contract, requires identical PostgreSQL/Timescale binaries, journals
replacement without volume removal and verifies actual WAL delivery. Prior
attempts remain evidence; uncertain outcomes refuse automatic replay. Runtime
readiness alone does not establish fresh collection or a complete backup pair.

An older forward handoff can retain an `online_guard_identity` trigger on the
promoted identity heap whose proof still requires the old header OID. The explicit operator repair
`scripts/db/manual_retire_promoted_identity_proof_v1.sql` checks the committed
operation, original proof OIDs, exact trigger body and permanent canonical
guards before removing only that obsolete row trigger. It uses a non-waiting
table lock and five-second SQL bounds, changes no rows, and preserves the proof,
functions, truncate guard and other constraints. A lost reply requires catalog
inspection; absence of the trigger refuses another dispatch. The forward-switch
implementation retires the exact source-bound row guards from the identity heap and any raw
mapping heap it promotes, within the existing final transaction. It verifies the
original proof, target OIDs and guard definitions while preserving the proof,
functions, truncate guards and unpromoted copies. Failure restores guards with the
transaction. The manual repair alone does not certify fleet-wide recovery; qualify the
preventive final transaction and subsequent writers on the exact release.

Completion inspection preserves the immutable forward publication and all
reschedule preimages. After a structurally complete `recovery_runtime_ready`
receipt, the canonical operation reader binds the current runtime recipe to the
final owner's recorded admission, including an explicitly corrected database
image. Before completion, publication still requires the original recipe bytes.
Plan, request, inventory, launch clocks and publication history remain checked in
both cases. Database identity comes from the authenticated operation request for
both original and forward capture formats; forward capture receipts own proof
and clocks, not a duplicate database identity. The existing runtime observer must
then verify the actual containers,
cluster, mounts, configuration and complete backup pair before deployment
publication; matching a recipe alone grants no operational authority. Inspection
never renews a deadline or dispatches the retired migration. The reader can report
current_layout_recovery_pending until a complete backup pair exists.
Deployment publication requires the full runtime/recovery observation.

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
pause and capacity admission require exact-release qualification. The runtime package explicitly
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
The explicit spool scan budget counts directories, acknowledgements and pending
files, up to the existing one-million-entry traversal bound. It is independent
of the unchanged 4,096 pending-file copy limit and byte, reserve and deadline
bounds. A large acknowledgement history is scanned and retained, never copied
or removed to make a migration fit. Production scan time must fit the original
final pause; accepting a larger declared inventory is not performance admission.
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
private new SSD working root, inventory and maintenance limits. The exact admitted
UUID observation directory is mounted at the fixed runtime data path; its host
basename does not define container layout. Applications keep
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
before maintenance; startup does not wait for a long physical baseline. Maintenance
uses the already prepared archive root as its working root and creates its existing
`tmp` and `canonical-staging` children there. It validates this mount before
registering a worker, so an absent configured working root fails at startup. Infrastructure
and UI services are not replaced by this application-recovery step.

`recovery_runtime_ready` records application health only. Normal pending-WAL recovery,
fresh collection, current/frozen reads, complete paired recovery and the final
operator/release outcome still require connected evidence. The final marker remains,
and ordinary launch/deploy has no new bypass. A connected disposable rehearsal now covers real application entrypoints and a
successful raw-archive maintenance cycle after the switch. It uses synthetic
source peers and filesystem identities; saved history/backup policy is unconfigured.
It does not qualify production resources, pending-WAL acknowledgement, fresh
collection or a complete encrypted pair through this operation.


### Atomic initial policy in the online switch

The online COMMIT command now stages the existing fixed targets, prepared history
tablespace registration and first saved policy in the same database transaction
as the verified table switch. It reuses the existing initial-policy implementation;
the historical separately invoked activation remains available for its original
procedure. No new CLI, scheduler, policy format or authority owner is added.

Catalog observation uses the already retained connection while target logins are
closed. The storage/migration transaction locks, physical verification, existing
configuration refusal, resource watch and original final/capture deadlines cover
both operations. A failure rolls back both the table switch and initial policy.
The ordinary runtime still performs no implicit migration or policy activation.

A successful pipe reply must confirm initial policy activation. After a lost
successful reply, the same worker freshly inspects the committed handoff and
current policy before the host accepts the outcome. The existing final receipt
binds that confirmation; older committed receipts without it cannot enter recovery.
No possibly dispatched COMMIT is replayed. This closes the missing registration
that otherwise let application startup reach an unconfigured history worker.


### Retained worker channel ownership

The shared host boundary owns `OnlineWorkerChannel`, the host side of the
existing bounded worker protocol. The real host rehearsal now uses this same
channel instead of its local read/write loop. It admits one initial controller
identity, assigns each sequence once, permits one outstanding operation, and
bounds command/reply frames and nonblocking pipe I/O by the caller's original
capture/final deadline. Poll supports the already admitted high descriptor
counts. Duplicate fields, nonfinite numbers, wrong identities/sequences,
truncated or oversized frames and uncertain writes permanently retire the
conversation from further use; it never reconnects or replays.

Phase owners still persist dispatch intent and validate operation-specific
results. The channel supplies no switch, rollback or recovery authority, and
the existing launcher independently verifies process retirement. A caller that
has fully received a valid response and then discards it can continue on that
same channel with the next sequence; unread or partial replies cannot take
that path. No replacement deadline, receipt, state owner, listener or operator
entrypoint is added. This channel alone does not qualify the complete operator
or actual source/load admission.

### Initial capture in the same confined worker

The existing private worker request may include `capture_preparation`, fixing the
cutoff, requested capture duration, request time and the original initial
preparation deadline. It cannot also claim an existing capture start. The launcher
requires the already completed source-resumption receipt, unchanged serving
clients and the exact original 600-second deadline. It persists the existing
worker intent before dispatch; it does not stop collectors for capture or copying.

The same confined worker first admits the actual pinned database environment and
builtin job set. It reuses `prepare_history_tablespace` and `prepare_attempt` to
create the fixed destination and atomic online captures. The latter uses NOWAIT
writer fencing for the short transaction, not a client stop or baseline copy.
Both phases use only the remaining initial window; no new 600-second allowance
is created. Source roots and permissions remain unchanged.

The launcher independently observes the committed capture and records its actual
start and duration in the existing worker receipt before yielding the retained
controller. That database capture owns the unchanged cumulative migration limit.
A background restart must match those original fields, inventory and immutable
request. A missing capture can be created only inside the original preparation
window; an existing capture is inspected, never replaced or given a new clock.
The final-state owner rechecks this exact saved capture before any final pause.
Uncertain final operations still have no replay authority. No new operator CLI,
receipt file, scheduler, service or general workflow framework is introduced.

The complete supported operator still needs production fleet/resource admission,
terminal outcome handling and measured collection/query impact before release.

### Worker-owned reference discovery and fixed catalog preparation

Catalog moves retain the controller's exact placement and capture start, checked
inside the existing migration/storage locks. Their time limit is clipped to the
original live proof and capture deadlines, including already completed retries.
An absent optional retained rollback table is reported without creating it.
Discovery and catalog preparation are background-only operations; they cannot
resume after final-session admission or an uncertain command. No generic mover,
new entrypoint, state owner or receipt is introduced. The complete operator,
production resource admission and measured collection impact remain separate
release requirements.

## Single local migration operation

`qt storage migrate --operation-file /absolute/private/operation.json` inspects a
fixed local Linux migration plan. Adding `--execute` runs the admitted operation.
The CLI calls `storage_online_operation.run_operation_plan` directly; it does not
send an HTTP request, add an MCP operation, or implement another state owner.

The operation acquires the existing deployment lock before initial admission and
retains it across the existing initial preparation and prepared-source driver.
The latter connects the existing owners through
background copy, held switch, independently verified reader retirement, recovery
mounts/repositories, private pending-WAL copy and application startup. It keeps
the deployment lock and source hold through that entire transition. It does not
create receipts or infer authority from empty queues. The existing phase owners
still validate all mutations and original capture/final wall/boot/monotonic clocks.

Background preparation finishes finite SQL baselines and relocations before
following the growing SQL/archive tails; reference pages stay bounded. A failed
command, uncertain COMMIT, retirement failure or recovery action propagates
without any automatic reconnect, replay, reopen, abort or marker removal. Numeric
limits are explicit inputs, not production budgets. Runtime readiness explicitly
does not claim a complete encrypted pair or ordinary relaunch authority.

The fixed recovery database recipe can be rendered before dispatch without
starting or changing a database. The maintenance PID setting may use the exact
Compose reference `service:tsdb`, which permits advance runtime configuration;
actual container admission still requires the exact replacement database PID
namespace. Foreign services and application PID sharing remain refused.

Before worker launch and again before the final pause, the driver observes the
serving source and inspects the proposed fixed runtime composition. Source image
and writer contracts, the separate empty private SSD spool, independent local
socket, private recovery-key directory, shared archive root, maintenance limits,
application mounts and environment must all match. Recipe/configuration digests
and directory identities must remain unchanged between those observations.
The runtime owner supplies the same configuration inspection used at activation;
spool and socket checks likewise remain with their existing owners. Future
maintenance must refer to `service:tsdb`, never the current source container PID.

These bounded observations neither capture data nor pause/start services. They
confer no lock, publisher-exclusion, COMMIT or recovery authority. After background
preparation the check is also bounded by the original capture deadline. All later
live checks remain mandatory, including independently verified reader retirement
before private recovery mounts. A changed or invalid plan refuses before pause.

Database-setting equality binds the resolved environment and every effective
container/host setting. Compose invocation metadata (`project.environment_file`,
like `project.config_files` and `project.working_dir`) is excluded from that
fingerprint: a resolved private recipe can omit the original env-file label while
preserving all credential and configuration values. Unknown labels and changed
environment values still fail equality. This normalization grants no permission
to reinterpret older receipts, renew deadlines or resume an expired preparation;
retained attempts must be reconciled with their original qualified owner.

Before the initial pause, the operation additionally admits the original
one-PGDATA source, its fixed mount-preparation recipe, candidate image attestation,
source archive identity and the full future runtime configuration. A bounded
read-only probe in the same candidate image checks the existing policy/resource
rules, original cluster/database identity, supported builtin jobs and the history
filesystem UUID. This probe has no source, archive, PGDATA or recovery-key mounts,
no PostgreSQL PID sharing and no capabilities. It uses the existing `PG_DSN`.
A failed check refuses before the initial preparation journal or any service stop.
The initial capture-absence check recognizes the earlier retained rollback table
`qt_fact_storage_cutover_v1.fact_versions` and its attached indexes as historical
data. That exact namespace must contain the logged table and no other relations
or functions. Other cutover namespaces, including the v2 capture, still refuse.
This distinction grants no mutation authority; retained-source immutability and
placement checks remain with the existing catalog movement owner.

The plan is a caller-owned private regular JSON file, with no symlink, duplicate
fields or unknown fields. It declares `qt.storage_online_operation.v1`, exact
source/candidate identities, prepared paths, immutable worker configuration and
explicit measured limits. It supplies a history cutoff and cumulative attempt
limit; it cannot supply a capture start or preparation clock. The existing
initial receipt supplies its saved completion time and original 600-second
deadline to the worker request. A serving-source reentry uses those same values.
An incomplete initial intent or uncertain final intent requires reconciliation
rather than repeating the operation. A durably ready runtime permits the completion observation and optional terminal
configuration handoff described below. The configuration is read again before mutation.

Runtime-ready return retains all phase journals and explicitly reports that
ordinary relaunch and a complete encrypted pair are not yet confirmed. Terminal
reconciliation/release, actual source/fleet/resource admission and measured
production impact remain required. This command is not permission to dispatch
an unqualified plan on production or bypass review.

### Private operation plan fields

The exact top-level fields are `schema_version`, `state_root`, `project`,
`source_revision`, `source_image`, `image`, `history_uuid`, `history_before`,
`attempt_seconds`, `request`, `inventory_path`, `descriptor_limit`, `memory_bytes`,
`limits`, `keys_root`, `socket_volume` and `spool_destination`. Paths are canonical
absolute existing paths. Existing private database/runtime recipes stay under
`state_root`; key contents and DSNs do not belong in this plan.

`request` is the existing `qt.storage_online_worker.v1` configuration. Its
`expected_started_at` must be null and `capture_preparation` must be absent.
`limits` contains the exact `OperationLimits` fields: `preparation_seconds`,
`final_seconds`, `recovery_seconds`, `runtime_seconds`, `spool_max_bytes`,
`spool_max_entries`, `spool_reserve_bytes`, `repository_max_bytes`,
`repository_reserve_bytes` and `recent_free_bytes`. These values must come from
release measurements; tiny rehearsal limits are not production defaults.

### Completion observation after runtime readiness

Reentering the same `qt storage migrate --operation-file ...` command after
`recovery_runtime_ready` performs bounded runtime observation, including with
`--execute`. Optional private environment staging and terminal publication follow only a
successful observation, as described below. All runtime actions must already be durably completed inside the
original final window. Inflight or incomplete actions, reboot, backward clocks,
changed configuration and another handoff refuse. No service action, migration
retry, policy change, deadline renewal or journal removal occurs.

The original live worker returns the initial-policy plan identifier during its
fresh committed-outcome inspection. That identifier hashes the full handoff
receipt and is retained by the host final-state owner. The dedicated maintenance
process verifies that exact database certificate, current applied policy, live
supervisor outcome and matching complete encrypted database/archive pair. It
reuses the existing policy-record and recovery-copy checks. It does not gain
access to private application source files; the earlier live switch inspection
remains the filesystem proof. Older receipts without the confirmed identifier
are preserved and cannot enter this completion path.

The host also freshly checks the admitted image/recipe, private spool, helpers,
retired reader, database identity and healthy exact application containers. A
backup still running yields a pending observation while collection continues.
A complete pair returns `recovery_verified`; this is an observation, not a new
journal state or deployment authority. The original final marker remains, and
ordinary deployment/relaunch continues to refuse until release bookkeeping and
its preserving deployment path are separately qualified. The observation's
bounded I/O timeout never extends initial, capture or final mutation deadlines.

### Restoring the original UI and administration services

The final runtime journal also restores `frontend`, `frontend-v2`, `grafana` and
`pgadmin` after the recovered backend, collector and maintenance process are
ready. It starts only an exact original container recorded as previously running;
an originally stopped service stays stopped. The original identity, image,
configuration, mounts and networks are checked through the same initial-source
client contract. Every requested start is journaled before dispatch and uses the
original final deadline. A lost reply remains inflight and cannot be replayed.

Runtime readiness requires these actions to finish and each restored service to
be running with its existing health check healthy. Completion observation checks
them again. Older runtime-ready receipts lacking these journal actions remain
preserved but cannot claim the stronger readiness condition. This restoration
preserves the original UI/admin versions; it is not a full-fleet image upgrade or
ordinary deployment release. The final marker and remaining deployment checks
still apply.

Restoration observations for the four preserved services are batched within each
check. Container configuration, state and network identities remain fresh for
every check before and after dispatch; no observation is cached across actions.
This removes repeated Docker reads without extending the runtime deadline.


### Canonical deployment resource bindings

The fixed storage overlay requires explicit `QT_STORAGE_POSTGRES_VOLUME`,
`QT_STORAGE_RECOVERY_SOCKET_VOLUME` and `QT_STORAGE_NETWORK` names. They identify
already prepared external resources; Compose must not silently create replacements
under its project-name defaults. Backend bot launches use that same explicit
network. These names must agree with the admitted migration/runtime configuration,
which is a prerequisite for releasing the retained migration hold.

Ordinary deployment with the recorded storage layout refuses any remaining
`QT_STORAGE_SOURCE_FENCE_ROOT`, including an empty value, in the process or private
environment file. That setting belongs to the co-located source layout. Compose
null/reset overrides can resolve it again from the env file, so they are not an
acceptable way to retire it. The preserving completion transition must prepare the
correct private environment while retaining its original evidence. Source-layout
deployment behavior is unchanged. Explicit names and an absent source-only setting
alone do not authorize release, restart, or deletion of any migration journal.

### Preparing the private deployment environment

The existing operation plan may include `deployment_environment`, the absolute
path of the existing private (0600) deployment environment. After a fresh
`recovery_verified` observation, or the explicit software repair admission below,
inspection computes a proposed environment
fingerprint. `--execute` additionally preserves the exact original bytes as
`storage-online-source.env` and creates `storage-online-deployment.env` in the
private state directory. Both are 0600, created once, and refuse changed or
partially written artifacts without overwriting them.

The internal release adapter derives the project, copied SSD spool, HDD archive,
UUIDs, inventory, maintenance limits, recovery-key directory, shared group,
database image, named volumes and network from the admitted runtime. It preserves
unrelated environment entries and credential bytes, removes the source-only
writer-fence setting from the proposal, and rejects duplicate or malformed dotenv
input. It does not edit the active environment, release metadata or migration
journals, restart services, or grant deployment authority. Publishing the proposal
still requires canonical configuration comparison and the terminal transition in
the existing release owner. Its hashes are configuration evidence, not completion
or replay tokens.

The same adapter carries six optional numeric settings only when they are
explicitly present in the admitted recipe: the backend's research evidence byte
limit and history-cache quota/free-space floor; and maintenance's lifecycle
interval, maximum canonical steps and maximum run seconds. It derives no new
defaults, refuses invalid or conflicting service values, and preserves all other
environment entries. The full canonical comparison must retain these exact
choices. This uses the existing deployment configuration owner so activating the
selected operational budgets does not require another release or a settings
registry. Runtime/resource admission and measurement still determine whether
those choices are safe and keep up with collection.

The storage overlay requires `QT_STORAGE_DATABASE_IMAGE` as the qualified local
image ID, removes the inherited database build, and sets `pull_policy: never`.
The deployer refuses database rebuilding in this layout and excludes PostgreSQL
from application pulls. Maintenance shares the newly built backend image and is
also excluded from remote pulls. Ordinary source-layout builds are unchanged.


### Terminal handoff to the existing deployer

An operation plan may additionally name `deployment_repository`, an absolute
clean checkout of the exact candidate revision and application source hash.
With both deployment inputs present, the normal `--execute` path can publish
the configuration only after fresh runtime and complete encrypted-pair admission.
Inspection without `--execute` does not publish. This is the final-state owner's
terminal file transition, not another deployment command or migration retry.

A real canonical Compose render must preserve all five storage service definitions,
private inputs, privileges, commands, health checks, resources and existing named
volumes/network. Only explicit fixed environment bindings, immutable image-to-build
wiring, the preserved secret-file location and equivalent read-only udev data
mount are normalized. Unknown differences refuse without logging private values.
The four application roles also use `pull_policy: never`; the existing deployer
owns their build. Non-storage UI and observability profiles remain deployer-owned.

Before either active file changes, a `release` section in the existing final
receipt records exact proposal, original-file, configuration and request hashes.
Original environment and `release.env` bytes are preserved as private create-once
artifacts. Source metadata accepts the older four-field deployer record or the
later record with an empty `storage_layout`; both must identify the original
source revision. Unknown fields, missing required fields, duplicates and any
claimed layout refuse. The read-only configuration inspection checks this before
rendering and detects metadata changes during rendering; publication repeats the
check under the deployment lock. It preserves the original bytes without adding
fields to historical evidence. Atomic replacements publish the proposed environment and a bridge
release record with the fixed layout and exact pending candidate, but no completed
current/previous revision. An old source-layout revision is not a rollback target.
All original migration journals and clocks remain unchanged and retained.

Interrupted file publication can reconcile only exact original/proposed bytes,
after fresh runtime/pair and clean-checkout/configuration checks. Conflicting or
partial inputs refuse. This does not replay SQL, Docker actions or incomplete
migration phases. Publication may occur after the original final deadline only
because all migration/runtime actions already completed inside that deadline.

The existing deployment interlock then admits only `deploy` for that exact first
candidate. It checks the actual resolved storage configuration again after builds
and immediately before activation, rejecting environment or mount drift. Its
normal fleet checks, including the dedicated maintenance image, must pass before
release bookkeeping records the completed handoff. A crash between normal release
recording and final receipt recording permits only the same normal deployment,
which repeats its full fleet checks; a release file alone never certifies success.
Later ordinary releases and credential/configuration updates use the existing
deployment/recovery validation and retain the migration evidence; the initial
environment fingerprint does not freeze their active settings forever. Operation reentry after handoff reports recorded state without
claiming fresh fleet health from obsolete migration container identities.

This terminal path still requires integrated release qualification and review;
component file/render tests do not establish production pause, capacity, workload
or successful migration-plus-deployment outcomes.

### Software repair before recovery completion

`qt storage migrate --operation-file <existing-plan> --repair-release-file <request>`
inspects an explicit software repair after a durably completed cutover/runtime
activation. `--execute` publishes only its deployment configuration; the existing
server deployer still owns service replacement. It cannot combine with migration,
deadline, cancellation or repository-continuation options.

The private request has exactly `schema_version: qt.storage_repair_release.v1`,
`final_sha256`, `source_revision` and `source_tree_hash`. The final digest uses
`storage_host_boundary.digest` over the original final journal, excluding a later
`release` section. The revision must differ from the original migration candidate.
The exact clean checkout and application hash are verified during configuration
inspection; qualification and operator authorization remain prerequisites.

Admission requires every migration/runtime action to be complete, the original
reader/helpers retired, unchanged journal/configuration/volume/network identities,
and the original database, backend, collectors and preserved clients healthy.
Only the maintenance health condition changes: its exact original container must
be cleanly stopped, with zero PID/exit code, no restart/OOM/dead state and a stable
state throughout inspection. A bounded read-only backend probe checks the committed
schema certificate, schema contract and current policy using existing database
owners. It does not claim the maintenance-only physical/recovery checks passed.

The original request and runtime recipe remain unchanged. Canonical rendering
requires the backend's `SOURCE_REVISION`, `SOURCE_TREE_HASH`, and
`QT_BOT_RUNTIME_IMAGE` to identify the exact selected release. It rejects stale,
missing, or different release identifiers; these software bindings do not change
historical run provenance. The existing release section stores the repair request;
canonical rendering still rejects changes to
storage definitions, private inputs, privileges, mounts, resources or database
image. Publication reports `complete_backup_confirmed: false`. Interrupted file
publication rechecks the same stopped runtime and exact proposal; after publication,
only the exact first software release can run through the normal deployer.

Successful software deployment does not certify the encrypted database/archive
pair, enable research, reclaim retained data or clear other target-state gates.
Verify those outcomes separately through their existing owners. Preserve partial
backup evidence; select the next native backup from verified chain state, not an
invented completion marker. Recovery uses the existing deployer and compatible
software fixes, never replay of the committed migration or rollback to the old
source-layout release.


The public storage composition also requires the existing read-only application
secret file and backend Docker socket to exist; neither may be created as an
empty fallback directory. This matches the migration runtime mount admission.
The fixed database recipe accepts Compose's explicit null entrypoint, which
inherits the qualified image entrypoint. Empty or custom entrypoint overrides
remain prohibited, and actual image/container entrypoints remain bound by the
preserving preparation checks.


Terminal deployment admission also verifies the initial preserving hold through
its immutable hash in the completed online preparation receipt, itself bound by
the final receipt. Publication, partial-file reconciliation and ordinary deploy
all require that exact retained hold. The normal deployer recognizes it only
after the qualified online terminal grant succeeds. Missing, changed, unrelated
or standalone legacy holds still refuse; no hold is deleted and no migration
operation is replayed.

### Online baseline order and SSD headroom

For a new online attempt, complete the raw lookup baseline and its HDD
relocation, build the identity registry on SSD and relocate it to HDD, then
allocate copied headers. This sequence prevents the three large SSD allocations
from overlapping. The saved progress owns each cursor, completion bit and
placement. Earlier attempts without the identity cursor retain their original
combined copy order; reentry cannot reinterpret a started cursor.

Identity pages compare every field and retain the source high-water mark.
They never retire captured header entries: only exact full-header comparison
may do that. Header pages compare existing identities before inserting missing
ones, reread inserts including concurrent conflicts, and retain native FKs and
source guards. Page comparison, checkpoint advancement and queue retirement
commit atomically. Source and earlier committed work survive failed pages.

The explicit identity_order step is available only before the first header
page and identity mirroring. It clusters the completed private identity table
using its primary index and records header_id_order in the same transaction.
An active mirror, existing header copy, insufficient rewrite allowance or
unavailable NOWAIT lock refuses it. Started copies retain their original order.
In this mode, pages advance by global ID under the original
(storage_day, market_commit_seq, id) upper bound; captured arrivals behind the
cursor remain queued. Sorting uses admitted SSD temporary space, a backend
temp-file bound and no parallel maintenance workers. Rewrites retain the
existing HDD allowance, resource watch and original capture clock.

### Background SQL page budget

An explicit page_rows request accepts up to 4,096 rows for background SQL copy
and identity preparation; direct callers default to 128. Archive copy/reproof
and held final work use at most min(page_rows, 256), preserving their byte,
object and final-time bounds. Each SQL command handles at most two pages with
per-page transactions and resource observations. A final-bound worker cannot
return to background copying. Larger batches do not extend command or
cumulative capture deadlines.

### Baseline-to-identity transition under ongoing collection

After finite raw, identity and header baselines, alternate bounded SQL catch-up
passes until both queues have been observed empty. Entering catch_up is not
identity-mirror readiness. The identity-capture step independently takes its
nonwaiting writer fence and rechecks queue bounds, exact source and live
attempt; intervening arrivals can still make it refuse. References and final
SQL/archive catch-up follow under the original deadlines. No empty-queue
observation grants final-switch authority.

### Explicit amendment of an unexpired capture deadline

    qt storage migrate --operation-file <plan> --extend-attempt-seconds <seconds> --capacity-file <forecast>

Inspection is the default; --execute applies only the amendment. It does not
launch another migration or stop clients. The maximum is 96 hours from the
original database capture start. Legacy implicit durations, expired/canceled
attempts, a live worker or final-switch intent are ineligible.

Require the deployment flock, independent proof that the exact background worker
is stopped with PID zero, full prepared-source preflight, and a fresh private
qt.storage_deadline_capacity.v1 forecast bound to the original plan and amended
deadline. Both filesystems must cover remaining targets, source growth, queues,
WAL, temporary, maintenance and recovery allocations above unchanged floors.
Forecast digests are retained; supplied numbers are not measured capacity.

storage_online_deadline.py owns host publication and its original 300-second
wall/boot/monotonic transition window. fact_header_v2_deadline.py owns the
transactional duration/audit change, excluding controller and page owners and
remaining within the old attempt deadline. The duration and before/after audit
commit together. Original start, pages, queues, placements and shorter phase
limits do not change.

Intent precedes dispatch. A lost reply requires exact audit/capture
reconciliation, never blind replay. Preserve the stopped container and original
plan/request/launch bytes; interrupted publication accepts only recorded old/new
states. Incomplete publication blocks launch. Reboot, transition expiry or
uncertain SQL needs explicit reconciliation. Completed publication permits
separate ordinary worker admission; it does not grant a final switch.

### Preserving a retired worker's package before header copying

--replacement-package-file accepts a private qt.storage_online_package.v1
manifest binding the original plan hash and immutable image, source revision
and source-tree hash. It is separate from a deadline amendment and is admitted
only after raw/identity history preparation, before any header page or identity
mirroring, with the worker stopped and no final intent.

Under deployment ownership, compare old and proposed packages through complete
prepared-operation preflights and read-only observations of the same capture,
progress and proof. SQL probes have database networking only, without data,
PID-namespace or key mounts. Record old/new request, launch, plan and runtime
recipe before preserving the stopped container and publishing files.

Only the four application image references may change in the recipe; database
image, credentials, mounts, ownership, settings and limits remain equivalent.
Both recipes pass the runtime validator. Partial publication reconciles exact
preimages within the original 300-second transition and unchanged capture
deadline. Ordinary launch remains blocked until complete. The amendment
neither launches nor deploys. Package journals are bounded to 2 MiB; deadline
journals retain their 64 KiB bound. Never log recipe contents.

### Explicit preserving terminal operator

    qt storage migrate --operation-file <plan> --cancel-attempt-file <package>

Inspection is read-only. --execute records one private terminal intent before
cancellation. qt.storage_online_terminal.v1 binds plan_sha256, immutable image,
source_revision and source_tree_hash. This route cannot combine with package
or deadline amendments and never edits their original files.

Admit the original retired worker, serving fleet, immutable request and prepared
runtime. The confined terminal command has read-only local mounts, retaining
only source-read capability after UID transition. PostgreSQL remains the writer
and must independently prove writable PGDATA/history on the same files. No
keys or recovery mounts are exposed.

Dispatch has one 300-second wall/boot/monotonic window with retirement time
reserved; SQL is bounded to 30 seconds. Reentry cannot renew dispatch or survive
reboot as execution authority. After dispatch, even an absent SQL receipt permits
only observation. Read-only reconciliation can occur later. Save confirmed
transient-worker retirement before removal without volumes; preserve the
original migration worker. Any terminal intent blocks copying and amendments.
A controller cancelling its own attempt retains the owning SQL session so
foreign controllers cannot enter a lock-handoff gap.

## Explicit composite-key preparation after terminal cancellation

fact_header_forward_keys.py reconciles exact committed cancellation, original
v1 layout and preserved shadow shapes before binding source heap/search-index
identities. It prepares both unique composite indexes concurrently while the
same SQL session retains controller exclusion. One original preparation
deadline covers builds and reentry. A valid index with lost acknowledgement is
reconciled by definition/OID; invalid, foreign or replaced indexes refuse
without automatic deletion or rebuild. Existing statement/lock limits remain.

The keys do not refresh identity/raw baselines across the uncaptured interval.
They install no early range CHECK that could reject collection at UTC rollover.
A keys_prepared result grants no adoption, attachment, switch, placement or
runtime activation; these belong to the following owners.

## Explicit schedule amendment while source collection continues

    qt storage migrate --operation-file <successor-plan> --reschedule-forward-file <package>

Inspection is the default; --execute publishes one stopped-worker amendment,
without launching work or stopping services. The command cannot combine with
another migration phase.

| Package schema | Permitted change | Preserved authority |
| --- | --- | --- |
| qt.storage_online_forward_reschedule_package.v1 | Later UTC end_day, finite max_objects and matching descriptor_limit, new plan path and fresh capacity | Original adoption expiry, proof, scope/resource limits and all start clocks |
| qt.storage_online_forward_reschedule_package.v2 | Exact completed V1 journal digest plus a strictly larger cumulative attempt_seconds, at most 345600 seconds from original adoption | Original start, progress, placements, archive queues and unchanged floors/byte/memory limits |
| V3 package correction after completed V2 retained-raw publication | Immutable worker image/source provenance only, bound to exact predecessor | Effective request, day, starts, expiry, inventory ceilings, placements and budgets |

Publication retains exact preimages, SQL proof identity and the original
five-minute wall/boot/monotonic clock. Intent precedes SQL; uncertain dispatch
is observation-only. Torn/expired publication refuses launch. Initializer
duration/expiry and archive-capture expiry change atomically for V2; queued
entries are never rewritten. No automatic extension exists. V3 is one package
correction, not another duration amendment.

V2 continue_guarded_proof=true is limited to raw_mapping_mode=retain_source.
An expired but unretired adoption requires exact live native/reference/archive
verification, complete identity proof and intact ALWAYS guards. Ordinary worker
admission still refuses expiry. Only this explicit amendment can extend the
allowance; starts and the 96-hour cumulative maximum remain. A missed new day,
insufficient capacity, changed guards or reboot refuses.

A separately invoked worker must present the published request and original
launch/adoption clocks. Completed publication remains readable after expiry,
without authorizing unfinished work.

### Explicit retention of the canonical raw mapping table

V2 may select only raw_mapping_mode=retain_source. Omission preserves historical
copy-and-swap behavior. Initializer admission requires completed source/target
identity checks and a brief raw-relation fence. It records the choice without
changing rows, cursors, counts or files and removes only the obsolete raw source
mirror; identity guards remain.

The worker omits both raw-copy scans but retains native FK, archive and resource
verification. Final handoff preserves the original canonical raw OID, seals the
abandoned copy in the retained namespace and records
raw_history_placement_pending=true. This is an SSD capacity bridge, not completed
HDD placement. Forecasts cannot claim reclamation credit. The distinct physical
move below must be followed by matching recovery verification.

    qt storage place-retained-raw --request-file <file-or-dash>

The request schema qt.retained_raw_history.v1 binds source_revision, request_id,
handoff_sha256, saved policy and existing resource_limits. It cannot select
arbitrary tables, drives or SQL. Inspection creates no intent or clock and
reads catalogs/sizes in a bounded read-only transaction. --execute performs
the admitted move; --cancel reconciles an unmoved intent. Flags are exclusive.

raw_mapping_v2_placement moves the fixed retained canonical heap/TOAST and
secondary indexes to HDD, keeping its selected primary lookup index on SSD.
Native DDL and fact_storage_state.evidence completion commit atomically under
storage exclusion, fresh capacity/watchers and the original plan clock.
Reentry first verifies completion. Cancellation proves original placement;
a new request follows only a reconciled terminal plan. No rows, keys,
permissions, archives or historical journals are rewritten.

The packaged adapter requires dedicated database UID, QT_IMAGE_SOURCE_*
attestation, admitted archive/working mounts and a live maintenance heartbeat.
It uses PG_DSN without bootstrap. During ordinary SQL, psycopg2 wait_select
permits SIGINT/SIGTERM cancellation through the watcher; restore prior callbacks
and handlers on exit. No COPY/large-object API or failed dispatch renews intent.

    scripts/automation/server_deploy.sh qt storage place-retained-raw --request-file -

This route owns deployment exclusion and requires the recorded SSD/HDD release,
clean checkout and matching maintenance image. It changes no collector/spool
or recovery admission. Settle the pinned first release before using a subsequent
qualified image; do not bypass old mount guards to inject the command.

The completion pointer changes the recovery layout fingerprint. Older pairs
are no longer current; a placement result returns recovery_verified=false.
Obtain a matching encrypted pair and restore qualification separately.
Collector admission includes pending-segment queue occupancy, rotation and rollback
time. Writers wait for raw-relation access before lease-row locks; after waiting,
expiry uses current wall time. Disk/WAL reservation alone does not establish
capture continuity for the maintenance duration.

## Retained-target adoption after an uncaptured interval

A separately authorized successor owns new adoption/archive journals and an
explicit operation identity. It binds reconciled predecessor retirement and
unchanged retained row/OIDs. Older queues and journals remain intact; no latest
attempt selection or expired-clock reset is permitted.

    qt storage migrate --place-forward-lookups-file <package>

fact_header_forward_placement.py performs at most one hour of fixed three-index
placement after reconciled retirement. It fences only private targets while v1
collection remains authoritative. All moves and completion commit together.
Intent preserves deadline and files; cancellation rolls back, and lost COMMIT is
reconciled without another move. Effective placement derives from the completed
lookup_operation_sha256 receipt; the previous all-HDD binding is not rewritten.
The confined host retires its worker and confirms SQL before publication.

The v2 --forward-package-file binds this placement and predecessor terminal
receipt while preserving earlier journals. fact_header_forward_adoption.py then
uses a short NOWAIT writer fence to install source-to-target mirrors and exact
mutation guards under one original adoption deadline. Native uniqueness/FKs,
trigger bodies/enablement and single-heap/OID/file topology remain required.

Target verification compares retained rows to source; forward coverage inserts
only missing source rows and rejects extra target rows. Mirrors cover inserts
behind the scan cursor. Page inserts and progress commit together. This closes
the uncaptured interval without assuming timestamps or allocated sequences are
commit ordered.

retire_adoption removes only the eight owned mirror/validation/seal triggers,
preserving rows, functions, queues and journals. It can retire expired work
without renewing clocks. Removal and terminal receipt are atomic; a lost reply
permits exact post-state inspection. Retired work cannot resume.

### Forward reference preparation

After bidirectional adoption proof, stage fixed payload/archive FKs through
the shared reference owner. Original FKs stay authoritative until final handoff.
Validate new leaves before parent adoption; later partitions inherit the admitted
parent reference. The header composite identity FK uses an ordered mirror to
publish identity before the immediate FK check. Retirement removes dependencies
before mirrors and restores the original header constraint shape atomically.

Identity/raw verification reads physical heap pages bounded by 128 blocks and
the configured row limit, committing original extent and tuple cursor with each
page. Exact source lookups prove retained rows; key-only target lookups avoid
repeated heap reads for already-proven rows. Missing rows are inserted in source
order and compared exactly. File/single-heap bindings reject rewrites/inheritance.

raw_heap_v1 extents initialize only in explicit adoption, not observer reads.
heap_verified counts physical visits; verified retains older work and physical
visits, so it is not a distinct-row count after protocol transition. Old identity
scan journals refuse incompatible resume; preserve them and use explicit terminal
retirement. A package/query change never updates a running worker or its clocks.

### Atomic forward SQL handoff

stage_forward_tables requires live complete adoption/reference proof and the
exact UTC boundary under fixed writer fences. Native range validation, guard
removal, FK promotion, retained heap attachment, archive retirement, policy and
terminal receipt commit in one transaction. Rollback restores the original
state. No early committed range CHECK may strand collection at rollover.
Preserve copied headers/catalogs and original raw source.

FK observations report elapsed SQL, reuse, heap size and transaction-local scan
counters. Reused constraints, index-only access or incomplete statistics do not
measure the full header scan. Assess final range validation and other pause
phases against the actual collector/I/O/temp/WAL envelope.

## Forward archive catch-up ownership

The existing copier/verifier uses the forward adoption identity, placement and
original deadline with separate capture/progress/queues. Canceled-operation
records remain unchanged. Late manifest inserts behind the baseline cursor enter
the capture queue; file publication surviving SQL rollback is verified and
reused before queue consumption.

Resource check_progress observes cancellation/deadlines each callback and
filesystem identity/free space every 0.1 seconds. Failed/stopped/stale watchers
refuse progress. Hashes, paths and read leases retain their normal checks;
perform a fresh inspection after the final lease pass.

Only the final retained-header transaction inside the live complete-inventory
context retires archive capture. Adoption cancellation removes its archive
triggers atomically while preserving queue rows/files/journals. Neither path
renews an expired work clock.

### Forward durable handoff certificate

Catalog relocation admits only the two fixed reference catalogs under the
original deadline and owning SQL session; a pooled second session cannot borrow
controller ownership. commit_handoff pins new parent/catalog OIDs, retained
legacy/raw OIDs, archive digest, forward terminal receipt and adoption-journal
digest. Finite baseline counters are not current full-row totals.

Outcome inspection uses the transaction fence, exact terminal/archive binding
and native legacy seal/partition contract. It remains observational after expiry.
Switch-time file identities are not permanent prohibitions on later policy-owned
relocation, and inspection is not a fresh whole-data hash verification.

The forward controller retains one actual SQL session across adoption,
references, archive work, catalog relocation, final attachment and observation.
Session loss refuses; it cannot reconnect to recover old ownership. Forward mode
and the actual adoption clock must be pinned by host admission before the
same-PID rule is allowed. Replies cannot opt legacy bindings into forward mode.

### Separate forward package publication after cancellation

    qt storage migrate --operation-file <original> --forward-package-file <package>

Inspection is the default; --execute publishes only metadata. The manifest
binds original plan digest, immutable image/source provenance, selected UTC
end_day and a new sibling operation path. Under deployment exclusion, preflight
both prepared runtimes and reconcile cancellation with the attested original
request, exact fleet, worker, inventory and file preimages.

Durable intent precedes publishing request, worker binding, separate plan and
four future application-image references. Preserve the original worker and
operation. Interrupted publication accepts recorded preimages within the original
300-second wall/boot/monotonic clock. Foreign edits, expiry, reboot or proof/fleet
drift refuse. Completed metadata does not launch work or grant capture/source
stop. Key, initialization and adoption clocks belong to their actual owners.

### Confined worker preparation clocks

The fixed entrypoint admits image, source-read identity and database before
supervised key preparation. Keys share one persisted deadline of at most 3600
seconds. After exact key/OID verification, persist initialization's separate
600-second intent before installing adoption/archive guards. Resource forecasts
use rounded-up actual remaining phase time, including tighter caller/retry bounds;
later phases receive fresh admission rather than charging the full horizon again.

The initializer retains SQL/controller/storage ownership. Adoption, archive capture
and initialization completion commit atomically. Reentry keeps original intents
and keys; lost COMMIT reconciles the complete record. Complete initialization
yields to the original active adoption expiry. Foreign bindings, changed keys,
expired incomplete work or retired adoption refuse.

Publication inspection reconstructs exact effective request/runtime/plan bytes.
Initialization and adoption are read together in one snapshot without scanning
history; request/cancellation and clocks must match. Observation never renews
time or grants source stop.

### Durable forward launch and final observation

Before Docker creation, record publication/request/worker-binding digests,
original wall/monotonic/boot clocks and key deadline. Container/contract/adoption
progress uses durable old/new preimages; interruption reuses only the exact created
worker. Changed or missing owned containers refuse instead of creating another
clock.

The 2 MiB launch journal retains complete cancellation/placement proof. Small
control replies carry its canonical digest; the host compares that digest to the
pinned full proof while logins are closed without reconnecting or truncating it.
Startup observes actual key and initialization receipts. Already committed
phases can reconcile after a deadline; unfinished work cannot extend.

Final-session admission requires completed publication, ready launch, exact live
worker and pinned initialization/adoption. With logins closed, compare its retained
owning-session observation to the receipt. Forward replies carry adoption identity,
not the canceled capture clock.

### Preserving forward retirement through the canonical terminal route

    qt storage migrate --operation-file <forward-plan> --cancel-attempt-file <package>

This selects the separate forward terminal owner after publication. Inspection
is read-only. Execution requires exact request/publication/launch, stopped worker,
preserved original worker, prepared runtime and serving source. Final-switch
intent refuses this route; retained-session rollback owns that state.

One 300-second intent precedes dispatch. The confined probe uses read-only mounts
and independently proves PostgreSQL's PGDATA/history writable. Lost COMMIT allows
only exact identity/clock/guard/reference/archive reconciliation; missing or changed
outcomes refuse. SQL removes only forward-owned temporary guards/references.
Preserve original journals and every source/target/queue row. Reconciliation can
occur after expiry or reboot without redispatch.

### Explicit recovery after a failed forward retirement

A stopped probe or absent terminal receipt cannot authorize retry after a
nonwaiting lock refusal. --cancel-attempt-file may instead accept one explicitly
reviewed qt.storage_online_terminal_recovery.v1 package: the existing terminal
fields plus previous_terminal_sha256 binding the exact dispatched predecessor.
Preserve that journal and create a separate recovery journal/probe.

Recheck the same plan/fleet/worker/capture/preimages and prove old probe retirement.
Reconcile SQL first; completed retirement or retired adoption cannot redispatch.
Only this recovery drains the Fact writer gate before acquiring remaining
identity/raw/archive locks without waiting. A known refusal rolls back the
lock-only savepoint and releases the gate before another attempt. No retirement
DDL runs without all locks. Lock acquisition shares a 20-second ceiling within
30-second SQL and 300-second terminal bounds. Clients are neither stopped nor
cancelled; timeout rolls back, and uncertain replies remain observation-only.

### Canonical forward operation and UTC cutover window

    qt storage migrate --operation-file <forward-plan> --execute

Derive the request only from completed publication and the separate operation
path. A successor's retirement journal prevents its normal dispatch; completed
predecessors remain evidence. Match package-derived backend source revision/hash,
bot-image label and application image IDs while preserving unrelated environment.
Legacy image-only receipts retain their interpretation.

Reuse the launcher and actual key/init/adoption clocks for background reference,
SQL and archive catch-up. Wait for the selected UTC day under adoption expiry
while collection serves. Source stop is admitted only within 30 seconds of the
boundary, or half the final allowance when smaller. Both operation and stop owner
cap final work at 600 seconds. Remaining wait after drain consumes that same
final allowance. Missed day, expired worker, insufficient lifetime or changed
publication refuses. Complete range CHECK and attachment remain one transaction.

### Review online preparation before source stop

--prepare-forward-only inspects by default. With --execute, it runs the same
bounded adoption/reference/archive preparation, rechecks publication/source/runtime,
closes control and independently retires the worker before returning
forward_background_prepared. It never enters the UTC wait, source stop, login
gate, handoff, recovery or deployment. Incompatible phase options or a non-forward/
already-final operation refuse before dispatch; failed or uncertain retirement
cannot report success.

Preparation is a mutation: mirrors and archive capture remain after worker
retirement, so collection still writes admitted targets and requires monitoring.
The result retains original expiry/day. A later separately authorized invocation
rechecks tails/references and rebuilds process-local archive proof under unchanged
clocks and worker identity. It neither renews time nor changes the day. Abandoned
preparation needs explicit preserving retirement; return does not cancel it.
A new source stop at or after the selected midnight refuses. An already-admitted
pause may drain across midnight within its original deadline.

Reference discovery returns strict prepared/validated booleans. Complete references
need no schema step; an existing unvalidated reference needs validation only.
Missing or contradictory flags refuse. Parent adoption/final handoff still
recheck the entire reference set; discovery is not switch authority.

### Measure forward keys before selecting a cutover

    qt storage migrate --operation-file <original-plan> --prepare-forward-keys-file <package>

Inspection preflights original and proposed runtimes. --execute runs only the
supervised two-key preparation in a confined source-read worker, without
publication, adoption, archive capture, client stop or selected cutover date.

Before a key intent exists, another proposal may inspect the same original
operation only after the previous probe is positively retired and absent by ID
and name. Preserve its exact receipt in a digest-named sibling before replacement.
Interruption reuses that receipt. Foreign owners, changed plans, unresolved
containers, existing key intent, terminal cancellation or retirement receipts
cannot use this handoff.

Record one original 3600-second host wall/boot/monotonic intent before dispatch;
native keys retain their own original 3600-second start/expiry/OIDs. Reentry uses
the tighter bound. Valid committed keys may reconcile and later be reused;
invalid/foreign keys refuse without automatic removal. Retire the worker before
a fresh read-only proof completes host publication. Expired/rebooted hosts may
observe completed results only. Normal forward publication refuses unresolved
key journals or live readers. Reusable completed keys do not renew the later
launch deadline or unfinished key work.
