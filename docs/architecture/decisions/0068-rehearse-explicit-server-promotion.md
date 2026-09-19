---
component: adr-rehearsed-server-promotion
subsystem: system
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - deployment
  - recovery
code_paths:
  - scripts/automation/server_deploy.sh
  - scripts/automation/check_release_ci.py
  - scripts/automation/pin_deploy_recovery.py
  - scripts/automation/storage_handoff_pause.py
  - scripts/ci/test_storage_handoff_pause.py
  - tests/test_storage_handoff_pause.py
  - scripts/ci/test_server_promotion.py
  - scripts/ci/test_server_core_recreation.py
  - docker/docker-compose.storage-server.yml
  - tests/test_server_storage_config.py
  - .github/workflows/test.yaml
  - tests/test_release_ci.py
  - tests/test_server_promotion.py
---
# ADR 0068: Rehearse Explicit Server Promotion

## Status

Accepted on 2026-09-07 for manually initiated, compatible single-node updates.

## Context

The deployment helper already builds exact-commit application images and checks
health, initialization, and image identity. Previously a failed update could
leave partially replaced services, and overlapping helper invocations could
change the same checkout. Local passing tests and PR review alone do not qualify
the actual merged commit or establish that restoring old code remains safe.

## Decision

Keep one operator deployment helper. Add an explicit `promote` action requiring
a full candidate SHA, exact successful develop push CI, and an operator's
compatibility acknowledgement naming the currently deployed SHA. Check every
required job in the same latest run attempt, including disposable rehearsal.
Serialize helper mutations using one host lock.

Before replacement, retain the running image identities under private recovery
tags and save the rendered Compose configuration. Recover compatible failures
with those local artifacts without rebuilding, fetching, or pulling. Never
roll back database contents automatically. Failed promotion remains a failed
operation even when recovery succeeds. An interrupted or failed recovery keeps
its marker and blocks new deployments until explicit `recover` succeeds.

Use two evidence layers: synthetic Docker services to inject deployment
failures into the real controller, and actual QT core images on isolated storage
to exercise bootstrap, clean collector stop/restart, health, and retained data.
No provider enrollment, real credentials, runtime egress, production volumes,
or remote host is admitted by these rehearsals.

## Consequences

An operator or agent still chooses when to deploy. No automatic merge trigger,
new always-running environment, or GitHub-to-host credential is required.
Compatibility is a release-specific decision, not inferred from tests or a
migration filename scan. Initial installation and incompatible schema/storage
cutovers remain explicit operator procedures. The existing `deploy` action is
an intentional manual escape hatch, not proof of CI qualification.

Retained images consume disk until deliberately retired; private snapshots may
contain resolved secrets and must not be published. Host loss, disk failure,
active provider reconnect, broker/observability behavior, and arbitrary schema
compatibility are outside the disposable promotion evidence. The first host
cutover still requires actual-state admission and post-deploy acquisition checks.

See [server deployment](../../engineering/server-deployment.md) for commands,
rehearsal scope, and first-cutover requirements.

## Preserving storage handoff interlock

The incompatible storage switch must not enter compatible promotion recovery.
The internal fixed-layout `paused_storage_clients` boundary shares the existing
host deployment lock and durably writes `storage-handoff.json` before stopping
any clients. It pauses backend/initializer/collector, both frontends, Grafana and
pgAdmin; PostgreSQL and passive telemetry remain running. It inventories both
the Compose project and its network, refuses unexpected peers (including bot
runtimes), unsafe restart policies, forced kills, identity changes and unhealthy
process state. It never stops an unrecognized container. Container exit is not
proof of application spool durability or a successful database handoff.

The lock spans the caller's procedure. The private hold survives process death,
exceptions and successful pause; re-entry requires the same recorded release
and container/image identities. Ordinary helper mutations, including direct
`deploy`, `recover`, `rollback`, `qt` and environment initialization, refuse any
hold presence, even a partial file or broken symlink. Read-only release status
reports it. No automatic restart, old-image fallback, or hold-clear operation is
provided. Direct Docker/SQL, a different state directory or an older checkout
remain privileged bypasses; the complete cutover must exclude them.

This is an internal, locally rehearsed boundary, not a public cutover command.
The owning procedure still needs to compose publisher pause, exact database
outcome reconciliation, two-root/policy/runtime activation and recovery before
it can safely retire the hold and resume collection. The database's handoff
certificate remains the sole authority for database commit outcome; the host
hold cannot substitute for it. Synthetic Docker rehearsal qualifies process
stop/lost-reply retry and the interlock, not the real database, actual server,
application drain, disk performance or the one-day migration budget.


## Fixed storage runtime wiring

The opt-in `docker-compose.storage-server.yml` overlay is the prepared first
SSD/HDD layout. It keeps the existing PostgreSQL volume/path and the absolute
SSD spool path, adds the prepared HDD at the same `/qt-history` path for database
and application services, and points archive readers/writers at its `archives`
subdirectory. Inventory and operating limits are explicit read-only files;
missing host paths never become implicit directories. The host archive path used
by bot readers must name that same HDD archive subtree. Host admission must prove
these relationships and distinct filesystem UUIDs before activation.

Backend, initializer and collector use the pinned database UID/GID 70 so private
archive files have compatible ownership. The collector shares PostgreSQL's PID
namespace for real file/resource verification. Backend retains its existing
Docker socket through an explicitly supplied supplemental group; neither other
writer receives socket authority. Compose-injected environment values are the
configuration source, with dotenv loading disabled for these non-root services.
Application scratch/report directories in the image are owned by UID 70; source
code and host data are not recursively chowned by the image or overlay.

The exact preserving-operator Python modules are packaged in the backend image
and included in its existing source-tree attestation. Unrelated manual SQL is not
bundled and no migration runs at startup. The existing actual-core rehearsal has
a fixed-storage mode that uses owned disposable PostgreSQL/working and history
volumes, synthetic UUID metadata/configuration and no provider egress. It checks
real application startup, cross-service private archive reads, preserved legacy
spool bytes, PostgreSQL namespace/resource observation, collector shutdown and
container recreation. It does not qualify physical disks, live file permissions,
real Docker socket access or a full migration/backup workload.

After the preserving cutover records `storage_layout=ssd-hdd-v1` in the existing
private release state, the ordinary helper retains that layout across releases
and selects the matching checkout's storage overlay. A checkout missing the
overlay is refused before replacing services. Alert preview/restore also retain
the recorded layout. Operator environment values cannot deselect it. Initial
activation remains owned by the complete preserving procedure; there is no new
layout toggle or automatic migration in the deployment helper.

Compatible promotion records the layout and SHA-256 of its fully rendered,
image-pinned recovery snapshot in the existing promotion state. Recovery requires
the same recorded layout and unchanged snapshot before changing the checkout.
It uses the frozen snapshot alone, without merging current storage or alert
settings. Legacy non-storage promotion records remain recoverable; fixed-layout
records must include the fingerprint. This checksum binds local evidence, not
arbitrary schema compatibility or the contents of bind-mounted configuration.

The preserving cutover must still compose host admission, pause, database/policy
result, matching runtime activation and recovery before recording the layout and
releasing the persistent host hold. Adding a file, setting a state field manually
or rendering Compose is not authority to activate it directly. Older helpers or
manual edits remain privileged bypasses and must be excluded by the cutover.


## Preparing PostgreSQL access to the HDD

A host-mounted HDD is not sufficient: the existing PostgreSQL container also
needs the fixed `/qt-history` mount before it can use historical tablespaces.
Preparation must preserve the original PostgreSQL data volume, image and cluster
identity. Include database container preparation in the planned pause and measured
migration duration; do not assume a zero-downtime mount change.

The fixed actual-core disposable rehearsal now starts PostgreSQL without the
history mount, writes an SSD record, stops it cleanly and creates the same-image
replacement with the history mount and unchanged PGDATA. It kills the preparation
controller before restart, reconciles the created replacement without recreating
it again, then checks cluster identity and source records. It also resolves real
historical table and index files beneath the HDD path and verifies their records
after the full application recreation. Synthetic filesystems and an empty owned
cluster limit this evidence; it does not authorize a live database replacement.

The preserving host procedure must still bind the intended replacement durably
under its existing hold and reconcile an interrupted container transition. The
current pause boundary admits only its original container identities. Do not
clear that hold or bypass its checks to use the rehearsal sequence on a server.


The PostgreSQL readiness probe uses loopback TCP, matching application transport.
The pinned image starts a socket-only temporary server during first initialization;
that server must not admit application startup or database preparation as healthy.
This correction does not weaken the requirement for a clean database stop.
