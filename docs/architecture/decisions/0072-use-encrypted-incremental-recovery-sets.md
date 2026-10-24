---
component: adr-encrypted-incremental-recovery
subsystem: persistence
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - recovery
  - storage
code_paths:
  - portal/backend/service/storage/incremental_recovery.py
  - scripts/automation/storage_recovery_prepare.py
  - scripts/ci/rehearse_incremental_runtime.py
  - tests/test_market_data/test_incremental_application_db.py
  - docker/test/incremental-application.compose.yml
  - docker/build-backup-tools.sh
  - scripts/ci/rehearse_incremental_recovery.py
  - docker/test/incremental-recovery.Dockerfile
---
# ADR 0072: Use Encrypted Incremental Recovery Sets

Proposed September 24, 2026 to replace repeated full logical recovery copies.
This decision specifies the paired recovery design; an implementation or
disposable rehearsal does not authorize activation on a serving installation.
Use [storage management](../persistence/STORAGE_MANAGEMENT.md) for the current
operator boundary and exact recovery procedure.

## Why a recovery point has two parts

A PostgreSQL backup alone is incomplete when frozen evidence depends on external
archive objects. A generic PGDATA file copy is also not a PostgreSQL-consistent
backup. Use pgBackRest physical backups for database pages, tablespaces, indexes
and required WAL, and restic for encrypted immutable archive snapshots. QT owns
pairing and retention; it does not invent cryptography or a database backup format.

Publish one complete recovery point only after both components are durable and
verified. Bind the database backup label, archive snapshot, database identity,
storage layout, archive inventory and tool versions. Failed or interrupted work
cannot expire the previous complete point.

Retain the archive-expiry fence through physical completion and subsequent
archive-inventory capture. The supported target is the database backup's earliest
consistent endpoint. Arbitrary later point-in-time recovery needs separately
available external dependencies; continuous WAL alone cannot establish it.

## Retention follows dependencies

An initial full baseline is necessary. Incremental backup still involves scanning
and changed-block work; it does not promise work proportional only to inserted
rows. Bound chain length and replace baselines using measured recovery, WAL,
growth and capacity limits rather than a calendar promise.

Retire dependency-safe database sets and corresponding archive snapshots only
after replacement complete points exist. Partial attempts, interrupted native
expiry and WAL growth remain visible capacity obligations. Never acknowledge and
discard WAL to hide a failed or full destination.

## Keys and process ownership

Keys stay outside source, receipts, logs and ordinary settings. The fixed storage
composition mounts the operator-provisioned private SSD recovery directory
read-only into PostgreSQL and the dedicated storage-maintenance service.
Application backend and collector services do not receive this recovery-key mount.
Maintenance retains its database-owned identity while application processes
retain their own spool ownership.

Repository preparation verifies existing database/filesystem/key identity; it
does not generate keys, enable PostgreSQL archiving or take a backup. Explicit
activation must prepare repositories before enabling archiving. Configuration
absence cannot silently activate the encrypted engine.

A separately recoverable key copy and compatible restore instructions are
necessary. Keys held only on the failed server cannot support disaster recovery.
Using the same HDD for both repositories does not protect against loss of that
HDD; an independent destination is a separate deployment choice.

## What verification must establish

Restore into empty isolated destinations using compatible PostgreSQL and
extensions with explicit tablespace mappings. Verify current, correction, frozen
and archived reads against the selected point, including interrupted pairing,
retirement, wrong keys and exclusion of later writes. Preserve existing recovery
evidence until replacement verification succeeds.

The tool rehearsal, runtime-engine rehearsal and QT application acceptance fixture
exercise different boundaries. Their existence is not a passing result, a
production throughput estimate or proof of a particular installation's restore.
Use exact-source results and measured operating limits for activation decisions.

The dedicated maintenance process reuses the existing supervisor, policy,
cancellation, database fence and status projection. It introduces no second
scheduler or recovery authority. Private restic parent selection can reuse
unchanged archive content, but complete selected-inventory verification and
independent restorability remain required.

References: [pgBackRest](https://pgbackrest.org/user-guide.html),
[restic backups](https://restic.readthedocs.io/en/stable/040_backup.html),
[storage management](../persistence/STORAGE_MANAGEMENT.md).
