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
  - scripts/ci/rehearse_incremental_recovery.py
  - docker/test/incremental-recovery.Dockerfile
---
# ADR 0072: Use encrypted incremental recovery sets

Proposed September 24, 2026 following explicit user authorization to replace
routine full logical recovery copies. Tool rehearsal is implemented; production
composition and application acceptance are not implemented or qualified by this
decision. The completed legacy copy and ongoing restore must be preserved.

## Problem

Repeated logical dumps copy all retained database content and archives. A logical
restore also rebuilds indexes. The measured current copy took about 85 minutes;
the much longer ongoing operation is restoration. Neither compressed backup
size nor a small fixture predicts production recovery time.

A backup of only PostgreSQL is incomplete: frozen research can depend on archive
objects outside the database. Backing up live PGDATA with a generic file copier
does not produce a PostgreSQL-consistent recovery point.

## Candidate and acceptance boundary

Evaluate pgBackRest block incremental physical backups for PostgreSQL data,
tablespaces, indexes and the WAL needed for consistency. Evaluate restic for
encrypted, deduplicated immutable archive snapshots. These are maintained backup
formats; QT must not invent cryptography or a physical database backup format.

One QT recovery point must bind the database backup label, archive snapshot ID,
database identity, storage layout, archive inventory and tool versions. It is
complete only after both components are durable and verified. Failed or
interrupted work must not expire earlier complete points.

The database backup runs under existing storage-management ownership and the
shared archive-expiry fence. Capture the archive inventory after the physical
backup completes while retaining the expiry fence. The chosen restore target is
that database backup's earliest consistent endpoint. An arbitrary later WAL
target is not qualified unless its external archive dependencies are also
available. This ordering still needs a real QT concurrency/application proof.

Physical recovery requires the compatible PostgreSQL major version and required
extensions. Restore into empty isolated destinations with explicit tablespace
mapping. Never overwrite a serving database as part of qualification.

## Retention, encryption and capacity

An initial full baseline is required. Subsequent backups can reuse prior content;
block incremental does not mean zero scanning or that work equals newly inserted
rows. Replacement baselines are necessary to bound retained dependencies.
Sunday is not a mandatory full-backup schedule. Establish baseline renewal and
retained chain limits from measured recovery time, changed-block growth, WAL and
peak capacity before activation. Do not silently reinterpret the current saved
backup-copy count as a different retention promise.

Expire only whole dependency-safe database sets and their corresponding archive
snapshots, after a replacement complete set exists. Never let independent native
expiry delete a baseline or archive snapshot still needed by a published point.
Partial/orphaned attempts and continuous WAL growth also require bounded,
visible cleanup and capacity accounting.

Encryption keys stay outside source, receipts, logs and ordinary settings. A
separately recoverable copy of the keys and restore instructions is required;
keys stored only on the failed server do not establish disaster recovery.
Both repositories initially use the existing HDD, with its UUID checked.
This protects against some logical failures but not loss of that HDD. A second
device/offsite copy remains a later deployment choice, not a new prerequisite
for implementing this local method.

## Rehearsal and remaining release work

The disposable script creates its own PostgreSQL 15 cluster, sockets and secrets.
It compares a physical baseline and incremental recovery, historical table/index
placement, frozen rows, matching archive hashes, exclusion of later writes,
wrong-key rejection, interrupted database/archive backup survival and recovery
after replacing and expiring an old native dependency chain. It has no production
DSN or QT data and is not an application acceptance test or performance forecast.

Before enabling this candidate, finish actual QT/Timescale recovery, coordinated
QT publication/retention across both repositories, key recovery, maintenance/status
integration, WAL/capacity guards and a production-size rehearsal with collection
performance observed. This is a change to the authorized backup slice only;
drive placement, storage UI and research semantics remain under their existing
contracts.

References: [pgBackRest guide](https://pgbackrest.org/user-guide.html),
[pgBackRest configuration](https://pgbackrest.org/configuration.html),
[restic backups](https://restic.readthedocs.io/en/stable/040_backup.html).

The opt-in disposable image docker/test/incremental-recovery.Dockerfile uses
QT's TimescaleDB 2.14.2/PostgreSQL 15 base, pinned backup-tool source checksums,
non-root execution and a private cluster. Its extension probe restores a real
hypertable as well as the ordinary table/index/archive fixture. A successful
native PostgreSQL run must not be reported as this image's Timescale result.
