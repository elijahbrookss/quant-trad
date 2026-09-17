---
component: adr-registered-storage-targets
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - storage
  - recovery
code_paths:
  - src/core/storage_targets.py
  - src/core/storage_inventory.py
  - portal/backend/db/storage_target_models.py
  - portal/backend/service/storage_management.py
  - portal/backend/service/storage/header_destinations.py
  - portal/backend/service/storage/header_filesystem.py
  - portal/backend/service/storage/header_journal.py
---
# ADR 0069: Bind Storage Objects to Registered Targets

Accepted on 2026-09-13 for drive enrollment and reviewed configuration.
Physical database tiering and policy execution remain under implementation.

QT needs to add storage without changing the identity of existing records or
silently reinterpreting an archive key relative to a newly selected drive.
A single archive-root setting cannot express that distinction.

Register prepared filesystems under immutable target IDs and filesystem UUIDs.
The administrator-owned inventory supplies host topology; the database behind
the existing PG_DSN owns enrollment, policy revisions, and review records.
The inventory does not select placement policy. One filesystem has one capacity
budget even when it serves several roles.

Allocation selects destinations for new work. Reads use a recorded target and
relative object key. The filesystem UUID must match on every admission; a
missing mount must never become a directory on the SSD. Existing target IDs
cannot be repointed to different filesystems. New HDDs can receive new work
without changing prior object locations. Relocation must verify content before
a fenced location update and retire the source only after readers are safe.

A policy review is durable but does not activate a policy. The server validates
the base revision and idempotency key, measures drive availability, and presents
setting changes and blockers. UI confirmation cannot substitute for successful
physical operations. Until an executor is implemented and its physical
preconditions are proved, application is explicitly rejected.

This design introduces a separate V2 Settings surface while preserving read-only
Overview and Operations. Browser requests select prepared target IDs and cannot
supply host paths, block devices, shell commands, or SQL. This retains the
existing private operator deployment boundary; it does not introduce multiuser
authorization.

The database layout still needs a measured choice. PostgreSQL 15 partitioned
unique constraints require the partition key. QT's fact identity and
observation/revision uniqueness currently omit a storage date. Naively
partitioning headers by date would weaken or break those constraints and
referencing foreign keys. A global identity registry placed on HDD with dated
detail partitions is a candidate, but its ingest cost and migration have not
been established. Moving an entire relation to an HDD tablespace is another
candidate with different latency and expansion costs. Neither is asserted as
implemented by this ADR.

See [storage management](../persistence/STORAGE_MANAGEMENT.md) and
[PostgreSQL partitioning limitations](https://www.postgresql.org/docs/15/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-LIMITATIONS).


## Prepared PostgreSQL destination refinement — 2026-09-17

The dated-header direction in [ADR 0070](0070-separate-global-fact-identity-from-dated-headers.md)
uses the same recorded-target rule. Register a prepared tablespace by database
identity, target ID, filesystem UUID, OID, name, location and verified directory
paths. Registration is immutable and accepts fresh server-namespace evidence;
each current move review binds that registration to its copy plan. An existing
target root must exactly match the root observed during verification.

Device numbers and directory inodes are current observations. Include them in
each move's review and durable intent, while retaining stable registration
across a newly verified remount or physical restore. This requires a fresh
review when physical evidence changes and does not resolve running or
uncertain operations automatically. A changed database identity or stable
binding needs an explicit recovery/cutover decision.

Consequences: a path or tablespace name alone cannot redirect a reserved move.
A pure placement hash is insufficient for reservation, and missing destination
proof blocks the entire batch. Registrations referenced by movement records
cannot be deleted. Terminal-ledger retention must respect that dependency.
The internal repository now implements these admission rules; public wiring,
physical execution and crash reconciliation remain incomplete.

Pure review tests cover the identity/hash boundary. Expanded disposable journal
tests cover registration and reservation transactions, but qualification of this
refinement is still pending. Namespace verification was separately qualified in
CI using a private PostgreSQL cluster. Neither evidence set certifies deployment,
an end-to-end move, or a restore workflow.
