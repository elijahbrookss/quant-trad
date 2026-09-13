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
