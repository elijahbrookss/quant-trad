---
component: adr-dated-fact-headers
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - storage
  - postgres
  - explicit-migration
code_paths:
  - portal/backend/db/fact_identity_schema.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/market_storage_models.py
  - portal/backend/db/fact_storage_schema.py
  - scripts/db/fact_header_v2_capture.py
  - scripts/db/fact_header_v2_copy.py
---
# ADR 0070: Separate Global Fact Identity from Dated Headers

Accepted on 2026-09-16 for the clean-schema integrity boundary. Physical
placement and migration qualification remain incomplete.

Payload archiving alone leaves detailed Fact headers and their lookup indexes
growing in the active database. Daily header partitions make that history
independently placeable, but including a date in every unique key would weaken
global duplicate protection. The same Fact could otherwise appear twice on
different days.

Keep a compact immutable global registry with a primary key on Fact ID and a
unique key on series, observation key and revision. Store the ID's immutable
storage day there. Detailed dated headers reference the ID and day together,
while ID-only payload and archive relationships reference global identity.
A header insertion registers identity in the same transaction, and a deferred
guard rejects identity without its matching header. Header/payload atomicity
remains enforced by the existing deferred payload guard.

This keeps storage placement out of Fact identity and market clocks. The
registry and detail partitions remain behind PG_DSN; they are not independent
databases or application-level sources of truth. Known-at selection, exact
frozen revisions, material hashes and correction semantics remain unchanged.

Runtime provisions new empty days under an advisory transaction lock and
records them in an immutable catalogue. A recorded missing partition is a
failure requiring inspection, not a request to recreate an empty table.
Startup validates the layout and refuses older or partial schemas without
performing an implicit migration.

A global registry still grows and requires indexes. Its bytes and write cost
must be measured and included in the whole-system storage forecast. Dated
headers do not themselves choose a drive. The initial implementation uses the
default tablespace pending registered-target placement and recovery tests.

The [v2 layout notes](../../engineering/fact-header-layout-v2.md) distinguish
implemented clean-schema support from the remaining operator cutover. This
decision extends [ADR 0069](0069-bind-storage-objects-to-registered-targets.md).

## Capture during the preserving upgrade

The one-time tiered-v1 upgrade uses a private pending-ID queue so writes
committed during backfill cannot fall behind the copy cursor. The internal
capture primitive in scripts/db/fact_header_v2_capture.py installs the queue
and source INSERT trigger in one caller-owned transaction and savepoint.
Every queued ID commits or rolls back with its original Fact. It does not
filter by commit sequence or a wall-clock watermark.

Preparation requires a brief writer-free boundary: a busy source is refused
immediately for coordinated retry, while ordinary readers remain permitted.
The source stays immutable, including against truncation, during capture.
The queue is logged PostgreSQL storage under the same PG_DSN. A narrowly scoped
trigger function writes it without granting collectors access to migration
internals. Retry verifies the original database, source and queue identities,
function bodies and enabled triggers instead of silently resetting progress.

This is a capture primitive, not full source admission or an executable
cutover. No CLI/runtime path calls it. The preserving orchestrator still needs
complete v1 schema admission, bounded copy and catch-up, capacity and one-day
rehearsal, correct FK/view handoff, rollback and final verification before
activating v2. Capturing IDs alone never makes migration_ready true.


The internal shadow-copy stage builds the existing v2 models in the same private
migration schema. It reads bounded pages using the installed source's
(storage_day, market_commit_seq, id) index, preserves every persisted header
field, builds global identity and conservative series/day bounds, and commits
the verified page and cursor together. New IDs drain from the transactional
queue without a sequence watermark. An ID leaves that queue only in the
transaction that verified its matching target header and identity.

The original header table, hot payloads, archive objects and frozen dataset
records remain authoritative and untouched. A private copy is not an admitted
runtime layout. No v2 ready certificate is written. Source admission, physical
placement, complete final verification and dependency handoff are separate
release requirements; the private target is never selected by application
queries. This primitive adds no alternate DSN, runtime writer or placement UI.
