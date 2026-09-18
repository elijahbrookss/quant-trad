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
  - scripts/db/fact_header_v2_admission.py
  - scripts/db/fact_header_v2_references.py
  - scripts/db/fact_header_v2_placement.py
  - scripts/db/raw_mapping_v2_copy.py
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


Before preparation commits, the known-v1 source admission compares persisted
columns/defaults/collations, checks, references and secondary indexes with the
trusted empty shadow model. It separately requires the original global ID and
revision keys, frozen v1 guard bodies, the source and payload-partition triggers,
the existing hot projection, known incoming references and the standalone commit
sequence. Unexpected active v2 objects or dependents are refused, including
views/functions attached through the hot projection.

Admission performs no repair. A refused preparation rolls back both capture and
shadow DDL, while a retry rechecks the current source. This is source-contract
admission only; the eventual operator must repeat it under the final writer
fence and still qualify physical placement, capacity, timing and rollback.

After baseline copying, a bounded catch-up step takes the source writer fence
and enables a fixed trigger that mirrors new global identities in the source
transaction. A busy writer or excess backlog refuses activation; a failed
activation rolls back catch-up, trigger and phase together. Enabling this
mirror at initial preparation caused private-partition DDL to wait on unfinished
source writers in the disposable rehearsal, so the initial phase remains
queue-only. This supports validating old
payload/archive references against the new identity registry while collection
continues. The original source is still the authoritative header during this
stage; an identity whose private header is still queued is not an active v2
record. The normal identity-to-header guard is installed only at the reviewed
handoff after draining and verifying the copy. Source, identity and pending ID
commit or roll back together. Retry refuses a changed mirror instead of repairing
it. This does not introduce a second runtime writer or a new storage policy.


The internal reference stage prepares and validates only the known payload
children and archive references against the mirrored private identities.
Original source FKs remain in force. Preparation briefly fences writers;
validation is separate and permits normal collection. Validated child
constraints are attached to a parent constraint without replacement. Changed
definitions, disabled enforcement or incomplete coverage refuse advancement.
These steps do not remove original references, activate v2 or qualify capacity
and final cutover duration.


The internal preserving copy can bind a fixed recent SSD and history HDD before
creating its shadow tables. It reuses the storage boundary's PostgreSQL process
and namespace checks. New global identities and their indexes go directly to the
verified history tablespace; dated headers use that tablespace before the fixed
cutoff and pg_default afterward. The current database, small routing catalogs and
temporary capture state remain on the verified recent filesystem. Copy progress
records the binding and refuses a changed destination rather than relocating
already copied data. This is not a general placement API or runtime policy.


The same fixed migration now has a bounded copy for the installed immutable
raw archive record lookup. This table is a measured source of SSD growth; it
cannot be left behind when the historical headers move. Its replacement and
all indexes are created directly on the already bound history HDD. The source,
small cursor and transactional composite-key queue remain on the recent SSD
during the copy.

Preparation admits the known model, original immutable guard, permissions,
indexes and foreign-key enforcement, and refuses unexpected dependents or
replication arrangements. Each page rechecks bound schema, trigger/function
definitions and physical placement. Every field is compared before its exact
(raw_record_id, manifest_id) queue key is retired. Cursor, copied rows and queue
retirement commit together. Source writes capture keys without waiting for an
HDD mirror. Changes and truncation of the original immutable table are refused.

This adds no generic table mover or runtime writer. It requires the prepared
header migration and does not switch the authoritative raw lookup, delete its
source or emit readiness. The production operator must still qualify final
verification, dependency handoff, capacity, throughput and the full one-day
migration budget. The optional raw lookup switch in the tiny disposable fixture
is test evidence only, never an operator entrypoint.
