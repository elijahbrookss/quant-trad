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
