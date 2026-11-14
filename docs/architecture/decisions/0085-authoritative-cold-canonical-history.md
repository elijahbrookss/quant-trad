---
component: adr-authoritative-cold-history
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - canonical-facts
  - storage
  - recovery
code_paths:
  - src/market_data/cold_history.py
  - portal/backend/db/cold_history_models.py
  - portal/backend/service/storage/repos/cold_history.py
  - scripts/db/canonical_cold_migration.py
  - scripts/db/canonical_cold_load.py
---

# Serve historical canonical Facts from verified cold pages

## Context

The former archive retained every PostgreSQL Fact envelope even after freeing its payload. Research needed those envelopes for version selection, exact identity and frozen replays. Active PostgreSQL replay and custody checks made this directory operationally significant. Its historical growth on SSD was unbounded.

## Decision

Move the complete accepted version to immutable cold pages when its hot lifecycle closes. The page preserves all Fact clocks, original ingestion identity, source and material evidence, provenance, quality and tombstones. The same canonical query filters each eligible version by source, commit and availability before ranking, then applies invalidation. Frozen datasets keep their original logical identity and commit ceiling.

The compact PostgreSQL page catalog and its per-series time and commit bounds identify candidates. Exact version, material and key lookups use rebuildable HDD tablespace indexes. Check every selected claim against the decoded exact row. A digest assists lookup. It supplies no independent authority. Store only live admission state and compact page-level metadata on SSD.

Use the per-series owner and shared commit sequence for both cold and hot admission. Write and verify immutable bytes before committing the locator and catalog rows. Uncommitted pages and interrupted files remain invisible. Preserve existing coherent PostgreSQL and external archive recovery with the exact cold inventory, raw and checkpoint holds. Keep newly reacquired provider observations under their own acquisition evidence.

Qualify and use one authority switch. Checkpoint staging and tests run in isolation. Retire the old serving dependencies only after exact inventory, frozen, custody and scope parity, a current independent paired restore, and explicit production approval.

## Consequences

Batch cold reads should favor sequential I/O. The historical lookup footprint moves to HDD. Test same-hardware random reads and live admission overlap before sustained use. Retain the documented recent window and a bounded cache where measurements justify it.

Legacy objects that match the full accepted evidence can be reused directly. Build locators and optional typed research projections from their verified content. A fresh provider response can fill a verified gap or regenerate a projection after equivalent inputs are established. It cannot recreate QT's prior receipt, canonical revision, custody or frozen identity.

The [cutover runbook](../../engineering/canonical-cold-cutover-runbook.md) defines the production recovery boundary. [Staging and parity](../../engineering/canonical-cold-staging.md) describe the checkpointed migration and isolated evidence.
