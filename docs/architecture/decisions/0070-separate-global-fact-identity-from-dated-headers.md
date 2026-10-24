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
  - scripts/db/fact_header_forward_placement.py
  - scripts/db/raw_mapping_v2_copy.py
  - scripts/db/archive_reference_v2_placement.py
  - scripts/db/archive_root_v2_copy.py
  - scripts/db/fact_header_v2_handoff.py
---
# ADR 0070: Separate Global Fact Identity From Dated Headers

Accepted September 16, 2026 for the canonical integrity boundary. Placement,
migration and release qualification are separate from accepting that boundary.
This record explains the design; [storage management](../persistence/STORAGE_MANAGEMENT.md)
owns the current operating procedure.

## Keep uniqueness independent of placement

Canonical Fact identity and observation/revision uniqueness span all retained
history. PostgreSQL partitioned uniqueness includes the partition key, so simply
adding a storage date to the header table would weaken existing identity or
break its references.

Keep global identity in one canonical registry and use dated headers for
physical grouping. Payload and archive references bind to immutable Fact
identity. A storage day is placement metadata, not observation time: old
observations can arrive or be corrected later. Causal selection, hashes and
frozen Dataset meanings do not change when physical placement changes.

The [series/day directory decision](0071-route-range-reads-through-series-day-directory.md)
addresses bounded time selection across those physical days.
[Registered targets](0069-bind-storage-objects-to-registered-targets.md) supply
immutable filesystem/database bindings for movement.

## A preserving migration needs committed change capture

Retain one serving source while a finite baseline is copied in bounded
transactions. Install capture before copying; an allocated sequence watermark
is not evidence that concurrent changes committed. Durable cursors and verified
pages allow interrupted preparation to resume without discarding prior work.

Final equality must include committed catch-up, complete reference/archive
closure and unchanged source identity under the required fences. Row counts,
sampled hashes and a previously empty queue cannot replace exact proof.
The final transaction must retain its original deadline and resolve an uncertain
COMMIT from authoritative database outcome, rather than retrying the switch.

Preserve corrections, historical schemas, current reads, frozen references and
source provenance. Runtime startup validates its schema and reader floor; it
does not perform this cutover implicitly.

## Physical resources remain a separate engineering constraint

A logical target is insufficient. Admit actual filesystem UUID, PostgreSQL
namespace, tablespace identity and owned files before moving data. Include
source/target growth, indexes, WAL, temporary allocations, retained originals and
replacement recovery baselines in capacity planning. Never credit future frees.

Global lookup indexes and dated payloads need not have identical placement.
Select their physical layout from measured ingestion and query behavior.
A small warm fixture can prove a semantic property without establishing HDD
latency, concurrent workload capacity or full-volume completion time.

## Consequences and successors

This design adds global-registry and directory metadata but preserves one
canonical Fact repository. It permits independently managed historical groups
without inventing another logical store.

[Online preparation](0073-prepare-storage-migrations-with-live-collection.md)
separates bulk work from collector downtime.
[Retained legacy headers](0077-retain-legacy-headers-during-forward-cutover.md)
provide a forward-cutover alternative to rebuilding an already equivalent
historical heap. These successors preserve the identity boundary; their
qualification and explicit operator admission still govern each transition.
