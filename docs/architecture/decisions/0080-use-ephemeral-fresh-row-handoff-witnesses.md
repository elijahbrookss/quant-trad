---
component: adr-ephemeral-fresh-row-handoff-witnesses
subsystem: data
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - storage
  - evidence
  - reclamation
code_paths:
  - portal/backend/service/storage/repos/fact_handoff.py
  - portal/backend/service/storage/repos/fact_archival.py
  - portal/backend/service/storage/repos/fact_dependencies.py
  - portal/backend/service/storage/repos/fact_reclamation.py
---
# ADR 0080: Use Ephemeral Fresh-Row Handoff Witnesses

## Status

Draft, 2026-10-18. Implementation and independent review are in progress;
whole-handoff timing and disposable PostgreSQL qualification are required.
This decision does not activate or authorize production reclamation.

## Context

A fresh byte proof can pass while the separate ten-second destructive handoff
refuses on repeated random metadata/source index and heap reads. Raising the
limit, retaining permission from an old proof, or dropping fresh checks would
change existing protections. A full-size directory-only experiment already
exceeded the entire handoff limit; cached representative timings are insufficient.

## Decision

Capture bounded, private physical row hints and typed full-field SHA256 witnesses
from the exact first-pass values validated by the unchanged durable v12 receipts
and source/byte proof. Keep physical ordering separate from canonical receipt
ordering. Never persist the hints, expose them as evidence, or accept them as
permission by themselves.

Under the separate exclusive lifecycle transaction, use NOWAIT relation locks,
re-admit relation OIDs/filenodes/columns/topology and canonical identity enforcement,
and freshly read all hinted proof fields. For each catalog, compare its current
hinted-row digest/cardinality and total manifest cardinality in one SQL snapshot.
All distinct hinted rows must still belong to that manifest. Matching contents
and an equal total count prove exhaustiveness, including detection of extra rows.
Missing, moved, reused or substituted tuples refuse. Type/null/field boundaries
use the exact PostgreSQL binary record encoding of explicit fixed projections.

Retain the first-pass logical directory validation. The final fresh header proof
can derive current directory agreement from the re-admitted identity/header keys
and actively enforced nondeferrable composite foreign key. This assumes continuous
canonical constraint enforcement between phases; validated metadata alone does
not certify historically bypassed violations. Preserve current hot lookup,
complete cold-manifest material and match multiplicity. Check current expiry
for every distinct dependency kind/ID in the final exclusive transaction.

A preparatory source comparison at the end of the shared phase brings the
bounded source working set near the exclusive check. It consumes the existing
run budget; it neither extends the exclusive deadline nor replaces any fresh
exclusive-phase read. Cold representative timing alone cannot justify it.

All mount/file guards, source coverage, pinned-Dataset audit, cancellation,
per-statement and overall budgets, final NOWAIT parent/child locks, DROP and
post-DROP rollback checks remain. No schema/index migration, persisted receipt,
scientific result or archive object changes. Witness memory has a private
128 MiB refusal bound. Bounded sorted source runs avoid retaining full source
rows and permit identical-reference deduplication without losing bindings.

## Consequences

- This trades bounded transient memory and stricter physical-location refusals
  for avoiding repeated global index routing and transferring full metadata.
- VACUUM FULL, rewrites, tuple movement, missing/disabled enforcement or conflicting
  relation locks can require a safe retry even when logical content is intact.
- New paths need real disposable PostgreSQL encoding/mutation/enforcement and
  transactional rollback tests. Unit mocks and live read-only diagnostics do not
  qualify a destructive release.
- Full-size timing must include source/cold checks, catalogs, expiry, coverage,
  pins and file fences; a cached subset does not establish readiness.

## References

- [Fact data plane](../data/GENERALIZED_FACT_DATA_PLANE.md#default-disabled-physical-reclamation)
- [Identity layout](0070-separate-global-fact-identity-from-dated-headers.md)
- [Legacy sealing](0077-retain-legacy-headers-during-forward-cutover.md)
