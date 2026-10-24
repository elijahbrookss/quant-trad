---
component: adr-ephemeral-fresh-row-handoff-witnesses
subsystem: data
layer: decision
doc_type: adr
status: accepted
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
  - portal/backend/service/market/canonical_retention.py
  - portal/backend/service/market/market_storage_lifecycle.py
  - scripts/automation/canonical_retention_action.py
---
# ADR 0080: Use Ephemeral Bounded Handoff Authority Witnesses

## Status

Accepted for implementation, amended 2026-10-24. Disposable PostgreSQL
qualification and an exact-action production canary remain release evidence;
this decision does not by itself activate or authorize reclamation.

## Context

A fresh byte proof can pass while the separate ten-second destructive handoff
refuses on repeated random metadata/source index and heap reads. Raising the
limit, retaining permission from an old proof, or dropping fresh checks would
change existing protections. A full-size directory-only experiment already
exceeded the entire handoff limit; cached representative timings are insufficient.

## Decision

Keep bounded, private typed SHA256 witnesses from the exact first-pass values
validated by the unchanged durable v12 receipts and source/byte proof. Catalog
tuple hints remain private and ephemeral. Source-lineage headers use per-page,
per-query-batch typed commitments instead of retaining every source tuple in
memory. Never persist these runtime witnesses, expose them as evidence, or
accept them as permission by themselves.

Under the separate exclusive lifecycle transaction, use NOWAIT relation locks,
re-admit relation OIDs/filenodes/columns/topology and canonical identity
enforcement. For each page catalog, freshly read all hinted proof fields and
compare its current hinted-row digest/cardinality and total manifest cardinality
in one SQL snapshot. All distinct hinted rows must still belong to that manifest.
Matching contents and an equal total count prove exhaustiveness, including
detection of extra rows. Missing, moved, reused or substituted tuples refuse.
Type/null/field boundaries use the exact PostgreSQL binary record encoding of
explicit fixed projections.

Retain the first-pass logical directory, current hot lookup, complete cold
manifest material and match multiplicity validation. The source proof retains
typed per-page commitments and the exact placement hash, plus the physical
header and hot-payload relations, their storage days, each referenced cold
manifest's typed digest and each cold storage day. It does not retain one Python
object or tuple location per source occurrence.

Under the final locks, re-admit the identity/header topology, validated
nondeferrable composite foreign keys, replication role, immutable-mutation
contracts and the always-enabled hot-payload insertion guard. Require each
captured daily header and hot-payload relation to remain the correct child for
its storage day. For cold source days, lock and admit any still-present hot
child so closed-day state plus the insertion guard preserves hot absence.
Freshly read every distinct referenced cold manifest and retention state and
refuse overlapping manifest ranges. Then freshly verify the authoritative
per-page dependency catalogs and active generation pointers. The immutable source relations and
manifest catalog cannot change through supported operations between the exact
first pass and final activation; their enforced authority plus the fresh final
catalog reads preserve the original source commitments without an O(source-row)
exclusive reread.

Final receipt lookup is batched with the existing bounded page enumeration.
Final catalog statements group at most16 requested manifests and split at256KiB
of tuple-hint text, preserving a single large page's existing admissibility.
Each result remains bound to one requested manifest, exact typed digest and
both hinted and logical counts. A missing/extra result or late mismatch fails
the entire batch before its pages are marked fresh. This reduces repeated SQL
transport without retaining final-proof authority across statements or runs.
The 10-second exclusive bound and 1-second statements are unchanged. Progress
logs distinguish final admission, headers, catalog batches and complete evidence;
only a qualified full normal-path observation can establish capacity.

For source lineage, each page's authoritative canonical-dependency catalog is
read in deterministic 1,000-row batches. Every batch is joined to its current
header, hot payload or complete cold manifest during the first pass. The proof
retains the typed header and cold-match digests, counts and complete placement
hash for each batch. Before final activation it verifies that these batch counts
equal the freshly checked canonical-dependency catalog count and that every
placement hash has the exact expected shape. Batch digests come only from the
fixed typed projections and remain private frozen values. Any missing page,
mismatched count, conflicting manifest identity, changed final catalog,
relation, generation, manifest material or storage-day authority refuses the
entire handoff.

This is an operational immutability proof, not protection against a privileged
actor deliberately disabling the admitted guards, mutating data and restoring
the guards between the two transactions. Such out-of-band mutation is outside
QT's supported storage contract and remains prohibited. Missing or altered
guards are detected at final admission.

All mount/file guards, source coverage, pinned-Dataset audit, cancellation,
per-statement and overall budgets, final NOWAIT parent/child locks, DROP and
post-DROP rollback checks remain. No schema/index migration, persisted receipt,
scientific result or archive object changes. Witness memory has a private
128 MiB refusal bound. Per-page commitments avoid retaining full source rows
without losing page bindings.

A private operator may request one exact canonical action by storage day and
expected action. It uses the same saved policy, planner, admission, locks and
executor, and refuses if the fresh plan selects another action or becomes
ineligible. It does not run raw history/recovery work and cannot be used as a
generic planner override.

## Consequences

- This trades enforced immutable source authority and bounded relation/manifest
  witnesses for the millions of retained source-row witnesses that previously
  exceeded the handoff memory limit. It removes no first-pass source, catalog or
  byte comparison and keeps final catalog and cold-manifest verification.
- DDL/rewrites, missing or changed enforcement and conflicting relation locks
  require a safe retry even when logical content is intact.
- New paths need real disposable PostgreSQL encoding/mutation/enforcement and
  transactional rollback tests. Unit mocks and live read-only diagnostics do not
  qualify a destructive release.
- Full-size timing must include source/cold checks, catalogs, expiry, coverage,
  pins and file fences; a cached subset does not establish readiness.

## References

- [Fact data plane](../data/GENERALIZED_FACT_DATA_PLANE.md#default-disabled-physical-reclamation)
- [Identity layout](0070-separate-global-fact-identity-from-dated-headers.md)
- [Legacy sealing](0077-retain-legacy-headers-during-forward-cutover.md)
