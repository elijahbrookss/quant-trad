---
component: adr-run-scoped-archive-evidence-reuse
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - storage
  - archive
  - evidence
  - performance
code_paths:
  - portal/backend/service/market/canonical_retention.py
  - portal/backend/service/storage/repos/fact_archival.py
  - portal/backend/service/storage/repos/fact_lineage.py
  - src/market_data/archive.py
---
# ADR 0084: Reuse Current-Byte-Bound Archive Evidence Within One Run

## Status

Accepted for implementation on 2026-10-26. Exact-path correctness tests,
independent review, disposable PostgreSQL qualification, and representative
complete dependency-proof timing remain required before deployment.

## Context

Canonical archive pages often depend on the same immutable raw objects. A
bounded census of September 7 pages 211 through 215 found 31,435 dependency
references but only 6,360 distinct objects. The overlap is a reason to test
reuse, not evidence of a general production speedup.

Every page currently reloads current placement authority, checks expiry, hashes
the selected object, decodes every row, reconstructs the content fingerprint,
and compares exact mapping witnesses. Repeating mutable authority and current
byte checks is required. Re-decoding the same unchanged object for the same
already-checked witness claim inside one bounded worker invocation is not.

## Decision

Separate three concerns:

1. Page planning and authority remain transaction-current. Every page freshly
   resolves exact mappings and page bindings, excludes expired placements,
   checks current dependency holds, and creates holds in its own atomic catalog
   transaction.
2. Current bytes remain authoritative. Every selected object receives a fresh
   size and SHA-256 check for that page, followed by the existing successful-exit
   file-stamp fence.
3. Fully decoded immutable evidence may be retained only by one bounded
   `CanonicalFactRetentionExecutor` invocation. It is keyed by the complete
   acknowledged manifest identity and the current file stamp. A cache hit is
   allowed only when the freshly loaded mapping-and-witness claim is a subset of
   claims already checked against those exact bytes.

The cached immutable evidence contains record count, ordinal coverage, decoded
logical-byte charge, source scope, and content fingerprint. Current placement,
expiry, hold, page, generation, and transaction authority are never supplied by
the cache. Mapping and witness claims are stored separately as short-lived proof
bindings and are recomputed from current rows on every page.

A changed manifest identity or file stamp invalidates the entry and fails the
operation. Different current mapping or witness claims force a complete decode
and exact witness comparison; missing or expired authority fails before cache
lookup. Different bytes fail the fresh checksum. Decode and logical-byte budgets
are charged as though cached evidence were decoded again, so reuse cannot make
an otherwise oversized page admissible. Object and claim counts bound retained
memory, cancellation remains active during fresh checks, and disposal clears all
entries at every executor exit, exception, stop, or action-boundary handoff.

Whole-partition verification, reclamation, the exclusive handoff, and their
fresh-byte and transaction-current authority checks do not consume this cache.
There is no persisted or cross-run verified cache and no receipt, schema,
retention, timeout, or resource-limit change.

## Consequences

- Repeated Parquet decoding and content-fingerprint reconstruction can be
  skipped for an exact unchanged object/claim pair within one run. Fresh object
  hashing, current SQL authority, expiry checks, and final reclamation proof
  remain mandatory.
- Same-byte path replacement still refuses because the file identity changed.
  Content mutation refuses through the fresh checksum. Mapping, witness,
  manifest, placement, or expiry changes cannot inherit prior authority.
- Cache hits are diagnostic evidence, not a throughput claim. Qualification
  must compare complete dependency-proof and total page elapsed time on
  identical inputs, including fresh SQL and checksums.
- The cache is deliberately narrow. A future persistent proof cache, reuse
  across worker runs, weaker byte checks, or broader reclamation reuse requires
  a separate decision and proof contract.

## References

- [Fact data plane](../data/GENERALIZED_FACT_DATA_PLANE.md#canonical-archive-staging)
- [Ephemeral handoff witnesses](0080-use-ephemeral-fresh-row-handoff-witnesses.md)
