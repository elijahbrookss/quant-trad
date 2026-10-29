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
# ADR 0084: Reuse Current-Byte-Bound Archive Evidence Across Routine Yields

## Status

Accepted for implementation on 2026-10-26 and extended on 2026-10-29 to
retain only sparse checkpoint-source evidence across routine scheduler yields.
Exact-path correctness tests, independent review, disposable PostgreSQL
qualification, and representative complete dependency-proof timing remain
required before deployment.

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

Production COPY receipts across nine bounded invocations showed why the
invocation boundary is sometimes also the wrong evidence boundary. Seven page
identities were retried across a routine yield. Three heavy-manifest retries
repeated about 164 seconds of checkpoint work each, while four later retries
had effectively no checkpoint penalty. The directly paired excess was 493.121
seconds, 8.59% of measured stage-call active work. This is a bounded observed
opportunity, not evidence that every yield is expensive.

## Decision

Separate three concerns:

1. Page planning and authority remain transaction-current. Every page freshly
   resolves exact mappings and page bindings, excludes expired placements,
   checks current dependency holds, and creates holds in its own atomic catalog
   transaction.
2. Current bytes remain authoritative. Every selected object receives a fresh
   size and SHA-256 check for that page, followed by the existing successful-exit
   file-stamp fence.
3. Fully decoded immutable checkpoint-source evidence may be retained by one
   `CanonicalFactRetentionExecutor` across a routine time- or step-budget yield
   for the same resolved storage root, storage day, and page action. Only the
   bounded value cache transfers into a newly constructed repository. Database
   sessions, transactions, locks, plans, statement deadlines, page buffers,
   byte-verifier state, and raw-lineage evidence never cross the yield. The
   entry is keyed by the complete acknowledged manifest identity. A cache hit
   is allowed only when the freshly loaded mapping-and-witness claim is a subset
   of claims already checked against those exact bytes.

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
an otherwise oversized page admissible. Object, row, and retained-byte counts
bound memory. The receiving repository must have exactly the same full-page
read and dependency limits or it disposes the cache. Cancellation remains
active during fresh checks. A
cancellation request, exception, busy refusal, storage-root/day/action
transition, incompatible limit, scan completion, selected action, process exit,
or other non-routine boundary disposes all entries deterministically. A routine
yield logs the retained counts and charge.

Whole-partition verification, reclamation, the exclusive handoff, and their
fresh-byte and transaction-current authority checks do not consume this cache.
There is no persisted verified cache and no receipt, schema, retention, timeout,
or resource-limit change. Durable catalog progress remains the only resume
authority; retained rows are an optimization that can always be discarded.

## Consequences

- Repeated Parquet decoding and content-fingerprint reconstruction can be
  skipped for an exact unchanged object/claim pair within one compatible page
  action, including after a routine scheduler yield. Fresh object hashing,
  current SQL authority, expiry checks, and final reclamation proof remain
  mandatory.
- Same-byte path replacement still refuses because the file identity changed.
  Content mutation refuses through the fresh checksum. Mapping, witness,
  manifest, placement, or expiry changes cannot inherit prior authority.
- Cache hits are diagnostic evidence, not a throughput claim. Qualification
  must compare complete dependency-proof and total page elapsed time on
  identical inputs, including fresh SQL and checksums.
- The cache is deliberately narrow. A future persistent or multi-process proof
  cache, weaker byte checks, raw-lineage retention across yields, or broader
  reclamation reuse requires a separate decision and proof contract.

## References

- [Fact data plane](../data/GENERALIZED_FACT_DATA_PLANE.md#canonical-archive-staging)
- [Ephemeral handoff witnesses](0080-use-ephemeral-fresh-row-handoff-witnesses.md)
