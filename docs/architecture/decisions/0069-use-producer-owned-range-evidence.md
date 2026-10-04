---
component: adr-producer-owned-range-evidence
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - coverage
  - research
  - provenance
  - market-data
code_paths:
  - src/market_data/range_evidence.py
  - src/market_data/fact_registry.py
  - src/data_providers/streams/coinbase.py
  - portal/backend/service/storage/repos/book_range_evidence.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/market/frozen_dataset_service.py
---
# ADR 0069: Use Producer-Owned Range Evidence

## Status

Accepted on 2026-09-23.

## Context

Book features describe seconds with actual book events. Dense interval coverage
mistook witnessed quiet seconds for missing collection. Existing stream validity
and processing receipts were not included in frozen research quality. Moving
provider rules into research coverage would make every new data family require
changes to unrelated consumers.

## Decision

The existing Fact contract names its range-evidence owner. The data boundary
invokes that owner and returns source-bound, versioned, hashed evidence through
its existing range-quality read. Provider adapters interpret transport continuity;
the book producer combines it with validity, canonical lineage and processing
receipts. Generic coverage validates evidence and accounts for intervals without
understanding Coinbase, instruments, or book internals. Research retains authority
over acceptable gaps, staleness and sample alignment.

The initial book projection is read-only and bounded. It certifies bracketed quiet
ranges only when acknowledged, verified archive objects and successful processing
receipts prove them. Recorded invalidation/recovery boundaries remain interruptions,
including partial seconds. Unknown boundaries remain unexplained. Freeze pins the
normalized proof and its archive dependencies; replay does not consult current
collector health or regenerate quality. Existing datasets retain their original
pinned interpretation.

## Consequences

No rows or samples are manufactured, no sampling alignment is relaxed, and dense
candle/trade-flow contracts remain unchanged. The projection adds bounded archive
reads during planning and freezing. Limits fail explicitly rather than silently
accepting partial evidence. It does not backfill a generic coverage ledger, add a
new collector framework, or implement hypothetical data families. If measured
usage outgrows these bounds, a producer-owned persisted summary can be evaluated
separately without teaching research transport semantics.
