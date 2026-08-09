---
component: research-replay-availability
subsystem: research-orchestration
layer: boundary
doc_type: architecture
status: active
tags:
  - research
  - replay
  - causality
  - datasets
  - holdout
code_paths:
  - src/research_science/study.py
  - src/research_science/temporal.py
  - src/research_science/replay_availability.py
  - portal/backend/service/storage/repos/market_structure.py
  - tests/test_research_science/test_study.py
  - tests/test_research_science/test_temporal.py
  - tests/test_research_science/test_replay_availability.py
---
# Research Replay Availability

## Purpose

Canonical `known_at` records when QT actually accepted and published a fact. It
is never moved backward. A continuously running pinned transform may have been
able to derive the same material earlier than a bounded-session finalizer. A
study may make that separate counterfactual claim only through an immutable
`AvailabilityTransform` and exact frozen evidence.

```text
StudyDefinition
  -> FactRequirement[]
  -> AvailabilityTransform[]
  -> TemporalJoinSpec[]
  -> exact registered resolvers
  -> provider-free frozen input derivation
  -> availability binding hashes pinned by ResearchRun
```

Availability is not a timestamp rewrite, provider event-time shortcut, or
permission to fetch missing history.

## Trade-flow receipt replay

`research.trade_flow_replay.v1` is one specialized transform in the generic
registry. It supports 60-second trade-flow facts and pins:

- frozen receipt time as the delivery clock;
- `market.trade_flow.receipt_replay.v1` as the transform;
- `first_subsequent_covered_trade.v1` as the watermark;
- deterministic processing latency; and
- complete coverage, archive, and canonicalization evidence.

For each bucket, QT loads the exact coverage revision named by the aggregate,
reconstructs it from frozen receipt-normalized trades, and requires both the
material hash and input fingerprint to match. A later covered trade must prove
the source advanced beyond the bucket. Availability is:

```text
max(bucket_end, last_in_bucket_receipt, watermark_receipt)
  + processing_latency
```

The same rule applies to zero-trade buckets. A missing watermark excludes the
bucket; incomplete/tampered coverage or raw/aggregate disagreement rejects the
binding.

## Generic causal joins

Trade flow does not imply OI, funding, price, book, or any other contextual
fact. The study declares each requirement and join. `TemporalJoinSpec` selects
the latest fact whose event, sample, and canonical known-at clocks are all no
later than the primary frame decision time. Its missing policy is explicit:
reject, exclude, or null. Prefix-invariance tests prove that appending future
facts cannot change prior frames.

The same contracts support a price-and-volume study with no raw trade, OI, or
funding dependency. New availability sources require new registered transform
versions and cannot silently change existing run evidence.

## Historical record

The terminal BTC perpetual V3 dossier is retained as a postmortem of an invalid
input contract. Its executable definition and runner were deleted, not migrated
or translated. Immutable generic protocol/family/trial evidence remains in the
research authority database.
