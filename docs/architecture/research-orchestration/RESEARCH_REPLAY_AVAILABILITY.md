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
  - admission
  - holdout
code_paths:
  - src/research_science/replay_availability.py
  - src/research_science/autonomous_campaign.py
  - portal/backend/service/research/campaign_runner.py
  - portal/backend/service/storage/repos/market_structure.py
  - tests/test_research_science/test_replay_availability.py
  - tests/test_research_science/test_autonomous_campaign.py
---
# Research Replay Availability

## Purpose

Canonical `known_at` records when QT actually accepted and published a fact. It
is never moved backward. A continuously running research transform may,
however, have been able to derive the same fact earlier than a later bounded
session finalizer did. Research replay represents that separate claim with an
explicit, immutable availability artifact.

```text
frozen raw trade receipts
  + exact frozen aggregate
  + exact immutable coverage revision
  + pinned transform and watermark policy
  + deterministic processing latency
  -> replay_available_at
  -> causal cross-fact joins
  -> campaign features and opportunities
```

This is a counterfactual research replay certificate. It is not a correction
to canonical history and does not claim that the batch aggregate was actually
published at `replay_available_at`.

## V1 availability rule

`research_replay_availability.v1` supports 60-second trade-flow aggregates.
Every campaign dataset must freeze the raw `market.trade` series alongside
`market.trade_flow`, open interest, and funding. The policy pins:

- receipt-time availability as the only replay delivery clock;
- `market.trade_flow.receipt_replay.v1` as the transform;
- `first_subsequent_covered_trade.v1` as the source watermark;
- an explicit deterministic processing latency; and
- mandatory complete coverage, archive, and canonicalization evidence.

For each bucket, QT loads the exact coverage interval and revision named by the
aggregate, reconstructs the aggregate from receipt-normalized frozen trades,
and requires the material hash and input fingerprint to match. A later covered
trade must have a provider event time at or beyond the bucket end and a receipt
position after every in-bucket trade. Availability is:

```text
max(bucket_end, last_in_bucket_receipt, watermark_receipt)
  + processing_latency
```

The same watermark rule applies to zero-trade buckets. Absence of trades alone
never proves a zero. A bucket without a qualifying frozen watermark is excluded
with `source_watermark_unavailable`; incomplete/tampered coverage or raw versus
aggregate disagreement fails the entire binding.

## Hash and lineage boundary

The replay semantic hash includes receipt evidence, provider event identity,
raw record identity, coverage material, aggregate material, the watermark,
the policy, and the derived availability. It deliberately excludes canonical
acceptance and `known_at` timestamps, so delayed batch publication cannot alter
the replay semantics.

The campaign replay binding hash then adds the frozen dataset ID/hash, dataset
manifest hash, and raw/aggregate series material and provenance. Train,
validation, and sealed holdout binding hashes are pinned in the immutable
scientific protocol. Attempt feature hashes bind the actual derived bars.

## Causal joins and campaign admission

The replay decision time is the only as-of time used to select open-interest
and funding facts. Both `sample_time` and canonical `known_at` must be no later
than that decision time. The selected facts and replay evidence flow into the
bar source hashes.

Before protocol activation or attempt allocation, QT derives actual causal
entry/exit opportunities for train, every validation fold, and the private
holdout. Admission enforces the declared sample, signal-capable trade, calendar,
exposure, and horizon floors. Public preflight evidence reports train and
validation counts, but the holdout reports only pass/fail and never its binding,
row count, opportunity count, fold evidence, or metrics.

## Version and migration policy

`autonomous_research_campaign.v1` charters remain readable as immutable
historical evidence. They cannot execute again. A replay-correct campaign needs
a new identity and `autonomous_research_campaign.v2` charter with the explicit
policy and raw trade fact in every frozen role dataset. No V1/V2/V3 campaign
record, protocol, attempt, or terminal governance state is rewritten.

Future watermark sources such as replay-certified heartbeat or book clocks
must be new policy/transform versions. They must not silently change V1.
