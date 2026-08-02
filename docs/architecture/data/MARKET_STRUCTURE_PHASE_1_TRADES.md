---
component: market-structure-phase1-trades
subsystem: data
layer: operations
doc_type: architecture
status: active
tags:
  - market-data
  - market-structure
  - trades
  - raw-archive
  - replay
  - datasets
  - operations
code_paths:
  - src/data_providers/streams/coinbase.py
  - src/market_data/archive.py
  - src/market_data/structure.py
  - src/market_data/contracts.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market/market_structure_service.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - tests/test_market_data/test_market_structure_phase1.py
  - tests/test_market_data/test_market_structure_archive.py
  - tests/test_market_data/test_market_structure_service.py
  - tests/test_market_data/test_market_structure_repository_db.py
  - tests/test_portal/test_market_structure_routes.py
  - tests/test_cli/test_market_data_cli.py
---
# Market Structure Phase 1 Trades

## Status And Boundary

Phase 1 is implemented and accepted for bounded BIP/BTC capture and research
dataset validation. It is not a production collector admission. Every stream
definition remains disabled and `production_admitted=false`; the implemented
24-hour capacity proof and explicit storage/cost budget remain mandatory after
Phase 4.

The implementation uses Coinbase Advanced Trade through the existing provider
and credential registry. Stream definitions contain no secrets or credential
references. The initial allowlist is BIP/BTC, ETP/ETH, and SLP/SOL, but only
BIP/BTC has completed the Phase 1 live implemented-path proof.

## Implemented Path

```text
Advanced Trade WebSocket
  -> fenced bounded stream session
  -> fsynced definition-scoped local spool
  -> deterministic Parquet/ZSTD archive object
  -> verified object acknowledgement + PostgreSQL manifest/mappings
  -> typed trade revisions + trade coverage intervals
  -> causal 1-second/1-minute flow revisions
  -> frozen dataset + raw archive retention pins
  -> provider-free typed dataset reads
```

One product owns one WebSocket connection. `market_trades` and `heartbeats`
share the provider's observed connection sequence. The collector never copies
credentials and cannot be claimed as a continuous production stream until the
post-Phase-4 gate changes both its admission and enabled state.

## Identity, Durability, And Publication

- `spool_segment_id` is deterministic from definition, logical session,
  connection epoch, and segment ordinal.
- `raw_record_id` is deterministic from definition, session, epoch, receive
  ordinal, and exact frame SHA-256. It exists before parsing or upload.
- Each raw frame is fsynced before parsing. A sealed spool remains recoverable
  after an upload failure or process crash.
- Archive objects are immutable Parquet/ZSTD files containing exact binary
  frames. Object bytes and replay identities are verified before a PostgreSQL
  manifest is committed.
- Canonical trade publication currently waits for the acknowledged
  manifest-to-record mapping. Facts reference `raw_record_id` and are never
  mutated to attach a later manifest.
- A spool segment is discarded only after object verification and the database
  manifest/mapping transaction. Its small acknowledgement sidecar remains as
  local recovery evidence.
- Backlog limits and status metrics are scoped by stream definition, so
  concurrent BIP and BTC sessions do not consume or report each other's budget.

## Typed Trade Contract

`market.trade.v1` preserves Coinbase `side` as maker side and derives aggressor
side only with `coinbase_maker_to_aggressor.v1`. The Phase 0 fixture and live
proof establish the supported unit translations:

| Product | Provider size | Base quantity | Quote notional |
|---|---|---|---|
| BIP-20DEC30-CDE | integral contracts | contracts x 0.01 BTC | price x base quantity |
| BTC-USD | BTC base quantity | provider size | price x base quantity |

Provider product/trade ID is the stable natural identity. Same-ID/same-material
redelivery is a no-op. Same-ID/different-material delivery fails and emits
typed quality evidence. Snapshot deliveries are preserved as canonical trade
evidence but are not treated as proof of complete live flow.

## Trade Coverage And Aggregates

A trade coverage interval opens only after all three are observed on one
connection epoch:

1. the `market_trades` subscription acknowledgement;
2. a healthy heartbeat;
3. the initial market-trade snapshot.

The first and last source positions, exact connection sequence assurance,
archive watermark, canonicalization watermark, closure evidence, gaps, and
known-at time are append-only typed facts. A zero-trade bucket is complete only
when its entire half-open event bucket is inside a closed-valid, contiguous,
archive-complete, canonicalization-complete interval with no invalidating gap.

Flow uses only current-session update deliveries carrying that coverage
identity. Initial snapshot history remains queryable as trades but cannot be
rolled into live flow. Edge buckets or invalid intervals may be retained as
explicit incomplete revisions; they cannot silently appear complete.

`market.trade_flow.v1` materializes one-second and one-minute counts, maker and
aggressor volumes, contract/base/quote volume, OHLC, and CVD delta with exact
Decimal arithmetic and deterministic provider-ID deduplication.

## Dataset Eligibility

The existing `market.datasets` and `market.dataset_series` boundary now freezes
raw trades and trade-flow aggregates. A trade is eligible only when its
`raw_record_id` has an acknowledged archive mapping. A flow series is eligible
only when every selected revision is archive- and canonicalization-complete and
has typed coverage identity. Complete versus incomplete coverage remains in the
quality hash and summary.

Each frozen market-structure dataset inserts immutable
`market.dataset_archive_refs` pins with the archive object checksum and content
fingerprint. `read_dataset_series` resolves the frozen commit watermark and
returns typed records without a provider call.

## Operator Surface

The supported API-backed CLI operations are:

```bash
qt data market-structure configure-pair --pair bip_btc --auth-mode authenticated
qt data market-structure definitions
qt data market-structure sessions --definition-id ms_coinbase_btc_usd
qt data market-structure status ms_coinbase_btc_usd
qt data market-structure capture ms_coinbase_btc_usd --duration 60
qt data market-structure replay <manifest_id>
qt data market-structure reconcile-recent ms_coinbase_btc_usd --limit 100
```

`status` reports manifest bytes/records, archive mapping lag, canonical trade
count, complete/incomplete aggregate counts, coverage and quality intervals,
frozen dataset ranges, configured spool/segment limits, and production
blockers. `replay` verifies object SHA-256, exact frame order, replay
fingerprint, raw mapping completeness, and canonical provider-trade identity
coverage.

Recent REST reconciliation is deliberately only a bounded overlap diagnostic.
REST-only IDs can occur after a capture and are not treated as gaps or evidence
of historical completeness.

## 2026-08-02 Live Implemented-Path Proof

The final reconnect-safe concurrent proof used authenticated definitions and a
10-second requested window per product after an earlier 30-second scrub exposed
and fixed cross-definition backlog reporting and snapshot aggregation pressure.

| Measure | BIP futures | BTC spot |
|---|---:|---:|
| Raw frames | 14 | 19 |
| Replay trades | 100 | 106 |
| Snapshot / update trades | 100 / 0 | 100 / 6 |
| Raw mapping lag after commit | 0 | 0 |
| Definition-scoped spool backlog | 0 | 0 |
| Closed-valid coverage | yes | yes |
| Invalidating quality events | 0 | 0 |
| Complete 1-second buckets frozen | 10 | 9 |

Archive and replay evidence:

| Product | Manifest | Object SHA-256 | Replay fingerprint | Canonical reconciliation |
|---|---|---|---|---|
| BIP | `ram_dd9cb4075fdde7a0ccaedf3251e5fe65289e469316b76c125b69c59c6e7ef06d` | `8773b7b5b075f713e87229791fa8fd8ce7f393880072b58848ba38abe167a0e4` | `d68e3ce3e52d9d71cac4e27c2a3b599d210da83f9ed16ff366a593848a3cb432` | 14/14 raw frames mapped; 100/100 unique trades present |
| BTC | `ram_029aa386812c3560810acc1e955b4a6980bfee6c93b7b8bc05056cd196c9dd16` | `da4a7941f2a39dd08465108efcf7f1b84288141e8ac393ea2d0d458eaaa00898` | `0bb2d7408a182df3da93d5028e671c1cb2bd0ea6d8fd80adf8812dde57d2c9e5` | 19/19 raw frames mapped; 106/106 unique trades present |

The provider-free flow dataset is
`mds_41085052f22068edb2f541bb67457ad3`, with full hash
`41085052f22068edb2f541bb67457ad3e2cb3f1837bb468150e43fed682496ed`.
It contains ten BIP and nine BTC complete one-second rows and pins both raw
archive objects above.

The proof is sufficient for Phase 2 implementation. It is not a production
capacity report and does not replace the deferred 24-hour gate.

## Verification

The Phase 1 slice proves:

- pre-upload raw identity and crash-tail recovery;
- object checksum, immutable reuse, failed-upload recovery, and DB-ack cleanup;
- ownership fencing and duplicate trade idempotency;
- maker/aggressor and futures/spot unit semantics;
- zero-trade versus incomplete-stream distinction;
- deterministic 1-second/1-minute aggregation;
- typed API/CLI operations;
- stable double-freeze identity, archive pins, and provider-free reads;
- live BIP/BTC raw-to-canonical replay reconciliation.

Production enrollment, Level 2, book reconstruction, Phase 3 features, Phase 4
normalization/runtime delivery, and the 24-hour admission measurement remain
outside this phase.
