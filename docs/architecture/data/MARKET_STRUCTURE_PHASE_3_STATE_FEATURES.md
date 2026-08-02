---
component: market-structure-phase3-state-features
subsystem: data
layer: operations
doc_type: architecture
status: active
tags:
  - market-data
  - market-structure
  - features
  - causality
  - replay
  - open-interest
  - funding
  - operations
code_paths:
  - src/market_data/market_state.py
  - src/market_data/order_book.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market/market_structure_service.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - docker/docker-compose.yml
  - tests/test_market_data/test_market_state_phase3.py
  - tests/test_market_data/test_market_structure_repository_db.py
  - tests/test_market_data/test_market_structure_service.py
  - tests/test_portal/test_market_structure_routes.py
  - tests/test_cli/test_market_data_cli.py
---
# Market Structure Phase 3 State Features

## Status And Boundary

Phase 3 is implemented and accepted for bounded BIP/BTC operational market-state
features. It does not authorize production enrollment or canonical backtest
delivery. All stream definitions remain disabled and
`production_admitted=false`. Immutable normalization specifications, frozen
feature datasets, runtime delivery, and the post-Phase-4 24-hour capacity and
budget gate remain Phase 4 or later work.

The implementation uses only typed Phase 1–2 facts plus existing OI/funding
facts. It does not call Coinbase while materializing or replaying features and
does not infer native CDE fields.

## Implemented Typed Facts

| Contract/table | Grain | Source and causal rule | Suppression rule |
|---|---|---|---|
| `market.bbo_feature.v1` / `market.bbo_feature_versions` | series × one-second bucket | last complete valid book state in the bucket; exact state hash, source position, validity interval, units, and input fingerprint | invalid book or unknown valuation contract |
| `market.depth_feature.v1` / `market.depth_feature_versions` | series × bucket × 5/10/25 bps band | exact BBO source plus same valid state; bid/ask provider, base, and notional depth with bounded imbalance | invalid book, empty denominator, or unknown conversion |
| `market.trade_flow_feature.v1` / `market.trade_flow_feature_versions` | series × 1s/1m bucket | one complete typed trade-flow aggregate; maker side is explicitly inverted to aggressor side under the proven Coinbase contract | incomplete coverage, zero trades, or zero flow denominator |
| `market.futures_spot_basis.v1` / `market.futures_spot_relationship_versions` | mapping × futures effective second | futures BBO aligned to only the last spot BBO at or before it, with each side no more than two seconds stale | missing, future, stale, or invalid either-side BBO |
| `market.derivative_state.v1` / `market.derivative_state_versions` | futures instrument × causal observation | exact OI and provider-reported funding records, source series, sample times, commit sequences, and input fingerprint | OI log change is null across an intersecting gap or without two positive consecutive observations |
| `market.market_response.v1` / `market.market_response_feature_versions` | series × direction × horizon × effective time | flow plus pre/trough/post valid book positions; buys consume/replenish asks and sells consume/replenish bids | mixed/absent flow, cross-validity book positions, gaps, or unavailable horizon state |

Every table is append-only, uses `market.fact_commit_seq`, has an immutability
trigger, and selects revisions through known-at plus optional commit watermark.
Exact typed source material must exist before a feature revision is accepted.
An identical identity/material pair is a no-op; changed material appends a
later revision.

## Deterministic Operational Path

```text
acknowledged raw trades -> typed complete 1s/1m aggregate -> flow/CVD fact
acknowledged raw L2 -> valid deterministic state -> BBO + 5/10/25 bps depth
                                                -> directional response fact
futures BBO + prior-only spot BBO -> basis fact
typed OI + funding + OI gap evidence -> derivative-state fact
same raw + persisted transport-quality timeline -> replayed states/features
```

Live reconstruction passes each valid state through the same pure feature
functions used by replay. Replay applies invalidating quality events at the
stored `(connection_epoch, receive_ordinal, event_ordinal)` before any L2 event
at that position. Persist-only augmentation fields are removed before reducer
hashing. Heartbeat gaps, sequence gaps, disconnects, and decode errors therefore
reproduce the same validity closure, final null/valid state, feature suppression,
and fingerprint as live ingestion.

Cross-stream materialization uses an input-only commit watermark bounded by
the requested time/known-at window and the exact futures/spot BBO, OI, funding,
and gap series. Basis or derivative-state output revisions cannot advance their
own source watermark. Repeating the same request without new eligible inputs is
a real no-op with a stable fingerprint.

## Local Archive Durability Correction

The Phase 3 scrub recreated the backend and discovered that the original
filesystem object root was on the container writable layer. PostgreSQL
manifests survived while their object bytes did not. Replay failed loudly with
`market_archive_object_missing`; no bytes or completion claims were fabricated.

The local stack now mounts named volume `market-structure-data` at
`/app/logs/market-structure` in both backend and collector services. This makes
the current filesystem spool/object/checkpoint boundary survive service
replacement and gives both processes one authority. It is a bounded local
deployment contract, not a cloud-object-store claim. Pre-correction manifests
remain unreplayable and cannot qualify a Phase 4 frozen dataset.

## 2026-08-02 Live Implemented-Path Proof

### Valid BIP capture and restart replay

| Measure | Evidence |
|---|---|
| Session | `mss_c573ffbb336248249518e82167637c0a` |
| Raw manifest | `ram_1e4b25aad1af6c6a3a54ac5be6073a0762ff561684af95c696ebcab84dbb5d82` |
| Raw records / source bytes | 43 / 155,308 |
| Snapshot / mutation batches / mutations | 1 / 25 / 77 |
| BBO / depth features | 11 / 33 |
| Validity | closed valid |
| Raw mapping lag / spool backlog | 0 / 0 |
| Final state hash | `22da83f01ba7d35242606a233d2970d69cfb1717b08937ae06259d313843ffdf` |
| Replay fingerprint before restart | `eb4f1dd11a93ae16f2f144a56e1e55197ef1584f7f93c9c67a84c3cc07895c54` |
| Replay fingerprint after restart | same |
| Full reconciliation / checkpoint-plus-delta / persisted features | equal / equal / equal |

### Invalid BTC capture and transport-quality replay

| Measure | Evidence |
|---|---|
| Session | `mss_7dacb7ff16c34bddb586bfa3552efe0b` |
| Raw manifest | `ram_a7352f4b061f29924ca0779056b4d5c98a2eade2d53eaf21e2504fe116c3d8f2` |
| Raw records | 101 |
| Snapshot / mutation batches | 1 / 92 |
| Replayed BBO / depth features | 6 / 18 |
| Terminal state | invalid; final state hash null |
| Replayed invalidations | 1 heartbeat gap |
| Replay fingerprint | `15a81f69b0e8e08f8a85304c58b8150d5e2b06fe723a958772ccecf6539780ff` |
| Full reconciliation / checkpoint-plus-delta / persisted features | equal / equal / equal |

The BTC result is acceptance evidence because the source defect remains visible,
the final book is invalid, and no features are emitted after invalidation. It is
not represented as a clean session.

### Cross-stream idempotency

For pair `bip_btc`, window `14:40:00Z..15:00:00Z`, and decision time
`15:00:00Z`, two consecutive materializations each selected input watermark
`9410`, derived 16 basis and 20 derivative-state facts, inserted zero rows,
reported 36 no-ops, and returned fingerprint
`057a9923af74b6d1ff378d964a4e4b06f44e6b65c507d0e9f9941a1f1e4cb184`.

That window includes pre-volume BBO evidence. It proves typed transformation,
source verification, causal selection, and idempotency, but it is deliberately
not Phase 4 dataset acceptance evidence because the old raw objects are gone.
Phase 4 must freeze a wholly post-correction input range.

## Verification

- 24 focused market-state/service/API/CLI tests pass.
- 2 opt-in real-PostgreSQL repository tests pass, including typed source
  enforcement, duplicate no-op behavior, shared commit sequence, and bounded
  input watermark SQL.
- Pure-feature tests cover replay equality, truncation invariance of earlier
  book output, flow reconciliation, incomplete suppression, prior-only basis,
  OI gap blocking, and direction-correct ask replenishment for aggressive buys.
- Live replay covers both valid closure and terminal transport invalidation.
- Backend replacement proves new raw/checkpoint objects remain readable.

## Remaining Phase 4 Gates

- immutable normalization spec and normalized-value tables;
- causal percentile, z-score, time-of-day baseline, ratio, bps, and volatility
  transforms with explicit warmup and minimum observations;
- typed dataset registry/planning for trades, aggregates, BBO, depth, flow,
  basis, derivative state, response, OI, funding, and candles;
- frozen normalized values plus spec/source/quality/archive fingerprints;
- provider-free research/backtest delivery and retention/compaction invariance;
- a wholly post-volume dataset proof;
- only after Phase 4, the required 24-hour implemented-path capacity and budget
  admission run.
