---
component: market-structure-phase4-normalization-datasets
subsystem: data
layer: operations
doc_type: architecture
status: historical
tags:
  - market-data
  - market-structure
  - normalization
  - datasets
  - backtests
  - known-at
  - causality
  - replay
code_paths:
  - src/market_data/fact_registry.py
  - src/market_data/normalization.py
  - src/market_data/backtest.py
  - src/market_data/contracts.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market/normalization_service.py
  - portal/backend/service/market/backtest_dataset_service.py
  - portal/backend/service/market/runtime_market_data.py
  - portal/backend/service/storage/repos/normalization.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/controller/market_data.py
  - portal/backend/controller/candles.py
  - cli/main.py
  - tests/test_market_data/test_normalization_phase4.py
  - tests/test_market_data/test_normalization_service_phase4.py
  - tests/test_market_data/test_market_structure_repository_db.py
  - tests/test_market_data/test_repository_db.py
  - tests/test_market_data/test_runtime_delivery.py
---
# Market Structure Phase 4 Normalization And Frozen Datasets

## Status And Boundary

Phase 4 is implemented and accepted for bounded market-structure normalization,
typed dataset freezing, and provider-free delivery. It does not authorize
production collector enrollment. Stream definitions remain disabled and
`production_admitted=false`.

The one-hour implemented-path proof is sufficient to close the implementation
campaign because it exercised acquisition, archive acknowledgement, typed
facts, causal normalization, repeat comparison, exact-source freezing, archive
verification, and deterministic dataset identity. The 24-hour capacity run and
explicit storage/cost budget approval remain mandatory post-Phase 4 gates before
production enrollment.

## Implemented Contracts

### Fact registry

`src/market_data/fact_registry.py` is the single typed planning contract for
candles, OI, funding, trades, aggregates, BBO, depth, flow, basis, derivative
state, response, and normalized facts. It defines timeframe behavior, record
time, archive policy, and dataset eligibility. Raw reconstructed L2 book state
is deliberately not a dataset fact surface; replayable derived book facts are.

Normalized series use
`market.normalized_feature.v1/<normalization-spec-id>`. The spec identifier is
`nsp_` plus the first 31 hexadecimal characters of the full SHA-256 spec hash;
the complete hash is persisted and verified.

### Immutable normalization specifications

`market.normalization_specs` stores one immutable executable semantic contract.
A semantic change creates a new semantic version and hash. The installed v1
catalog is version `1.0.1` and contains:

| Feature | Source | Formula | Causal history |
|---|---|---|---|
| funding rate bps | funding rate | `rate * 10,000` | current observation |
| funding percentile | funding rate | prior-only empirical percentile | 30 days |
| funding z-score | funding rate | prior-only population z-score | 30 days |
| relative notional by time of day | trade flow | current / prior same-minute median | 28 days |
| aggressive buy share | trade-flow feature | buy notional / total notional | current observation |
| volatility-adjusted return | candle | return / prior-only realized volatility | 60 minutes |

Rolling calculations never shorten their approved warmup to manufacture a
value. Missing or unusable input emits a typed null with one of
`insufficient_history`, `invalid_input`, `zero_denominator`, or `zero_variance`.
Predicted-funding evolution remains unsupported because the provider meaning is
not yet proven.

### Materialized normalized facts

Normalized features use append-only canonical `market.fact_versions` and share
`market.fact_commit_seq`; payloads follow the generalized hot/cold storage
boundary, not a separate normalized version table. Each row records effective time, known-at time,
status/value, input range/count, input-only watermark, source series, up to
three bounded witness hashes, the full input fingerprint, provenance, quality,
and revision.

The bounded witness list is diagnostic evidence, not the complete identity.
The complete ordered input material, spec hash, timing, and quality state remain
inside the SHA-256 input fingerprint.

Two watermarks have distinct meanings:

- `selection_watermark` fixes the database view used to select source facts and
  later-known gap evidence for one materialization request.
- `source_watermark` is the maximum commit sequence of selected source facts
  only and participates in the normalized input fingerprint.

Output commits therefore cannot change their own future input identity. Gap
evidence is causal at its detection/creation time; it invalidates a
materialization only when it was knowable by the requested decision time.

Source-witness lookup and latest-output checks use verified payloads from either
tier. Cooling does not weaken the older-watermark/known-at rejection, permit
contradictory fingerprints at one watermark, or rematerialize identical inputs.
Alias catalog entries locate candidates but cannot prove a witness without its
actual payload. Historical unindexed provenance and retired-spec reference
checks retain their existing meaning through logged, batched cold scans; these
rare paths are not constant-cost lookups. Destructive retention of normalized
outputs still requires separate complete input-window dependency admission.

### Frozen dataset boundary

`market.dataset_normalization_refs` binds a frozen normalized output to its
spec, exact input range/count/watermark/fingerprint, output material,
provenance, quality, and all source-series fingerprints. A normalized series is
not independently freezeable: its transitive source series and exact covering
range must be present in the same dataset.

Source facts that require raw evidence also bind
`market.dataset_archive_refs`. Freeze and retrieval verify the referenced local
object bytes and SHA-256, rather than trusting database metadata alone. Dataset
identity is a deterministic hash over the resolved typed series, revisions,
quality, provenance, archive references, normalization references, and request
metadata. Repeating an identical freeze is idempotent.

The current generalized trade/flow history contract is defined in
[Generalized Fact Data Plane](GENERALIZED_FACT_DATA_PLANE.md#exact-raw-revision-evidence).
It preserves canonical corrections, invalidations and historical partial flags
across hot/cold storage, rather than relabeling older latest-only datasets.
Validation checks the exact frozen quality document once. Re-appending its
already-recorded typed quality notes caused false hash disagreements; the
validator no longer regenerates or duplicates them.

The existing dataset endpoint accepts either a legacy candle selector
(`instrument_id` plus `timeframe`) or an exact typed `series_id`; mixing both is
rejected. This preserves old callers while making the same frozen-data boundary
available to all registered fact types.

## Runtime And Backtest Delivery

Dataset reads resolve typed rows only at the manifest's commit watermark and
verify normalization/archive lineage before delivery. Backtests and research do
not call Coinbase, reconstruct mutable books, or read current operational
normalization tables.

Runtime may read frozen typed facts from an admitted dataset. Mutable runtime
delivery remains intentionally limited to the previously approved OI and
funding paths. Normalized facts require a frozen dataset.

## Operator Surface

Phase 4 extends the existing API-backed `qt` workflow:

```text
qt data market-structure normalization-specs-install --approved-by <operator>
qt data market-structure normalization-specs
qt data market-structure normalization-materialize <spec-id> <series-id> ...
qt data market-structure normalization-compare <spec-id> <series-id> ...
qt data freeze-dataset --request-json '{"series":[{"series_id":...}]...}'
qt data dataset <dataset-id>
```

The API and CLI expose explicit typed status, watermarks, no-op/insert counts,
fingerprints, archive references, and provider-call evidence. The frontend is
not workflow truth.

## 2026-08-02 Live Implemented-Path Proof

The bounded authenticated BIP trade proof used session
`mss_9afa24ff39bb4c48a114011a2ad7466a` for approximately 81 seconds.

| Measure | Evidence |
|---|---|
| Requested / elapsed capture | 75 s / 81.385 s |
| Raw records / canonical trades | 86 / 115 |
| New archive manifests / mapping lag | 1 / 0 |
| Trade coverage | closed valid, contiguous, archive complete |
| Source flow series | `94`, five valid one-second facts |
| Normalized output series | `220`, five valid aggressive-buy-share facts |
| Source input watermark | `11485` |
| Normalization comparison | two exact repeats, five no-ops each |
| Normalization fingerprint | `ca53ecc92eba151da20f30e48fd989539d26648a9ec710c9d6f2b53898e397c8` |
| Provider calls during normalization | false |

The source and normalized ranges were frozen together twice. Both requests
returned dataset `mds_9f1975e270d8fe6b9b69859066a6904f` with hash
`9f1975e270d8fe6b9b69859066a6904fab5a0ab296c3656f246c81b63d8ea0e9`.
Retrieval reverified raw archive manifest
`ram_14f65ffb10f1ca104187a84be610b22bbe99112a71c9b8010c2ed7e5e2646c45`,
its object checksum, the normalized spec, and the exact source fingerprint.

The capture did not span a complete wall-clock minute after subscription
alignment, so it intentionally proves the one-second path only. Full rolling
30-day and 28-day features were tested with deterministic fixtures and retain
their complete causal warmup requirements.

## Correctness Evidence

- Pure tests cover prior-only percentile, z-score, time-of-day median, ratio,
  basis-point, and volatility transforms, explicit warmup/null states,
  truncation invariance, and stable spec/material hashes.
- Service tests cover source selection, later-known gaps, bounded witnesses,
  selection-versus-source watermark separation, idempotent persistence, and
  compare-without-provider behavior.
- PostgreSQL integration tests cover funding normalization, later funding
  corrections, immutable earlier/frozen values, transitive typed dataset
  freeze, provider-free replay, compaction/retention invariance, and archive
  pins.
- API/CLI tests cover spec install/list/materialize/compare, typed series
  discovery, exact-series freezing, ambiguous request rejection, and legacy
  candle compatibility.

## Remaining Production Gates

Phase 4 closes the implementation campaign. Before any definition becomes
production-admitted:

1. Run the planned 24-hour implemented-path BIP/BTC capture, including a
   representative high-volume period.
2. Measure event rate, raw/canonical bytes, compression, CPU, replay speed,
   spool backlog, upload lag, and recovery behavior.
3. Apply the documented 3x observed-p99 safety factor and approve explicit
   storage, retention, and operating budgets.
4. Repeat admission separately before ETP/ETH or SLP/SOL enrollment.

Until those gates pass, the implementation remains operable only through
bounded, explicitly invoked proof sessions.
