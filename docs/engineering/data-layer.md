# Data Layer

The data layer connects Quant-Trad to market-data providers while preserving one
canonical, auditable source-fact truth.

## Canonical Ownership

Provider adapters under `src/data_providers/` acquire and normalize external API
responses. They do not persist, cache, repair, or serve consumer reads.

Typed facts and identity contracts live under `src/market_data/`. The explicit
intake and provider-free read boundary lives in
`portal/backend/service/market/feed_service.py`; PostgreSQL ownership lives in
`portal/backend/service/storage/repos/market_data.py` and schema `market`.

## Candle Intake

Historical candles enter only through an explicit audited CLI or API operation.
The request is split into bounded provider segments, validated, and appended as
`candle.ohlcv.v1` revisions. Empty segments, exceptions, and coverage gaps remain
structured evidence.

Paper candles are persisted after closure and before runtime visibility. Replay,
research, reports, indicators, and backtests read the same canonical revisions;
a read miss never calls a provider.

## Causal And Dataset Reads

Every candle has an explicit close time, known-at value and method, acceptance
time, source identity, ingestion operation, exact hash, revision, and commit
sequence. Readers use half-open windows and may pin a commit watermark and
known-at cutoff. Backtests pin one watermark across nested reads.

Frozen dataset manifests hash exact selected candle material, provenance, and
quality evidence. Runtime separately hashes the final derived candle/ATR frames
it consumed, so source identity and execution input identity are both visible.

## Sparse Data And Corrections

Sparse truth remains sparse. Gap evidence is stored separately from candle
material and is never replaced by synthetic OHLCV. Corrections append revisions;
accepted facts and evidence cannot be updated or deleted.

## Legacy Data

The market-data v2 hard-cutover migration verifies and copies usable legacy
candles, then archives old tables under `legacy_market_v1`. That archive is a
snapshot for manual reasoning only. There is no application fallback logic or
supported legacy reader/writer path.

## Extending The Feed

New observations such as open interest or basis should define a new fact
contract and typed series semantics, then reuse source identity, ingestion,
known-at, provenance, quality, and dataset concepts. They should not become
optional columns on the candle table. No additional fact type is implemented or
claimed yet.

## References

- [Data boundary](../architecture/data/DATA_BOUNDARY.md)
- [Canonical market-data ADR](../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md)
- [Adding a provider](../guides/adding-a-provider.md)
- [Runtime contract](../contracts/platform/01_runtime_contract.md)
