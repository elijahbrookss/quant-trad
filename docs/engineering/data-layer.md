# Data Layer

The data layer connects Quant-Trad to market-data providers while preserving one
canonical, auditable source-fact truth.

## Canonical Ownership

Provider adapters under `src/data_providers/` acquire and normalize external
responses. Typed contracts and requirement resolution live under
`src/market_data/`. Historical candle intake lives in
`portal/backend/service/market/feed_service.py`; scheduled producer
orchestration lives in `portal/backend/service/market/collector_service.py`.

PostgreSQL schema `market` is owned by
`portal/backend/service/storage/repos/market_data.py`. Durable collector
definitions, attempts, pacing, retries, and ownership leases are owned by
`portal/backend/service/storage/repos/market_collection.py`.

## Implemented Intake

Historical candles enter only through an explicit audited operation. Requests
are split into bounded provider segments, validated, and appended as
`candle.ohlcv.v1` revisions. Paper candles are persisted after closure and before
runtime visibility.

Coinbase venue-specific open interest enters through durable scheduled polling
as `derivatives.open_interest.v1`. The worker is restart-safe, bounded, and
database-fenced. Missed schedules and final failures remain gap evidence. Since
the provider exposes no OI event time through this adapter, sample time is the
collector schedule and known-at is platform acceptance after receipt.

Coinbase perpetual funding enters through the same durable scheduler as
`derivatives.funding_rate.v1`. It stores a signed fractional rate, the
provider-reported funding time, and the funding interval. Coinbase documents the
field but not its publication semantics, so funding time is preserved without
driving known-at; schedule and platform acceptance remain authoritative.

## Consumer And Dataset Reads

Reads never acquire. Consumers declare typed facts and explicit primary,
underlying, benchmark, or explicit instrument roles; they do not select provider
endpoints or storage. Underlying and benchmark roles resolve from canonical IDs
in immutable run configuration.

Candles, OI, and funding use append-only revisions and one shared market commit
clock.
Readers may pin commit and known-at cutoffs. Frozen datasets hash exact selected
material, provenance, and quality. Backtests freeze all transitive inputs before
execution, then resolve latest-known OI at every warmup and decision bar without
provider calls. Paper/runtime reads use the same causal and staleness rules
against current canonical storage.

Runtime separately hashes final derived candle/ATR frames, so source dataset
identity and actual execution input identity remain visible.

## Sparse Data, Corrections, And Legacy State

Sparse truth remains sparse. Quality and gap evidence never become synthetic
source facts. Corrections append revisions; accepted facts and evidence cannot
be updated or deleted.

Legacy candle tables are archived under `legacy_market_v1` for manual reasoning
only. Prior dataset manifests using the old provenance identity are archived by
the fact-clock migration. There is no application fallback reader or writer.

## Extending The Feed

Basis or another observation must define its own fact contract and
typed series semantics, then reuse source identity, ingestion, known-at,
provenance, quality, collector, and dataset concepts where those semantics fit.
New facts must not become optional columns on candle or OI tables. The current
collector handlers support Coinbase OI and funding. They do not imply historical
backfill, aggregation, basis, or strategy/indicator funding delivery.

## References

- [Data boundary](../architecture/data/DATA_BOUNDARY.md)
- [Canonical market-data ADR](../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md)
- [Frozen backtest ADR](../architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md)
- [Typed collector ADR](../architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md)
- [Coinbase OI collector guide](../guides/coinbase-open-interest-collector.md)
- [Coinbase funding collector guide](../guides/coinbase-funding-rate-collector.md)
- [Runtime contract](../contracts/platform/01_runtime_contract.md)
