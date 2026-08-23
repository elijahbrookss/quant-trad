# Data Layer

The data layer connects Quant-Trad to market-data providers while preserving one
canonical, auditable source-fact truth.

## Canonical Ownership

Provider adapters under `src/data_providers/` acquire and normalize external
responses. Typed contracts, exact numeric meaning, dimensions, and requirement
resolution live under `src/market_data/`. Historical candle intake lives in
`portal/backend/service/market/feed_service.py`; scheduled producer
orchestration lives in `portal/backend/service/market/collector_service.py`;
explicit numeric acquisition lives in
`portal/backend/service/market/numeric_fact_acquisition.py`.

PostgreSQL schema `market` is owned by
`portal/backend/service/storage/repos/market_data.py`. Durable collector
definitions, attempts, pacing, retries, and ownership leases are owned by
`portal/backend/service/storage/repos/market_collection.py`. Providers do not
own schema, canonical series, coverage cache policy, or frozen datasets.

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

Provider-neutral exact scalar facts enter through a strict
`market.numeric_fact_sources.v1` manifest and an explicitly authorized bounded
operation. `market.reference_price.v1` and `market.reserve_balance.v1` retain
unbounded exact decimal material, unchanged raw provider value, contract-owned
unit/dimensions, causal clocks, source-event identity/material, provenance, and
append-only revisions in `market.numeric_fact_versions`. Binary floats are
rejected before canonicalization.

The first exact-numeric adapter is `chainlink_aggregator_v3.v1`. It performs
public read-only, phase-aware EVM log acquisition with chain/feed validation,
round reconciliation, explicit confirmation depth, bounded requests/logs/blocks,
and typed gap evidence. Checked-in ETH/USD and TUSD-reserves manifests are
disabled references. No wallet, signer, transaction, continuous collector, or
consumer fallback is introduced.

## Consumer And Dataset Reads

Reads never acquire. Consumers declare typed facts, exact contract versions and
dimensions, and explicit primary, underlying, benchmark, or explicit instrument
roles; they do not select provider endpoints or storage. Underlying and
benchmark roles resolve from canonical IDs in immutable run configuration.

All typed facts use append-only revisions and one shared market commit clock.
Readers may pin commit and known-at cutoffs. Frozen datasets hash exact selected
material, provenance, and quality. Exact numeric material includes raw and
normalized values plus source-event material; acquisition coverage remains
separate evidence.

Backtest preparation fails on missing facts by default. It may invoke numeric
history only when `acquire_missing` and an explicit manifest/binding,
actor/reason authorization, and bounded budget are supplied. Matching complete
coverage, including a zero-event range, is reused without constructing a
provider. Partial/failed acquisition cannot satisfy a required freeze.
Backtests freeze all transitive inputs before execution and replay locally at the
admitted commit scope.

Runtime separately hashes final derived candle/ATR frames, so source dataset
identity and actual execution input identity remain visible.

## Sparse Data, Corrections, And Reorgs

Sparse truth remains sparse. Quality, acquisition coverage, and gap evidence
never become synthetic source facts. Corrections append revisions; accepted
facts and evidence cannot be updated or deleted. A changed source-event material
hash appends a correction even when numeric value and timestamps are unchanged.
Only a complete explicit repair may invalidate an active event that disappeared;
partial repair cannot prove absence.

Legacy candle tables are archived under `legacy_market_v1` for manual reasoning
only. Prior dataset manifests using the old provenance identity are archived by
the fact-clock migration. There is no application fallback reader or writer.

## Persistence Bootstrap

A clean database receives the complete current canonical Fact schema,
`market.fact_acquisition_coverage`, registry, indexes, and immutable triggers
atomically on first startup. No historical migration file is clean-install
input.

`manual_migration_numeric_fact_store_v1.sql` is retained as lineage for the
retired pre-canonical scalar store. An operator preserving a database that
predates the canonical cutover may use the reviewed offline migration
procedure; rebuilding from a new empty database is the other supported choice.
A current non-empty database is validated and never silently repaired.
`PG_DSN` remains the only persistence DSN.

## OI And Funding Consolidation Gate

The current result is **`NUMERIC_FACT_CONSOLIDATION_DEFERRED`**.

Coinbase OI/funding v1 convert provider decimal strings to binary floats and
store floats, so original exact text cannot be reconstructed. Funding interval
is row-scoped rather than a v1 series dimension. Frozen v1 datasets pin the
specialized series, revisions, commit sequence, ingestion/source provenance, row
hashes, and material hashes. Rerouting them in place would change existing
evidence and leave specialized consumers split across two truths.

A bounded follow-up must use new v2 contracts/series, preserve raw decimal text
and exact values at the adapter boundary, include funding interval in series
identity, migrate consumers explicitly, and prove every existing v1 dataset
identity and provider-free read unchanged. It must not fabricate v2 exact values
from v1 floats; specialized v1 rows remain immutable evidence.

## Extending The Feed

For a new scalar fact, first add a fact-registry contract that defines exact
numeric type, unit, dimensions, value domain, time semantics, and dataset
eligibility. If those semantics fit the exact-numeric storage shape, add a
strict data-driven binding and provider adapter implementing
`NumericFactProvider`; reuse canonical source/series, ingestion, gap, coverage,
revision, and dataset paths. Do not add provider SQL or a routing branch in the
repository.

A fact gets a different physical storage shape only when its value/temporal
constraints materially require one. New facts must not become optional columns
on candle, OI, or funding tables. Coinbase OI/funding requirement resolution
and mutable/frozen delivery are wired, but the collector handlers do not imply
historical backfill, aggregation, basis derivation, or that every strategy or
indicator already declares those facts as inputs.

For a structured provider observation, register an immutable payload schema and
keep the external state atomic. Add a strict reviewed structured-fact manifest
and provider decoder, then use the canonical source/series, writer, Dataset,
runtime, and research paths. Chainlink MVR reserve collection follows this
pattern; no downstream consumer branches on provider identity. If the source
has no reliable bounded history, use the durable scheduled collector and state
the activation boundary instead of fabricating a backfill.

## References

- [Data boundary](../architecture/data/DATA_BOUNDARY.md)
- [Numeric facts and on-demand acquisition](../architecture/data/NUMERIC_FACTS_AND_ON_DEMAND_ACQUISITION.md)
- [Provider-neutral exact numeric ADR](../architecture/decisions/0061-use-provider-neutral-exact-numeric-facts-and-bounded-acquisition.md)
- [Canonical market-data ADR](../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md)
- [Frozen backtest ADR](../architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md)
- [Typed collector ADR](../architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md)
- [Chainlink numeric facts guide](../guides/chainlink-numeric-facts.md)
- [Chainlink structured facts guide](../guides/chainlink-structured-facts.md)
- [Coinbase OI collector guide](../guides/coinbase-open-interest-collector.md)
- [Coinbase funding collector guide](../guides/coinbase-funding-rate-collector.md)
- [Runtime contract](../contracts/platform/01_runtime_contract.md)
