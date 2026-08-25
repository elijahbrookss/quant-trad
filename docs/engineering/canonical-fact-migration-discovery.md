---
status: historical
---

# Canonical Fact Migration Discovery

> Historical evidence record. "Current" and database-state descriptions below
> are observations at the recorded 2026-08-09 pre-cutover baseline, not claims
> about the platform today.

Status: historical pre-cutover discovery checkpoint. The inventory below was
completed before implementation and was the comparison boundary for the now
completed canonical migration. See
[`canonical-fact-migration-validation.md`](canonical-fact-migration-validation.md)
for final results.

Baseline: Git `77f4d41805031ad30cffd5b03983bdd09e76acba`, PostgreSQL 15.6,
TimescaleDB 2.14.2, database `quanttrad`, inspected on 2026-08-09. The verified
rollback artifact is recorded in
[`canonical-fact-migration-backup.md`](canonical-fact-migration-backup.md).

## Finding

QT has one logical series namespace and one causal commit clock, but it does not
yet have one canonical Fact contract. The active data plane has four mutually
incompatible core records plus a second structured market-state repository:

| Family | Python/storage shape | Durable table | Precision / identity |
| --- | --- | --- | --- |
| Candle | `CandleFact` / `CandleRecord` | `market.candle_versions` | binary floats; identity is series/open-time/revision |
| Open interest | `OpenInterestFact` / `OpenInterestRecord` | `market.open_interest_versions` | binary float scalar; identity is scheduled sample/revision |
| Funding | `FundingRateFact` / `FundingRateRecord` | `market.funding_rate_versions` | binary float plus funding time/interval; identity is scheduled sample/revision |
| Exact numeric | `NumericFact` / `NumericFactRecord` | `market.numeric_fact_versions` | exact `numeric`, but permanently scalar; identity is provider event/revision |
| Structured source and derived state | trades, trade-flow aggregates, L2 snapshots/mutations/checkpoints, BBO/depth/flow/basis/derivative/response/normalized features | typed tables in `market` | exact numeric fields and family-specific identities/hashes |

`market.series` already supplies provider-neutral logical identity through
instrument, fact type, contract version, timeframe, and dimensions.
`market.fact_commit_seq` already supplies a shared accepted-order watermark.
Those are useful foundations, not a complete Fact abstraction.

The main limiting assumption is not that every stored row is literally scalar.
It is that the general path recognizes only `NumericFact` as extensible, and its
registry fixes `storage_shape="exact_numeric"`. Any richer provider state must
either be flattened into unrelated numeric rows or grow another family-specific
repository branch.

## Fact-like contracts found

### Core contracts

- `SourceIdentity`, `MarketDataRequirement`, `DatasetSeriesRequest`, and
  `FrozenDataset` establish source, requirement, and freeze identity.
- `CandleFact`, `OpenInterestFact`, `FundingRateFact`, and `NumericFact` each
  encode their own time fields, material/hash rules, and record envelope.
- `MarketDataStore` exposes separate ingest/read methods for every core family
  plus a generic `read_series_records` dispatcher.
- `fact_registry.py` validates scalar exact-numeric types and declares the
  currently supported `market.reference_price.v1` and
  `market.reserve_balance.v1` contracts.

### Existing structured observations

- `MarketTradeFact` is an atomic provider trade with price, provider size,
  unit conversion, side semantics, provider event/message/receipt/acceptance/
  known-at times, source position, raw-record identity, and quality lineage.
- `TradeFlowAggregateFact` is a structured causal bucket with counts, volumes,
  OHLC, coverage completeness, late-data evidence, and an input fingerprint.
- `L2EventFact`, `L2SnapshotFact`, `L2MutationBatchFact`, and
  `BookCheckpointFact` preserve ordered book state and replay evidence.
- `BboFeatureFact`, `DepthFeatureFact`, `TradeFlowFeatureFact`,
  `BasisFeatureFact`, `DerivativeStateFeatureFact`, `ResponseFeatureFact`, and
  `NormalizedFeatureFact` are typed structured/derived observations with
  explicit schema versions and deterministic material hashes.

These types prove QT already needs atomic multi-field facts. They must inform
the generalized contract instead of being forced through a scalar payload.

## Provider and acquisition boundaries

- Generic candle providers implement the existing `ProviderInterface`; CCXT,
  Coinbase, Yahoo, Alpaca, and Interactive Brokers differ at acquisition.
- Coinbase separately returns float-based `ProviderOpenInterestSnapshot` and
  `ProviderFundingRateSnapshot`. `MarketDataCollectorService` translates them
  into specialized facts and invokes specialized repository writers.
- The streaming Coinbase adapter emits raw messages which the market-structure
  collector archives, canonicalizes into trades/L2 state, and materializes into
  typed structured tables.
- Chainlink implements `NumericFactProvider` only. Its AggregatorV3 adapter
  emits exact scalar `ProviderNumericObservation` batches; the acquisition
  service translates them into `NumericFact` and writes
  `numeric_fact_versions`.
- Historical Chainlink acquisition is explicit, bounded, finality-aware, and
  cached by coverage. It never supplies a runtime fallback. That authority
  boundary must be retained while generalizing the observation type.
- Current continuous poll collectors are OI/funding-specific. The durable
  market-structure stream collector already supplies restart recovery, ordered
  raw evidence, gap/coverage state, and append-only revisions, but is not a
  general latest-state poller.

Provider identity presently survives in `market.sources`, ingestion runs, raw
archive mappings, and provenance. Downstream requirement planning is mostly
provider-neutral, but collector creation and historical acquisition remain
family/provider-specific at the intended operational boundary.

## Dataset, replay, Indicator, Check, and Observation path

The active research path is:

```text
requirement planning
  -> resolve market.series
  -> optional explicitly authorized acquisition
  -> freeze dataset_series watermarks and hashes
  -> provider-disabled read_series_records
  -> Indicator/runtime evidence
  -> durable research Check evidence
  -> derived Observation and research links
```

Important findings:

- Dataset planning is generic at the requirement/series level but branches by
  fact type for coverage, acquisition, range semantics, and row selection.
- `freeze_dataset`, material/provenance hashing, and `read_dataset_series`
  dispatch across each physical family. Frozen manifests do not store provider
  credentials or acquisition authority.
- Backtest and Check execution normalize a provider-disabled frozen binding and
  re-read accepted revisions through the pinned commit watermark.
- Runtime mutable delivery admits only OI and funding (candles are delivered by
  the bar path); other facts require a frozen binding.
- Indicator manifests can declare arbitrary `MarketDataRequirement` values,
  but the active indicators remain candle-centric. Market-state derivation
  reads OI via `.fact.value` and funding via `.fact.rate`, `.funding_time`, and
  `.interval_seconds`.
- The registered `event_fact_analysis` Check explicitly calls
  `causal_numeric_fact_records` and converts `.fact.value` to `float` for latest,
  previous, and window calculations. This is the clearest downstream
  `Fact = scalar value` coupling.
- `research_science.temporal.FrozenFact.value` is typed as `Any`, so the temporal
  join envelope can already carry a structured value, but current Check
  feature extraction does not expose a schema-aware projection.
- Durable Check evidence embeds the frozen dataset binding and selected record
  material. Observations created from eligible Checks retain evidence/result
  hashes and link back to the Check and dataset.

## CLI, API, MCP, and worker surfaces

The cutover affects:

- `qt data acquire-numeric-facts`, series inspection, dataset freeze/inspect,
  backtest dataset preparation, collector management, OI latest, and funding
  latest;
- `/api/market-data/numeric-facts/acquire`, collector and latest-fact routes,
  market-structure routes, and `/api/candles/datasets/*`;
- bot dataset preparation/start and research Check prepare/run routes;
- MCP research Check preparation/execution, dataset-backed bot operations, and
  observation creation (MCP currently has no direct numeric-acquisition tool);
- collector workers, continuous stream collector/supervisor, bot runtime market
  resolver, candle feed service, normalization materialization, research
  planner/evaluator, and reporting dataset inspection.

These surfaces should keep purpose-specific operator names where useful, but
all storage and frozen-read behavior must converge on the canonical Fact path.

## Direct storage assumptions

The largest concentration is
`portal/backend/service/storage/repos/market_data.py`, which contains separate
row decoders, writers, readers, material hashes, dataset hashing, and frozen
read branches for candles, OI, funding, numeric facts, trades, aggregates, and
features. `market_structure.py` owns a second set of structured writers/readers.

Other direct assumptions exist in:

- `portal/backend/db/market_data_models.py` and database startup validation;
- collector, feed, runtime delivery, backtest dataset, normalization, market
  structure, and frozen-dataset services;
- `src/market_data/contracts.py`, `store.py`, `requirements.py`,
  `market_state.py`, `normalization.py`, and the structured fact modules;
- the candle repository and run-research reporting SQL;
- storage lifecycle policies;
- manual SQL migrations and DB integration tests.

The existing migration history may remain immutable, but none of these old
tables or dispatchers can remain an active runtime architecture after cutover.

## Persisted migration boundary

Exact source-database counts at discovery:

| Durable relation/evidence | Rows |
| --- | ---: |
| Candles | 29,123 |
| Open interest | 63,848 |
| Funding rates | 28,281 |
| Exact numeric facts | 8,016 |
| Market trades | 97,280 |
| Trade-flow aggregates | 45,941 |
| L2 snapshot versions / levels | 25 / 941,816 |
| L2 mutation batches / mutations | 1,043 / 11,244 |
| BBO / depth / flow feature versions | 77 / 231 / 9,852 |
| Basis / derivative-state / response versions | 16 / 20 / 1 |
| Normalized feature versions | 41 |
| Gap evidence | 165 |
| Acquisition coverage rows | 16 |
| Frozen datasets / dataset series | 54 / 105 |
| Research Checks / observations | 21 / 9 |
| Check-to-dataset evidence links | 13 |

The 8,016 numeric rows are all active
`market.reference_price.v1` Chainlink ETH/USD observations. No reserve-balance
rows are currently persisted. The database also contains real and test/fixture
sources; fixture-labelled durable rows are still valid persisted evidence and
must not be silently discarded.

The database contains 19 candle, 9 OI, 13 funding, and one reference-price
logical series. It also contains structured trade, trade-flow, L2, market-state,
and normalization series. Dataset manifests include candle-only, derivative,
and Chainlink-reference-price research evidence.

`legacy_market_v1` contains the original hard-cutover candle archive and
pre-multifact dataset snapshots. It has no active fallback reader, but it is
still obsolete infrastructure to evaluate for deletion after canonical
equivalence is proved.

## Data quality and migration hazards

Read-only validation found no orphan series/instruments, dataset/series rows,
or gap/source rows; no malformed dataset hashes/ranges; no non-finite or
causally invalid candles/OI; and no numeric envelope/series mismatches, invalid
states, or malformed hashes.

Funding `funding_time` often precedes collector `sample_time`. This is valid:
Coinbase does not define `funding_time` as QT publication or known-at time. The
migration must preserve it as payload semantics, not reinterpret it as the
observation clock.

Precision is the principal migration hazard:

- candle/OI/funding v1 values were stored as binary float;
- Chainlink and market-structure values are exact decimals;
- converting legacy float text to a new exact decimal cannot pretend the
  provider originally supplied exact decimal evidence unless provenance proves
  it;
- the canonical registry therefore needs explicit historical payload schema
  versions whose decoding and material hashes preserve the accepted v1
  evidence exactly.

Candle provenance JSON is empty for 27,583 rows and populated for the 1,540
legacy-import rows. Source identity still exists through each ingestion run.
The canonical envelope must preserve that distinction and must not manufacture
missing provider timestamps or provenance.

Existing dataset IDs and hashes depend on family-specific material and
provenance hash algorithms. A migration that merely reserializes rows will
break durable Check and Observation evidence. Equivalence validation must
compare every old revision, selected visible record, series row count,
watermark, material hash, provenance hash, quality hash, dataset ID/hash, and
research link before legacy deletion.

## Required final contract constraints

The implementation design must satisfy all of the following discoveries:

1. One immutable Fact envelope supplies fact type, payload schema version,
   subject/series identity, observation/effective time, known-at, source and
   provenance identity, revision/commit identity, state, quality, and gap
   semantics.
2. Payloads are registered typed schemas with field-level validation,
   deterministic canonical encoding, query metadata, and explicit evolution.
   Arbitrary unregistered JSON is not a payload contract.
3. Atomic provider observations remain atomic. Scalar facts are one payload
   schema, not the universal representation.
4. Historical v1 payload schemas remain interpretable through the canonical
   registry; this is versioned data interpretation, not a legacy table reader
   or compatibility adapter.
5. Provider decoding and operational acquisition remain above the Fact
   boundary. Dataset planning, freezing, replay, Indicators, Checks, and
   Observations consume canonical semantics and schema-aware projections.
6. One repository path owns insert, revision, causal selection, hashing,
   freezing, and replay for all dataset-eligible Facts.
7. Gap/acquisition coverage, raw archive evidence, and non-dataset live book
   state remain typed operational evidence linked to Facts; they are not
   flattened into the payload store.
8. Cutover is explicit and offline: populate new storage, validate old/new
   equivalence, switch every reader/writer, run frozen replay proofs, and only
   then remove the old active relations and dispatchers.

No schema implementation should begin from the old `NumericFact` abstraction
alone. The final contract and ADR must be derived from this complete inventory.
