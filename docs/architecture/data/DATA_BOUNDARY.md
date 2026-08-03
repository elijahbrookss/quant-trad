---
component: data-boundary
subsystem: data
layer: boundary
doc_type: architecture
status: active
tags:
  - data
  - providers
  - market-data
  - candles
  - open-interest
  - funding-rate
  - market-structure
  - trades
  - raw-archive
  - collectors
  - known-at
  - provenance
  - gaps
  - datasets
code_paths:
  - src/market_data
  - src/data_providers
  - src/core/candle_continuity.py
  - src/core/candle_snapshot.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/controller/market_data.py
  - portal/backend/service/providers
  - portal/backend/service/market
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/market_collection.py
  - portal/backend/workers/market_data_collector.py
  - cli/main.py
  - scripts/db/manual_migration_market_data_v2_hard_cutover.sql
  - scripts/db/manual_migration_market_fact_commit_clock_v1.sql
  - docs/architecture/data/diagrams/data-boundary-flow.mmd
  - docs/architecture/data/diagrams/candle-continuity-flow.mmd
---
# Data Boundary

## Purpose

The data boundary turns external observations into immutable, causally readable
market facts. It owns acquisition adapters, normalization, source and series
identity, accepted revisions, known-at and provenance evidence, gap quality,
collector schedules, and frozen dataset manifests.

Related diagrams:

- [data-boundary-flow.mmd](diagrams/data-boundary-flow.mmd)
- [candle-continuity-flow.mmd](diagrams/candle-continuity-flow.mmd)

## Boundary Contract

The boundary provides evidence. It does not make trading decisions, execute
orders, manufacture missing facts, or call a provider because a consumer read
missed.

| Owns | Does Not Own |
| --- | --- |
| provider registry, credentials, and acquisition adapters | indicator state or derived features |
| typed fact, source, series, and instrument-role contracts | strategy rules |
| historical, paper, and scheduled poll intake | execution or fill semantics |
| append-only revisions and causal reads | wallet or margin effects |
| provenance, gaps, and quality evidence | report readiness policy |
| frozen source-dataset manifests | BotLens projection state |

Provider adapters are acquisition-only anti-corruption boundaries. They isolate
API details, provider product IDs, pagination, and response metadata. They do
not create tables, choose fallback providers, or format runtime frames.

Provider/venue feature contracts enumerate only operations implemented by
Quant-Trad. Each declared operation states whether access is `public`, requires
shared provider `credentials`, or depends on `external_auth`. An undeclared
operation is unsupported and fails at the provider boundary; the API does not
advertise unbuilt upstream features. Provider errors still propagate with their
operation context because declared access is an operator contract, not a promise
that an upstream API can never change.

Credential field names live once on the provider/venue registry. Secret values
live only in encrypted provider credential references. Collectors, bots, and run
configuration do not copy secrets or credential references. Coinbase Advanced
Trade product metadata, public candles, public live data, current OI, and
current perpetual funding are declared public. Authenticated account-fee lookup
is a separate operation and uses the same shared provider credential record.

## Canonical Flows

1. Historical candles are requested explicitly through an audited CLI or API
   operation. Bounded provider segments are normalized and persisted before a
   consumer can read them.
2. Paper aggregation persists each closed candle before making it visible to
   runtime.
3. A durable collector worker registers a process heartbeat independent of any
   schedule lease, claims enabled schedules from PostgreSQL, applies database
   pacing and bounded retries, polls the exact configured product, and appends
   only while its ownership fence remains current.
4. A bounded market-structure session claims one product-scoped stream, fsyncs
   each exact frame into a definition-scoped spool, publishes an immutable
   object archive and record mappings, then appends typed trades, coverage, and
   causal aggregates under the same market fact commit clock.
5. Research, indicators, checks, backtests, paper runtime, reports, and APIs read
   canonical storage only. Missing series or facts are explicit errors or
   structured optional caveats.
6. Backtest preparation resolves transitive requirements, validates coverage,
   and freezes an immutable dataset. Startup admits that dataset against exact
   strategy, indicator, execution-policy, instrument, warmup, and run identity.

## Implemented Source Facts

Logical series are keyed by canonical instrument, fact type, contract version,
and optional timeframe. Candles, OI, funding, trades, market-state features,
and normalized features share one database-wide market fact commit sequence,
allowing one mixed-fact dataset watermark.

Physical fact storage is typed by fact contract, not by provider, venue, or
instrument. `candle_versions` stores all `candle.ohlcv.v1` series,
`open_interest_versions` stores all `derivatives.open_interest.v1` series,
`funding_rate_versions` stores all `derivatives.funding_rate.v1` series,
`market_trade_versions` stores all `market.trade.v1` series, and
`trade_flow_aggregate_versions` stores all `market.trade_flow.v1` series.
The generic source, series, ingestion, gap, dataset, collection-definition,
collection-attempt, pacing, lease, and commit-clock tables are shared.
`collector_worker_state` is a mutable operational read model: its independent
heartbeat proves idle or in-flight process liveness without becoming a market
fact. Successful and failed collection attempts retain one bounded typed timing
payload separating schedule lag, pacing, provider request wall time, contract
validation, canonical normalization, lease heartbeat, and persistence.

Phase 3 market-state tables preserve typed BBO, depth, flow, futures/spot,
derivative-state, and response facts. Phase 4 adds immutable normalization specs
and append-only normalized revisions without replacing their source facts. A
registry declares each fact type's contract version, timeframe behavior,
record-time field, archive policy, and dataset eligibility; reconstructed L2
book state is intentionally replay input rather than a dataset fact surface.

Frozen dataset requests may select a legacy candle identity or an exact typed
series identity. Normalized outputs require the exact transitive source series
and range in the same dataset. Dataset freeze and read verify spec/input
fingerprints and any required raw archive object bytes and checksums, preserving
provider-free execution after ordinary hot-store compaction or correction.

A new fact family gets a typed revision table only when its value and temporal
constraints differ materially. It does not get one table per exchange or symbol.
This keeps database constraints and causal reads explicit without collapsing
unrelated facts into a universal JSON payload. The scheduler itself is
provider-neutral. OI and funding use explicit typed collector handlers behind
the same schedule, retry, pacing, ownership, evidence, and gap lifecycle.
Lease-fence validation remains one shared repository guard.

### Coinbase Futures/Spot Trades

`market.trade.v1` revisions preserve provider product/trade ID, provider event
and message time, receipt/acceptance/known-at, exact Decimal price and provider
size, proven contract/base/quote translations, maker side, explicitly derived
aggressor side, provider sequence and delivery positions, product-definition
revision, raw record identity, coverage identity, provenance, quality, and
commit sequence.

Bounded sessions assign stable `spool_segment_id` and `raw_record_id` values
before parsing and fsync exact frames to local WAL. Verified immutable
Parquet/ZSTD objects plus PostgreSQL manifests and record mappings are required
before canonical publication and dataset eligibility. Stream definitions reuse
the provider credential registry and cannot contain secrets.

Typed trade coverage intervals distinguish healthy zero-trade periods from
gaps, disconnects, pending archive upload, and canonicalization lag.
`market.trade_flow.v1` produces causal one-second and one-minute buckets. The
initial subscription snapshot is canonical trade evidence but never complete
live flow. Frozen trade/flow ranges pin all required raw archive objects and can
be read without a provider.

### Candles

`candle.ohlcv.v1` revisions record half-open open and close timestamps, exact
OHLC, optional volume and trade count, publication and receipt timestamps when
available, platform acceptance, known-at and method, source, provenance,
revision, commit sequence, and causal row hash.

Historical rows use provider publication time when supplied; otherwise
`interval_close_inferred` states the limitation. Paper candles use platform
acceptance. Provisional candles and known-at before close are invalid. Windows
are half-open: `start <= candle_open_time < end`.

### Coinbase Open Interest

`derivatives.open_interest.v1` revisions record scheduled sample time, finite
nonnegative contract count, unit, receipt and acceptance time, known-at and
method, source, provenance, revision, commit sequence, and causal row hash.

Coinbase supplies current OI but no event timestamp through this adapter.
`sample_time` is therefore the collector schedule and `known_at` is platform
acceptance after receipt. The data is venue-specific polling evidence, not exact
exchange-event-time history.

Definitions are disabled by default and require an explicit canonical Coinbase
futures instrument plus exact provider product ID. Missed schedules and
exhausted attempts create gap evidence. Lease generation, secret token hash,
expiry, and a write-transaction fence prevent stale workers from publishing.

### Coinbase Perpetual Funding

`derivatives.funding_rate.v1` revisions record scheduled sample time, signed
finite funding rate as a fractional value, provider-reported funding time,
positive interval seconds, receipt and acceptance time, known-at and method,
source, provenance, revision, commit sequence, and causal row hash.

Coinbase exposes the tuple on the same public Advanced Trade product response,
so collection does not require credentials. Its documentation exposes
`funding_time` but does not define it as an event or publication timestamp.
The adapter therefore preserves it as provider-reported metadata and never uses
it to establish causal visibility. `sample_time` is the collector schedule and
`known_at` is platform acceptance after receipt.

Definitions require an explicit canonical Coinbase instrument with
`has_funding=true` plus the exact provider product ID. Storage, frozen dataset
reads, operator latest-known reads, retries, gap evidence, pacing, and fencing
are implemented. Indicator/strategy requirement delivery for funding is not
wired yet; consumers cannot imply that support merely because collection exists.

## Consumer Requirements And Instrument Roles

Indicators and checks declare fact type, contract version, key, required fields,
alignment, staleness, gap policy, and one instrument role:

- `primary`: the traded canonical instrument;
- `underlying`: the canonical underlying ID mapped for that primary;
- `benchmark`: a named alias mapped to one canonical ID;
- `explicit`: one declared canonical instrument ID.

Consumers do not declare provider, endpoint, table, schedule, or fallback.
Underlying and benchmark relationships come from immutable run configuration;
symbol parsing is not a valid relationship resolver.

## Dataset Identity, Provenance, And Quality

A frozen dataset manifest identifies exact selected ranges and keeps separate
hashes for typed-fact material, acquisition provenance, and quality evidence.
Its stable ID excludes unrelated global commit movement. Corrections append a
revision; reads pinned to commit `N` cannot observe a later revision.

Gap evidence is immutable, range-based, and conservative. It records expected
and observed counts, classification, detection watermark, and structured
provider or ingestion evidence. Closures, warmup, confidence, and caveats do not
mutate values or synthesize rows.

Backtest OI inputs are frozen far enough back to cover indicator warmup plus the
declared latest-known staleness window. Every warmup and decision bar resolves
only facts known at that bar. Required unavailable or stale facts stop the run;
optional facts produce a structured caveat. The indicator engine receives a
fresh per-indicator fact map each bar and clears it before the next bar.

Runtime separately fingerprints exact consumed candle/ATR frames through
`candle_series_snapshot.v1`. This complements the source manifest rather than
replacing source provenance and quality.

## Failure, Recovery, And Scaling

- Missing canonical data fails without provider fallback.
- Missing credentials fail before an explicitly authenticated provider
  operation. Public operations never consult the credential store.
- Unsupported provider, venue, product, fact contract, or instrument role fails
  with context.
- Duplicate, malformed, provisional, conflicting, or out-of-window facts fail
  before acceptance.
- Append-only table mutations are rejected by database triggers.
- Collector worker heartbeat expiry is explicit and independent from schedule
  freshness. An enabled definition is healthy only when both worker liveness and
  recent delivery evidence are current.
- Collector work is idempotent by scheduled sample, resumable after restart,
  paced in PostgreSQL, bounded in retries, and fenced across processes.
- Candle, OI, and funding revisions are TimescaleDB hypertables indexed for
  series/time, revision, commit, and known-at reads.
- Historical ingestion uses staged set operations and bounded provider segments.
  Collector and historical concurrency remain conservative until measurements
  justify widening them.
- Legacy candle tables are archived under `legacy_market_v1`; application code
  has no fallback reader. The fact-clock migration archives prior active dataset
  manifests whose provenance identity used the old contract.

## Invariants

- No consumer read performs acquisition.
- Provider-specific behavior stops at the adapter boundary.
- Accepted facts and evidence are immutable; corrections append.
- Known-at cannot precede the evidence required by the fact contract.
- Paper runtime cannot observe a closed candle before canonical persistence.
- One backtest uses one frozen dataset and one recorded commit scope across
  nested reads.
- Exact material, provenance, and quality remain distinct and inspectable.
- Required stale or unavailable inputs fail; optional gaps are explicit.
- Instrument relationships are canonical IDs, never symbol guesses.
- Provider credentials flow through credential references, not bot or run
  configuration.
- Provider feature declarations include only implemented operations and their
  authentication mode; undeclared requests fail as unsupported.
- No provider error is reclassified from guessed response text.

## Known Gaps

- Coinbase OI is polling-only and has no supported historical backfill. History
  begins when an enabled collector accumulates it.
- Coinbase OI lacks a provider event timestamp through the current endpoint, so
  poll schedule and platform acceptance bound what can be known.
- Coinbase funding is polling-only and has no implemented historical backfill.
  History begins when an enabled collector accumulates it.
- Coinbase funding time is provider-reported but not treated as publication
  time; platform receipt and acceptance govern known-at.
- Mutable OI/funding runtime delivery and frozen typed-fact delivery are
  implemented. Normalized facts require a frozen dataset. Raw Level 2 book
  state, options, and live-trading integration remain unsupported.
- Provider publication timestamps are unavailable from some candle endpoints;
  interval-close inference remains explicit provenance.
- Session/calendar evidence cannot classify every closure.
- Throughput has not yet justified wider historical or collector concurrency.

## Related Docs

- [System model](../system/SYSTEM_MODEL.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [ADR 0044: Known-at prefix invariance](../decisions/0044-enforce-known-at-prefix-invariance.md)
- [ADR 0050: Canonical append-only market data](../decisions/0050-use-one-canonical-append-only-market-data-store.md)
- [ADR 0051: Frozen datasets for canonical backtests](../decisions/0051-require-frozen-datasets-for-canonical-backtests.md)
- [ADR 0052: Typed fact collectors and instrument roles](../decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md)
- [Market Structure Data Plane](MARKET_STRUCTURE_DATA_PLANE.md)
- [Market Structure Phase 1 Trades](MARKET_STRUCTURE_PHASE_1_TRADES.md)
- [Accepted ADR 0053: Tiered market-structure archive and replay](../decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md)
