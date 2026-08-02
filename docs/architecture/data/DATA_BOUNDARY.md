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
  - portal/backend/service/providers
  - portal/backend/service/market
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/market_collection.py
  - portal/backend/workers/market_data_collector.py
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
Trade product metadata, public candles, public live data, and current OI are
declared public. Authenticated account-fee lookup is a separate operation and
uses the same shared provider credential record.

## Canonical Flows

1. Historical candles are requested explicitly through an audited CLI or API
   operation. Bounded provider segments are normalized and persisted before a
   consumer can read them.
2. Paper aggregation persists each closed candle before making it visible to
   runtime.
3. A durable collector worker claims enabled schedules from PostgreSQL, applies
   database pacing and bounded retries, polls the exact configured product, and
   appends only while its ownership fence remains current.
4. Research, indicators, checks, backtests, paper runtime, reports, and APIs read
   canonical storage only. Missing series or facts are explicit errors or
   structured optional caveats.
5. Backtest preparation resolves transitive requirements, validates coverage,
   and freezes an immutable dataset. Startup admits that dataset against exact
   strategy, indicator, execution-policy, instrument, warmup, and run identity.

## Implemented Source Facts

Logical series are keyed by canonical instrument, fact type, contract version,
and optional timeframe. Candles and OI share one database-wide market fact
commit sequence, allowing one mixed-fact dataset watermark.

Physical fact storage is typed by fact contract, not by provider, venue, or
instrument. `candle_versions` stores all `candle.ohlcv.v1` series and
`open_interest_versions` stores all `derivatives.open_interest.v1` series. The
generic source, series, ingestion, gap, dataset, collection-definition,
collection-attempt, pacing, lease, and commit-clock tables are shared.

A new fact family gets a typed revision table only when its value and temporal
constraints differ materially. It does not get one table per exchange or symbol.
This keeps database constraints and causal reads explicit without collapsing
unrelated facts into a universal JSON payload. The scheduler itself is already
provider-neutral. When a second point-in-time fact such as funding is
implemented, repeated repository and collector dispatch is the signal to
introduce a fact-handler registry.

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
- Collector work is idempotent by scheduled sample, resumable after restart,
  paced in PostgreSQL, bounded in retries, and fenced across processes.
- Candle and OI revisions are TimescaleDB hypertables indexed for series/time,
  revision, commit, and known-at reads.
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
- Funding, basis, cross-venue aggregation, expanded market state, L2, order flow,
  options, and live trading are not implemented.
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
