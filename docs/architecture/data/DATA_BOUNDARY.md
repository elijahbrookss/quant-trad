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
  - scripts/db/manual_migration_market_data_v2_hard_cutover.sql
  - docs/architecture/data/diagrams/data-boundary-flow.mmd
  - docs/architecture/data/diagrams/candle-continuity-flow.mmd
---
# Data Boundary

## Purpose

The data boundary turns external observations into immutable, causally readable
market facts. It owns acquisition adapters, normalization, source and series
identity, accepted fact revisions, known-at and provenance evidence, gap quality,
and frozen dataset manifests.

Related diagrams:

- [data-boundary-flow.mmd](diagrams/data-boundary-flow.mmd)
- [candle-continuity-flow.mmd](diagrams/candle-continuity-flow.mmd)

## Boundary Contract

The data boundary provides evidence. It does not make trading decisions, execute
orders, manufacture missing rows, or silently call a provider for a consumer.

| Owns | Does Not Own |
| --- | --- |
| provider registry, credentials, and acquisition adapters | indicator state or derived features |
| typed market-fact and source identities | strategy rules |
| explicit historical and paper intake | execution or fill semantics |
| append-only revisions and causal reads | wallet or margin effects |
| provenance, gaps, and quality evidence | report readiness policy |
| frozen source-dataset manifests | BotLens projection state |

Provider adapters are acquisition-only anti-corruption boundaries. They isolate
external API details, symbols, pagination, and provider metadata. They do not
create tables, cache rows, choose fallback providers, or format runtime frames.

## Canonical Flow

[data-boundary-flow.mmd](diagrams/data-boundary-flow.mmd) shows two explicit
intake paths and one read path:

1. An operator, audited CLI command, or API request chooses the canonical
   instrument, provider, venue, timeframe, and half-open window.
2. The selected adapter acquires external rows. Acquisition is never triggered
   by a read miss.
3. The feed service validates ordering, duplicates, OHLCV, candle closure,
   requested bounds, and known-at evidence.
4. The canonical repository registers the source and typed series, then appends
   accepted candle revisions and range-based gap evidence under schema `market`.
5. Paper aggregation persists each closed candle before placing it in the
   runtime-visible store.
6. Research, backtest, indicator, reporting, and API consumers read only the
   canonical store. Missing series fail with an explicit-ingestion error.
7. Backtest preparation freezes the complete required ranges into one immutable
   dataset manifest. Startup admits that dataset against the exact strategy,
   indicator, execution-policy, instrument, warmup, and run configuration;
   nested strategy and indicator reads inherit the admitted commit scope.

## Source-Fact Contract

The implemented fact type is `candle.ohlcv` contract `candle.ohlcv.v1`. Each
logical series is keyed by canonical instrument, fact type, contract version,
and timeframe. Every candle revision records:

- half-open open and close timestamps;
- exact OHLC, optional volume and trade count;
- provider publication and platform receipt timestamps when available;
- platform acceptance time;
- `known_at` and an explicit known-at method;
- immutable revision and market commit sequence;
- source identity, ingestion operation, and provenance;
- an exact causal row hash.

Historical data uses provider publication time when the adapter supplies it.
Otherwise, `interval_close_inferred` states the limitation directly. Paper data
uses platform acceptance as known-at. A provisional candle or a known-at value
before close is invalid.

Corrections append revisions. A read pinned to market commit `N` selects the
latest revision visible at `N`; a known-at cutoff may narrow that set further.
Current reads select the latest accepted revision. All windows are half-open:
`start <= candle_open_time < end`.

## Dataset Identity, Provenance, And Quality

A frozen dataset manifest identifies exact selected ranges and contains separate
hashes for candle material, acquisition provenance, and quality evidence. Its
stable dataset ID includes those selected hashes but excludes unrelated global
commit movement. An update to another series therefore cannot rename an
unchanged dataset.

Gap evidence is immutable, range based, and conservative. It records expected
and observed counts, classification, detection watermark, and structured
provider or ingestion evidence. Continuity, closures, warmup, confidence, and
caveats describe trust; they do not mutate candle values or synthesize rows.

Runtime also fingerprints its exact consumed candle/ATR frames through
`candle_series_snapshot.v1`. That derived-input proof complements the frozen
source dataset: the source manifest proves accepted market facts, while the
runtime snapshot proves what the engine actually consumed after feature
construction.

## Failure And Recovery

- Missing canonical data fails; it does not fall back to an external call.
- Missing credentials fail before explicit acquisition starts.
- Unsupported provider, venue, symbol, fact type, or contract versions fail with
  context.
- Empty provider segments and provider exceptions create gap evidence and fail
  the requested acquisition when no usable facts remain.
- Duplicate, malformed, provisional, or out-of-window rows fail before
  acceptance.
- Append-only table mutations are rejected by database triggers.
- Startup rejects active legacy market-data tables instead of supporting two
  storage paths.
- The one-off hard-cutover migration verifies legacy counts and hashes before
  archiving old tables under `legacy_market_v1`; application code cannot read
  that archive.

## Scaling Shape

Historical requests are split into bounded provider-sized segments. Acceptance
uses a staged set operation and a per-series transaction lock, rather than one
transaction per candle. Candle revisions are a TimescaleDB hypertable indexed by
series/time/revision, series/commit, and series/known-at. Consumers use bounded
windows and explicit watermarks. Concurrency is intentionally conservative
until measured provider and database evidence justifies widening it.

## Invariants

- No synthetic candle exists unless a future typed fact contract explicitly
  models it.
- No consumer read performs provider acquisition.
- Provider-specific behavior stops at the adapter boundary.
- Accepted facts and evidence are immutable; corrections append.
- Known-at cannot precede candle close, publication, or receipt evidence.
- Paper runtime cannot observe a closed candle before canonical persistence.
- One backtest uses one recorded market-data watermark across nested reads.
- Every canonical backtest names one frozen dataset ID; execution cannot create,
  expand, or substitute that dataset.
- Exact material identity and quality evidence remain distinct.
- Missing or malformed evidence is unavailable, never optimistic empty proof.
- Instrument metadata is validated before execution depends on tick size,
  contract size, fees, shorting, or margin.
- Provider credentials flow through credential references, not bot or run
  configuration.

## Related Docs

- [System model](../system/SYSTEM_MODEL.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [ADR 0044: Known-at prefix invariance](../decisions/0044-enforce-known-at-prefix-invariance.md)
- [ADR 0046: Exact candle inputs and separate quality](../decisions/0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md)
- [ADR 0050: Canonical append-only market data](../decisions/0050-use-one-canonical-append-only-market-data-store.md)
- [ADR 0051: Frozen datasets for canonical backtests](../decisions/0051-require-frozen-datasets-for-canonical-backtests.md)

## Known Gaps

- Only `candle.ohlcv.v1` is implemented. Open interest, basis, funding, market
  state, L2, order flow, options, and live trading are not supported by this
  contract yet.
- Provider publication timestamps are not available from every historical API;
  interval-close inference remains an explicit provenance limitation.
- Session/calendar evidence is not complete enough to classify every closure.
- Historical provider segments are bounded and sequential; concurrency has not
  yet been justified by measured throughput.
