---
component: adr-canonical-market-data-store
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - market-data
  - candles
  - known-at
  - provenance
  - datasets
  - hard-cutover
code_paths:
  - src/market_data/contracts.py
  - src/market_data/store.py
  - src/data_providers/providers/base.py
  - portal/backend/db/market_data_models.py
  - portal/backend/service/market/feed_service.py
  - portal/backend/service/market/candle_service.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/bots/paper_market_stream.py
  - scripts/db/manual_migration_market_data_v2_hard_cutover.sql
  - docs/architecture/data/DATA_BOUNDARY.md
---
# ADR 0050: Use One Canonical Append-Only Market-Data Store

## Status

Accepted on 2026-07-26.

**Retroactive cleanup ADR:** this records the candle-feed hard cutover implemented
while establishing the market-data baseline. It extends ADRs 0003, 0044, and
0046; it does not replace their continuity, known-at, or runtime snapshot
contracts.

## Context

Provider adapters previously owned acquisition, cache policy, table creation,
repair, fallback selection, closure storage, and runtime formatting. Historical,
paper, replay, and reporting paths could therefore observe different candle
truth or silently call a provider when local data was missing. The old tables
also overwrote logical candles, making corrections and causal replay difficult
to audit.

The platform needs a boundary that can add open interest, basis, and other facts
without treating them as candle columns. ADR 0052 extends this store with the
first non-candle contract and a durable collector; unsupported fact types remain
visibly unsupported.

## Decision

Provider adapters acquire and normalize external responses only. Explicit
service, API, or CLI commands perform historical acquisition. Consumers never
invoke providers as a read fallback.

PostgreSQL schema `market` is the one canonical source-fact store. A logical
series is identified by instrument, fact type, contract version, and optional
timeframe. Closed candles use contract `candle.ohlcv.v1` and are stored as
append-only revisions with a monotonic market commit sequence. Sources,
series, candle revisions, gap evidence, and frozen datasets are immutable.

Every accepted candle records close time, `known_at`, the known-at method,
platform acceptance time, optional provider publication and receipt times, and
a hash of exact causal values. Historical rows use provider publication time
when supplied; otherwise they explicitly use interval-close inference. Paper
candles use platform acceptance as known-at and are persisted before runtime can
observe them.

Reads are local, half-open, provider-neutral, and optionally pinned by market
commit sequence and known-at cutoff. A backtest captures one commit watermark
before worker execution and records that scope in its material run config.
Nested indicator and strategy reads inherit the same scope.

Frozen datasets hash exact selected facts, source provenance, range identity,
and quality evidence. Their identity excludes unrelated global commit changes.
Quality and gaps remain separate from candle material while still participating
in the frozen dataset manifest. Runtime's derived candle snapshot remains a
separate fingerprint of the exact post-feature rows consumed by a run.

The hard-cutover migration copies valid legacy rows into `market`, verifies
counts and hashes, and moves old tables into `legacy_market_v1`. Application
code has no fallback reader or writer for that archive. Failed validation aborts
the migration.

## Invariants

- No consumer read causes a provider or paid API call.
- Candle windows are half-open: `start <= open_time < end`.
- Provisional candles are rejected; known-at cannot precede candle close.
- Corrections append a revision; accepted source facts are not updated or
  deleted.
- A read pinned to commit `N` cannot observe a later correction.
- Paper runtime cannot observe a closed candle before canonical persistence.
- A backtest and every nested market-data read use one recorded commit scope.
- Exact value or causal-time changes alter material identity.
- Provenance or quality changes remain inspectable and alter frozen dataset
  identity without pretending the candle values changed.
- Empty, duplicate, malformed, out-of-window, or unsupported data fails loudly.
- Legacy tables cannot coexist as active storage; startup rejects that state.

## Consequences

Historical acquisition is an explicit operation before research or backtest
execution. This removes convenient implicit fetches but makes cost, provenance,
and failures auditable. Append-only revisions use more storage than overwrites,
but make causal replay and correction history possible. Dataset manifests are
reusable evidence; backtests still retain their existing exact consumed-row
snapshot as the final proof of derived runtime inputs.

ADR 0052 now implements venue-specific Coinbase open-interest facts on the same
series, provenance, quality, and dataset concepts. Basis, funding, aggregated OI,
order-flow, L2, options, and live-order capability remain unclaimed.

## Rejected Alternatives

- Keep provider-owned caches and synchronize them with a new store.
- Fall back to provider acquisition when a canonical read misses data.
- Overwrite candles in place and trust ingestion timestamps in logs.
- Put open interest, basis, or quality fields into a widening candle table.
- Include the database-global commit sequence in dataset identity.
- Preserve application readers for archived legacy tables.

## Enforcing Tests Or Evidence

- `tests/test_market_data/test_contracts.py` enforces candle validation,
  causal hashes, dataset identity, and quality separation.
- `tests/test_market_data/test_feed_service.py` enforces explicit segmented
  acquisition, provider-free reads, known-at construction, gap evidence, and
  paper persistence.
- `tests/test_market_data/test_runtime_scope.py` and
  `tests/test_portal/test_container_runtime_transport.py` enforce one pinned
  backtest read scope and unpinned paper reads.
- `tests/test_portal/test_paper_market_stream.py` proves persistence occurs
  before runtime-store visibility.
- `tests/test_cli/test_market_data_cli.py` and
  `tests/test_portal/test_candle_coverage_routes.py` cover auditable CLI/API
  operations.
- The hard-cutover migration was executed twice on an isolated PostgreSQL /
  TimescaleDB clone: 1,540 legacy candles became 1,540 canonical revisions;
  the second execution verified idempotence. A repository replay returned all
  1,271 selected rows, and an unrelated series correction left the selected
  dataset ID unchanged.
- `tests/test_market_data/test_repository_db.py` additionally enforces the
  shared candle/OI commit clock and mixed-fact frozen datasets introduced by ADR
  0052.

## References

- [ADR 0003: Preserve Data Boundary Source Facts](0003-preserve-data-boundary-source-facts.md)
- [ADR 0044: Enforce Known-At Prefix Invariance](0044-enforce-known-at-prefix-invariance.md)
- [ADR 0046: Fingerprint Exact Candle Inputs And Keep Quality Separate](0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md)
- [ADR 0052: Typed Fact Collectors And Explicit Instrument Roles](0052-use-typed-fact-collectors-and-explicit-instrument-roles.md)
