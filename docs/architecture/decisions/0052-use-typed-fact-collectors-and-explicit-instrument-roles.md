---
component: adr-typed-market-fact-collectors
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - market-data
  - collectors
  - open-interest
  - known-at
  - datasets
  - fencing
code_paths:
  - src/market_data/contracts.py
  - src/market_data/requirements.py
  - src/data_providers/providers/coinbase.py
  - src/data_providers/registry.py
  - portal/backend/service/providers/provider_service.py
  - portal/backend/db/market_data_models.py
  - portal/backend/service/market/collector_service.py
  - portal/backend/service/market/runtime_market_data.py
  - portal/backend/service/storage/repos/market_collection.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/workers/market_data_collector.py
  - portal/backend/service/market/backtest_dataset_service.py
  - src/engines/indicator_engine/contracts.py
  - cli/main.py
---
# ADR 0052: Use Typed Fact Collectors And Explicit Instrument Roles

## Status

Accepted on 2026-08-01.

## Context

Candles have bounded historical endpoints and an existing paper stream, but
venue-specific observations such as open interest arrive on different schedules
and do not belong in a widening candle row. Consumers also need to say whether a
fact applies to the traded instrument, its underlying, a named benchmark, or one
explicit instrument without choosing a provider endpoint or guessing symbols.

Coinbase Advanced Trade exposes current futures open interest through its product
response but does not provide historical backfill or a provider event timestamp
for that field. Quant-Trad polls the public Advanced Trade product endpoint, so
current OI does not require account credentials. A durable poller still needs
explicit sampling, known-at, retry, gap, pacing, and ownership semantics.
Backtests use accumulated facts from a frozen dataset and never poll Coinbase.

## Decision

Indicators and checks declare typed `MarketDataRequirement` inputs. A declaration
owns fact type, contract version, input key, required fields, alignment,
staleness, gap policy, and one instrument role: `primary`, `underlying`,
`benchmark`, or `explicit`. Immutable run configuration supplies canonical IDs
for underlying and benchmark relationships. Symbol heuristics are forbidden.

Collection is a producer concern independent of consumers. Durable PostgreSQL
definitions own schedule, provider product ID, retry policy, pacing, enablement,
lease generation, and attempt evidence. Workers claim due work with row locking,
bounded concurrency, expiring leases, and a secret token hash. The fact append
transaction rechecks that ownership fence, so a stale worker cannot publish.
Missed schedules and exhausted attempts create immutable gap evidence.

Provider venues declare only operations Quant-Trad implements. Each declared
feature states one authentication mode: `public`, `credentials`, or
`external_auth`. Undeclared operations fail as unsupported and are not
advertised as a catalog of upstream-provider features. Credential keys remain
defined once at the provider/venue boundary, and encrypted references are
resolved only by authenticated provider operations. Collection definitions
never carry credentials or credential references.

The first handler is Coinbase venue-specific `derivatives.open_interest.v1`.
`sample_time` is the scheduled poll identity. Because the provider supplies no
event time, `known_at` is platform acceptance after receipt and provenance says
that provider event time is unavailable. Values are nonnegative finite contract
counts; malformed, absent, or conflicting fields fail loudly. Definitions are
disabled by default and require an explicit canonical Coinbase instrument and
provider product ID.

Open-interest revisions have their own append-only hypertable and share the
database-wide market fact commit clock with candles. Dataset manifests can
contain both contracts. Backtest planning resolves transitive indicator inputs,
freezes enough OI history for warmup and staleness, verifies causal availability
at every engine step, and then executes with provider-free latest-known reads.
Paper/runtime reads use only canonical stored facts and the same max-staleness
policy. The indicator engine receives a fresh immutable per-indicator fact map on
each bar and clears it between bars.

## Invariants

- Consumers never declare provider, endpoint, table, poll schedule, or fallback.
- An operation is callable only when declared in the provider feature contract;
  declarations describe implemented Quant-Trad behavior, not every upstream
  provider offering.
- Public operations do not access the provider credential store.
- Instrument relationships use canonical IDs supplied by run configuration; no
  symbol parsing infers underlying or benchmark identity.
- A read miss never calls Coinbase or another provider.
- OI `known_at` never precedes schedule, receipt, or platform acceptance.
- One scheduled OI sample is idempotent; changed same-schedule values are not
  silently accepted as consumer-path corrections.
- Only the current fenced owner can append a collected fact or complete an
  attempt.
- Required unavailable or stale facts stop execution; optional facts return a
  structured caveat.
- Frozen backtests cannot observe facts beyond the dataset commit watermark or
  facts not known at the decision time.
- Collector failures and missed schedules remain quality evidence, not synthetic
  zeroes or forward-filled truth.
- Funding, basis, aggregated OI, historical OI backfill, L2, order flow, options,
  and live order submission remain unsupported.

## Consequences

Collectors can accumulate venue facts continuously and independently of bot or
backtest speed. More providers and fact handlers can reuse scheduling and
storage ownership without forcing one universal payload. The first OI history
starts only when the collector starts, so old backtests requiring OI remain
unavailable until explicit history exists. Polling gives an observation known at
platform receipt, not the exchange's exact internal change time.

The shared commit clock permits one mixed-fact dataset watermark. Existing
dataset manifests created under the prior provenance hash are archived by the
manual migration rather than read through compatibility logic.

## Rejected Alternatives

- Put OI, funding, or basis columns on candle rows.
- Let each bot start its own poller or call a provider on read miss.
- Infer underlying instruments from symbols.
- Treat poll time as a fabricated Coinbase event timestamp.
- Store only the latest OI value or overwrite prior samples.
- Use in-process locks without database ownership fencing.
- Claim Coinbase historical OI acquisition where no supported endpoint exists.
- Implement a provider-agnostic payload before a second fact contract requires
  it.

## Enforcing Tests Or Evidence

- `tests/test_market_data/test_contracts.py` and
  `tests/test_market_data/test_requirements.py` enforce typed facts, roles,
  deterministic plans, known-at selection, staleness, and fail-loud inputs.
- `tests/test_data_providers/test_coinbase_provider.py` enforces Coinbase OI
  response validation without live credentialed calls.
- `tests/test_market_data/test_collector_service.py` and
  `tests/test_market_data/test_collection_repository_db.py` enforce scheduled
  identity, retries, pacing, leases, missed schedules, and fencing.
- `tests/test_market_data/test_repository_db.py` enforces append-only OI,
  corrections, the shared commit clock, and mixed frozen datasets.
- `tests/test_market_data/test_backtest_dataset.py` and
  `tests/test_market_data/test_runtime_delivery.py` enforce frozen warmup
  coverage, provider-free latest-known delivery, no look-ahead, stale-input
  rejection, and per-bar input clearing.
- A clean PostgreSQL/TimescaleDB bootstrap and repeated fresh-process bootstrap
  were validated on an isolated database; the fact-clock migration and mixed
  repository tests also passed there.

## References

- [ADR 0044: Enforce Known-At Prefix Invariance](0044-enforce-known-at-prefix-invariance.md)
- [ADR 0047: Fence Async Job Ownership](0047-fence-async-job-ownership.md)
- [ADR 0050: Use One Canonical Append-Only Market-Data Store](0050-use-one-canonical-append-only-market-data-store.md)
- [ADR 0051: Require Frozen Datasets For Canonical Backtests](0051-require-frozen-datasets-for-canonical-backtests.md)
