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
  - funding-rate
  - known-at
  - datasets
  - fencing
code_paths:
  - src/market_data/contracts.py
  - src/market_data/requirements.py
  - src/market_data/store.py
  - src/data_providers/facts.py
  - src/data_providers/providers/coinbase.py
  - src/data_providers/registry.py
  - portal/backend/service/providers/provider_service.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/controller/market_data.py
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

Coinbase Advanced Trade exposes current futures open interest and perpetual
funding through its product response. Quant-Trad polls the public Advanced Trade
product endpoint, so neither operation requires account credentials. The
adapter has no supported historical backfill for either fact. OI has no provider
event timestamp, while Coinbase exposes funding time without defining it as a
publication timestamp. A durable poller still needs explicit sampling, known-at,
retry, gap, pacing, and ownership semantics.

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

The second handler is Coinbase venue-specific
`derivatives.funding_rate.v1`. It records a signed fractional rate,
provider-reported funding time, and positive funding interval. Funding time is
preserved but never substituted for source publication or known-at because its
provider semantics are unspecified. Schedule identifies the observation and
platform acceptance after receipt establishes causal visibility. A funding
definition additionally requires a canonical instrument with
`has_funding=true`.

Open-interest and funding revisions each have a typed append-only hypertable and
share the database-wide market fact commit clock with candles. Dataset
manifests and repository reads support all three contracts. Backtest planning
and runtime delivery currently consume OI but not funding; collection does not
implicitly advertise an unbuilt engine consumer path.

## Invariants

- Consumers never declare provider, endpoint, table, poll schedule, or fallback.
- An operation is callable only when declared in the provider feature contract;
  declarations describe implemented Quant-Trad behavior, not every upstream
  provider offering.
- Public operations do not access the provider credential store.
- Instrument relationships use canonical IDs supplied by run configuration; no
  symbol parsing infers underlying or benchmark identity.
- A read miss never calls Coinbase or another provider.
- OI and funding `known_at` never precede schedule, receipt, or platform
  acceptance.
- Provider funding time remains distinct from publication and known-at.
- One scheduled OI or funding sample is idempotent; changed same-schedule values
  are not silently accepted as consumer-path corrections.
- Only the current fenced owner can append a collected fact or complete an
  attempt.
- Required unavailable or stale facts stop execution; optional facts return a
  structured caveat.
- Frozen backtests cannot observe facts beyond the dataset commit watermark or
  facts not known at the decision time.
- Collector failures and missed schedules remain quality evidence, not synthetic
  zeroes or forward-filled truth.
- Historical OI/funding backfill, funding engine delivery, basis, aggregated OI,
  L2, order flow, options, and live order submission remain unsupported.

## Consequences

Collectors can accumulate venue facts continuously and independently of bot or
backtest speed. More providers and fact handlers reuse scheduling and storage
ownership without forcing one universal payload. OI and funding history start
only when their collectors start. Polling gives an observation known at platform
receipt, not the exchange's exact internal change time.

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
- Collapse distinct OI and funding values into a provider-agnostic JSON payload.

## Enforcing Tests Or Evidence

- `tests/test_market_data/test_contracts.py` and
  `tests/test_market_data/test_requirements.py` enforce typed facts, roles,
  deterministic plans, known-at selection, staleness, and fail-loud inputs.
- `tests/test_data_providers/test_coinbase_provider.py` enforces Coinbase OI
  and funding response validation without credentialed calls.
- `tests/test_market_data/test_collector_service.py` and
  `tests/test_market_data/test_collection_repository_db.py` enforce scheduled
  identity, retries, pacing, leases, missed schedules, and fencing.
- `tests/test_market_data/test_repository_db.py` enforces append-only OI and
  funding, causal reads, the shared commit clock, and frozen datasets.
- `tests/test_market_data/test_backtest_dataset.py` and
  `tests/test_market_data/test_runtime_delivery.py` enforce frozen warmup
  coverage, provider-free latest-known delivery, no look-ahead, stale-input
  rejection, and per-bar input clearing.
- Live BIP, ETP, and SLP funding collectors were validated through public
  provider normalization, fenced attempts, PostgreSQL rows, causal `qt` reads,
  repeated schedules, and worker restart recovery.

## References

- [ADR 0044: Enforce Known-At Prefix Invariance](0044-enforce-known-at-prefix-invariance.md)
- [ADR 0047: Fence Async Job Ownership](0047-fence-async-job-ownership.md)
- [ADR 0050: Use One Canonical Append-Only Market-Data Store](0050-use-one-canonical-append-only-market-data-store.md)
- [ADR 0051: Require Frozen Datasets For Canonical Backtests](0051-require-frozen-datasets-for-canonical-backtests.md)
