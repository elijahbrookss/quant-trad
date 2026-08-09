# Adding A Provider

This is the minimal provider extension checklist. A provider is an acquisition
adapter; it is not a schema owner, consumer fallback, or runtime dependency.

## Where Providers Live

Provider code and binding contracts live under:

- [provider adapters](../../src/data_providers/providers/)
- [provider registry](../../src/data_providers/registry.py)
- [provider factory](../../src/data_providers/providers/factory.py)
- [numeric provider protocol and manifests](../../src/data_providers/numeric_facts.py)
- [backend provider services](../../portal/backend/service/providers/)
- [numeric acquisition service](../../portal/backend/service/market/numeric_fact_acquisition.py)
- [fact contract registry](../../src/market_data/fact_registry.py)

Feed deployments, addresses, expected metadata, units, and dimensions belong in
strict manifests or canonical registries, not Python switch statements.

## Adapter Concept

An adapter isolates external API/client behavior and returns typed provider
observations to Quant-Trad services. It must not leak provider-specific
credentials, pagination, symbols, RPC quirks, or response error guesses into
strategy or runtime code. It never creates tables, chooses canonical series,
freezes datasets, or fetches because a consumer read missed.

Errors include operation, provider, venue/network, product or binding, and range
context. Do not swallow or translate them into an empty successful result.

## Credential And Endpoint Contract

Authenticated providers use the credential-reference boundary:

- declare required and optional secret keys in `src/data_providers/registry.py`;
- resolve secrets through `src/data_providers/services/credential_store.py`;
- return only safe credential metadata from backend/API/UI/CLI reads;
- never add provider API keys to `src/core/settings.py`,
  `config/defaults.yaml`, bot environment, or run config.

CLI setup should work without exposing secrets in logs:

```bash
qt setup provider coinbase
```

For agent/automation workflows, prefer stdin or environment-variable mapping:

```bash
qt setup provider coinbase \
  --secrets-json - \
  --no-input
```

A public exact-numeric manifest declares only an environment-variable name in
`endpoint_ref`; it never contains the endpoint URL. The backend resolves that
reference only after explicit network authorization. If a JSON-RPC URL embeds a
provider token, manage the environment value as a deployment secret and never
commit or log it. Durable provenance stores the reference name, not the URL.

## Candle Requirements

Candles must preserve source-data truth:

- include stable timestamps;
- preserve OHLCV semantics;
- be ordered deterministically;
- avoid duplicate bars;
- expose missing data instead of silently filling it;
- carry enough provider/venue/symbol/timeframe context for diagnostics.

## Exact Numeric Fact Requirements

Before adding a scalar numeric source, decide whether its semantics fit the
provider-neutral exact storage shape. Add or reuse a fact-registry contract that
declares:

- stable fact type and contract version;
- `storage_shape="exact_numeric"` and exact decimal numeric type;
- subject type and record-time field;
- required/optional dimensions and every meaning-changing series dimension;
- unit rule and valid value domain;
- dataset eligibility and alignment semantics.

Do not pass a binary float across the adapter boundary. Preserve the original
provider value as text and parse the normalized value as `Decimal`, integer, or
decimal string. Include distinct effective, publication, receipt, acceptance,
and known-at evidence where available.

Implement `NumericFactProvider.fetch_current` and/or
`fetch_history`. Each batch must return:

- stable source event/group/component keys;
- exact value and raw value;
- source-event material sufficient to detect a provenance-only reorg;
- typed provenance and explicit gaps;
- half-open covered range and source positions;
- `complete` or `partial` status;
- measured request, log, and block use.

Historical capability must be proven, not inferred from a current endpoint.
Bound page/range sizes and retries. A successful zero-event scan may be complete;
a denied page, unavailable phase/archive, unresolved event, budget exhaustion,
or failed reconciliation must be a gap or failure. Never advertise partial
history as an empty complete range.

Add strict `market.numeric_fact_sources.v1` reference manifests under
`config/market-data/numeric-facts/`. Both root and binding stay disabled in
source control. A binding declares canonical instrument and role,
fact/unit/dimensions, schedule, quality and risk policy, source identity,
endpoint reference, and adapter configuration including its historical lower
bound. The manifest loader rejects unknown or missing root/binding fields,
invalid roles and policy metadata, unsupported contracts, and unit/dimension
mismatch. Acquisition/provider construction separately validates source and
adapter configuration shape; live feed metadata mismatch fails loud before
ingest.

If the new fact fits this contract, reuse `market.numeric_fact_versions`,
`market.fact_acquisition_coverage`, canonical gaps, the shared fact commit
clock, and frozen datasets. Do not add provider-specific tables, DSNs, caches,
repository branches, or migrations.

## Canonical Intake

Adapters acquire observations only. Explicit historical candle intake validates
and persists through `HistoricalCandleIngestor`; paper intake persists closed
candles before runtime visibility. Explicit exact-numeric intake validates
authorization and budgets, consults matching complete coverage, constructs the
provider only for missing ranges, and persists through the canonical
market-data repository.

Consumer reads use canonical storage and never call an adapter on a miss.
Dataset preparation may acquire only when the caller explicitly supplies
`acquire_missing` and the relevant authority. Backtest admission, execution,
replay, strategy/indicator evaluation, and reporting are provider-free.

A new physical storage shape requires material value/temporal pressure and a
coordinated contract/schema/migration decision. Do not add optional fields to
candle, OI, or funding tables. Do not let an adapter issue DDL.

## Gap, Correction, And Reorg Handling

Sparse source truth remains sparse. Do not synthesize missing observations
unless a future contract models synthetic facts separately.

Classify gaps conservatively and retain request/capability evidence. Only
matching `complete` acquisition coverage is reusable. Partial and failed rows
remain diagnostic and missing.

Retries are idempotent by stable source event key. Changed value, causal
material, state, or source-event material hash appends a correction revision.
Only a complete explicit repair can invalidate an active event that disappeared.
Never update or delete accepted facts to hide a reorg.

## Migration Ownership

The shared exact-numeric tables are already owned by
`scripts/db/manual_migration_numeric_fact_store_v1.sql`. A new compatible
provider or binding must not change that schema.

Those tables are a scoped explicit-migration exception: startup validates its
required subset but does not create or repair them. Existing legacy behavior is
otherwise unchanged. If a genuinely new storage shape is approved, coordinate
its contract, ORM, manual migration ordering, startup validation, tests, and
architecture docs under one owner.

## OI And Funding Warning

Do not route `derivatives.open_interest.v1` or
`derivatives.funding_rate.v1` through exact numeric storage.
**`NUMERIC_FACT_CONSOLIDATION_DEFERRED`** preserves their specialized v1 rows
and frozen datasets because retained floats cannot recover raw decimal truth and
funding interval is not a v1 series dimension. Any future consolidation uses new
v2 contracts/series and must prove v1 dataset identities unchanged.

## Testing Expectations

Add focused tests for the capabilities introduced:

- provider registry routing, aliases, and authentication declaration;
- credential schema/metadata or endpoint-reference behavior;
- provider factory/build behavior;
- exact value and raw value preservation with large/high-precision fixtures;
- float rejection, unit/dimension/value-domain validation;
- stable event identity, source-event material, corrections, and reorgs;
- distinct effective/publication/known-at clocks;
- bounded pagination/ranges/retries and budget accounting;
- complete zero-event coverage and cached no-network reuse;
- explicit partial/failed gaps and default-deny network authorization;
- dataset freeze and provider-free replay;
- strict disabled reference manifests and metadata quarantine;
- failure context without swallowed errors.

Useful examples:

- [provider contract tests](../../tests/contract/providers/)
- [data provider tests](../../tests/test_data_providers/)
- [Chainlink provider tests](../../tests/test_data_providers/test_chainlink_provider.py)
- [numeric fact contract tests](../../tests/test_market_data/test_numeric_fact_contracts.py)
- [numeric acquisition tests](../../tests/test_market_data/test_numeric_fact_acquisition.py)
- [Coinbase runtime session tests](../../tests/integration/runtime/test_coinbase_runtime_session.py)

## Next

- Data overview: [data layer](../engineering/data-layer.md).
- Provider boundary: [data boundary](../architecture/data/DATA_BOUNDARY.md).
- Exact numeric architecture: [numeric facts and on-demand acquisition](../architecture/data/NUMERIC_FACTS_AND_ON_DEMAND_ACQUISITION.md).
- Chainlink operation: [Chainlink numeric facts](chainlink-numeric-facts.md).
- Archive-backed example: [Binance futures public data setup](binance-futures-public-data.md).
- Live-stream/paper example: [Coinbase derivatives paper setup](coinbase-derivatives-paper-setup.md).
- Runtime contract: [runtime contract](../contracts/platform/01_runtime_contract.md).
