# Data Layer

Providers acquire observations; the canonical Fact repository owns durable
identity and causal selection. Consumers request typed facts, not provider
endpoints. Use the [data boundary](../architecture/data/DATA_BOUNDARY.md) for
meaning and the [generalized Fact data plane](../architecture/data/GENERALIZED_FACT_DATA_PLANE.md)
for storage and frozen-read details.

## Follow an observation into storage

Adapters under `src/data_providers/` return provider material. Contracts and
registries under `src/market_data/` define type, unit, dimensions, clocks and
versioned encoding. Backend acquisition translates observations through
`portal/backend/service/storage/repos/market_data.py`.

`market.fact_versions` holds canonical version headers; registered payloads,
global identities and tiered locations preserve meaning across storage movement.
Current ORM and startup admission define a clean install. Retired candle, OI,
funding and numeric-version tables are not runtime stores. Historical migrations
are not clean-install input. `PG_DSN` is the one application persistence DSN.

Historical candle intake is explicit and audited. New inputs must be provider-
native at the requested timeframe and frozen directly. Do not aggregate, split,
upsample or synthesize other intervals to satisfy a new input. Pagination still
requests the same native timeframe. Existing internally derived candles retain
original provenance and replay compatibility.

Scheduled polls and continuous streams use the shared collector control plane.
OI/funding are latest-state polls; trade and Level 2 projections additionally own
their raw evidence, coverage and recovery semantics. Chainlink scalar acquisition
is explicitly bounded; structured latest-state polling uses reviewed manifests.
See [collector operations](../guides/collector-operations.md),
[Coinbase scheduled Facts](../guides/coinbase-scheduled-facts.md),
[Chainlink numeric acquisition](../guides/chainlink-numeric-facts.md) and
[Chainlink structured collection](../guides/chainlink-structured-facts.md).

## Separate acquisition from a read

Reads never acquire on a miss. Requirements declare Fact type, version, dimensions
and instrument role. Dataset preparation may acquire only through an explicitly
authorized supported operation with declared bounds. Execution and replay consume
a frozen binding without provider access.

Freeze pins material, provenance, quality and range evidence at its admitted
commit/known-at scope. Dataset identity does not certify every consumer's
readiness: each Indicator, Check or backtest owns its input requirements.
Missing or sparse truth remains missing or sparse.

Corrections append revisions. Complete explicit repair may invalidate an event
that disappeared; partial acquisition cannot prove absence. Keep effective,
publication, receipt, acceptance and known-at clocks distinct. Never invent
provider precision or timestamps. Existing v1 floats remain versioned evidence
even when newer payload contracts preserve exact decimals.

## Extend the existing boundary

Register typed payload and Fact semantics before adding a provider. Reuse source/
series identity, ingestion, gaps, acquisition coverage, Dataset freeze and replay.
Atomic structured observations remain atomic; exact numeric material is one
payload shape, not a universal scalar store.

Follow [adding a provider](../guides/adding-a-provider.md) for adapter, credential,
endpoint, precision and qualification requirements. Storage changes use the
explicit [preserving deployment procedure](server-deployment.md#storage-move-and-recovery);
startup validates existing data rather than silently migrating it.
