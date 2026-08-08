---
component: numeric-facts-on-demand-acquisition
subsystem: data
layer: service
doc_type: architecture
status: active
tags:
  - market-data
  - exact-numeric
  - provider-neutral
  - chainlink
  - acquisition
  - on-demand-acquisition
  - known-at
  - provenance
  - reorgs
  - coverage
  - datasets
  - explicit-migration
code_paths:
  - src/market_data/contracts.py
  - src/market_data/fact_registry.py
  - src/market_data/requirements.py
  - src/market_data/store.py
  - src/data_providers/numeric_facts.py
  - src/data_providers/providers/chainlink.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market/numeric_fact_acquisition.py
  - portal/backend/service/market/backtest_dataset_service.py
  - portal/backend/service/market/runtime_market_data.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - scripts/db/manual_migration_numeric_fact_store_v1.sql
  - config/market-data/numeric-facts
  - tests/test_market_data/test_numeric_fact_contracts.py
  - tests/test_market_data/test_numeric_fact_acquisition.py
  - tests/test_data_providers/test_chainlink_manifest.py
  - tests/test_data_providers/test_chainlink_provider.py
---
# Numeric Facts And On-Demand Acquisition

## Purpose

This component admits provider-published scalar observations whose precision,
units, dimensions, event identity, and causal provenance must remain exact. It
adds a provider-neutral storage and dataset path without making providers part
of consumer reads or creating a table per upstream feed.

The first admitted facts and source adapter are:

| Fact contract | Subject | Required dimension | Unit rule | Value domain |
| --- | --- | --- | --- | --- |
| `market.reference_price.v1` | canonical instrument | `quote_currency` | equals `quote_currency` | greater than zero |
| `market.reserve_balance.v1` | canonical instrument | `reserve_unit` | equals `reserve_unit` | greater than or equal to zero |
| `chainlink_aggregator_v3.v1` | acquisition adapter | manifest-bound | provider value scaled by verified decimals | read-only public EVM evidence |

This component does not implement multi-variable response feeds, sequencer
uptime, continuous collection, websocket subscriptions, wallet/signer flows, or
transactions.

## Ownership

| Layer | Owns | Must not own |
| --- | --- | --- |
| fact registry | canonical type/version, numeric type, unit, dimensions, value domain, record time, dataset eligibility | provider endpoints or SQL routing branches |
| numeric manifest | reviewed binding from adapter/source/feed to canonical instrument and fact semantics | secret endpoint values or runtime acquisition authority |
| provider adapter | source calls, exact translation, phase/log discovery, event identity, source clocks, provenance, gaps, measured budget use | schema, series policy, dataset freeze, consumer fallback |
| acquisition service | explicit authorization, missing-range planning, provider construction, persistence, repair, coverage evidence, lifecycle logs | strategy decisions or automatic read-miss acquisition |
| repository | source/series identity, append-only revisions, causal reads, gap/coverage evidence, frozen material | provider-specific RPC behavior |
| dataset preparation | transitive requirements, optional explicit acquisition, post-acquisition coverage validation, freeze | provider access during admission or execution |

The dependency direction is:

`consumer requirement -> fact contract -> canonical series -> local revisions`

Only an explicit operation takes the separate acquisition path:

`operator authorization -> manifest binding -> provider adapter -> canonical ingest`

## Canonical Fact Contract

`NumericFact` carries:

- exact normalized `value` and unchanged `raw_value`;
- contract-owned `unit` and normalized `dimensions`;
- `effective_at` plus the method that establishes it;
- optional source publication and platform receipt times;
- platform `accepted_at`;
- causal `known_at` plus its method;
- stable source event, optional group, and optional component keys;
- `active` or `invalidated` state;
- source-event material hash and canonical row hash.

Values may enter as `Decimal`, integer, or decimal string. Binary floats,
non-finite values, missing raw values, undeclared dimensions, invalid units, and
out-of-domain values fail before storage. PostgreSQL stores the normalized value
as unbounded `numeric`, not `double precision`, and retains the raw source text
independently.

Series identity is canonical instrument, fact type, contract version, optional
timeframe, and normalized contract dimensions. The two v1 numeric contracts
forbid a timeframe. Currency/unit dimensions are uppercased. A dimension that
changes fact meaning therefore produces a different series ID.

## Event Identity, Revisions, And Reorgs

The source event key identifies the same logical observation across retries.
The row hash includes exact value material, raw value, dimensions, causal times,
source-event keys, state, and the source-event material hash. The material hash
captures source identity that can change under a reorg, including Chainlink
phase/round, block hash, transaction hash and position, log position,
confirmation block/hash, answer, and feed metadata.

Repository ingest has three outcomes:

- identical material for an existing event is an idempotent no-op;
- changed material appends the next event revision and shared market commit
  sequence;
- a complete explicit repair appends `invalidated` revisions for previously
  active source events that disappeared from the rescanned range.

A partial repair never invalidates disappeared events because the absence was
not proven. Historical revisions remain readable at their original commit
scope. Accepted fact, gap, and coverage rows reject update and delete.

## Causal Clocks

Numeric facts keep source and platform clocks separate. For Chainlink
AggregatorV3:

| Field | Evidence |
| --- | --- |
| `effective_at` | AggregatorV3 round `updatedAt` |
| `effective_at_method` | `chainlink_round_updated_at` |
| `source_published_at` | timestamp of the EVM block containing `AnswerUpdated` |
| `accepted_at` | time Quant-Trad accepts the observation |
| `known_at` | timestamp of `event_block + confirmations` |
| `known_at_method` | `evm_confirmation_block` |

Historical platform acceptance may occur long after the source-known time. The
two fields are intentionally not collapsed. Runtime latest-known resolution
uses the frozen fact's declared known-at semantics and commit scope; it does not
pretend the platform stored the row at the original source time.

## Manifest Contract

`market.numeric_fact_sources.v1` is strict and hash-addressed. Each binding
declares exactly:

- ID and explicit enabled state;
- adapter ID, canonical instrument, and declared instrument role;
- fact type, contract version, unit, and dimensions;
- an environment-variable name in `endpoint_ref`, never the endpoint value;
- provider/venue/source-kind/adapter-version identity;
- expected update/deviation schedule metadata;
- maximum-staleness policy whose only v1 behavior is an explicit gap;
- reviewed official-catalog, risk-tier, deprecation, and verification metadata;
- adapter-specific, validated configuration.

For Chainlink the configuration pins chain ID, network, proxy, deployment block
lower bound, explicit `history_start`, confirmation depth, maximum log page
span, current lookback, expected decimals and description, and optionally proxy
version. Feed addresses, roles, quality/risk policy, and expectations live in
data rather than Python conditionals.

Both the manifest and selected binding must be enabled. Checked-in ETH/USD and
TUSD-reserves reference manifests are disabled at both levels. They document
verified bindings but do not authorize acquisition.

## Chainlink Historical Acquisition

The adapter uses public JSON-RPC and performs a bounded, phase-aware scan:

A start before the binding's `history_start` fails before RPC planning. An end
after the confirmed-head timestamp preserves the confirmed portion, records a
`chainlink_range_unconfirmed` gap for the trailing interval, and returns a
partial batch rather than certifying future/unconfirmed coverage.

1. Verify chain ID, head, configured confirmation depth, deployment-block read,
   and confirmed-head block/hash.
2. Read proxy decimals, description, version, current phase, and aggregator;
   quarantine any configured metadata mismatch before log acquisition.
3. Binary-search block timestamps for the requested half-open time range and
   reject a scan exceeding `max_blocks`. Block zero is accepted as the
   legitimate genesis lower bound when its timestamp is zero; other source
   timestamps remain strictly positive.
4. Read the proxy phase at both bounded block endpoints and scan only the
   inclusive phase range active within the request. If archive state calls are
   unavailable, emit a warning and fall back to all configured phases so
   completeness is not weakened. A required phase that cannot be resolved
   becomes explicit gap evidence.
5. Page `AnswerUpdated` logs by both the manifest's `max_log_span` and the
   operation's request/log budgets.
6. Rebuild the proxy round ID from phase and local round, call `getRoundData`,
   and reconcile event answer, update time, round ID, and `answeredInRound`.
7. Reject unconfirmed events, record block/transaction/log/finality provenance,
   and retain any unresolved round as a gap.
8. Reconcile the acquired range with `latestRoundData` when the latest round is
   inside the range.

The returned batch is `complete` only when no gap remains. A successful scan
with no events is still complete evidence for that range. Unsupported archive
reads, denied log ranges, unavailable phases, unresolved rounds, finality
limits, budget exhaustion, and latest-round mismatch fail or produce explicit
partial evidence; they are never translated into a complete empty history.

Current acquisition scans only the current phase within the bounded configured
lookback and selects the newest confirmed `AnswerUpdated` event. It still
reconciles the proxy's latest round. If a newer proxy round is not yet confirmed,
the batch is partial rather than silently presenting the prior round as fully
current. If the confirmed observation exceeds the binding's
`quality_policy.max_staleness_seconds`, it is retained with a
`chainlink_latest_round_stale` gap and the batch is partial.

A current read persists its fact and any gap evidence but never writes reusable
historical coverage. Only a full historical batch scans every resolved phase
and can certify an interval for the missing-range cache.

HTTP JSON-RPC requests are uniformly paced at a 0.5-second minimum interval by
default. Deployments may set `CHAINLINK_RPC_MIN_INTERVAL_SECONDS` to a
nonnegative numeric override appropriate for the reviewed endpoint. Transient
JSON-RPC errors retry only within `max_retries`, with bounded exponential delay;
each attempt consumes the request budget. No retry widens a block or log range.

## Authorization, Budgets, And Coverage Cache

`NumericAcquisitionAuthorization` defaults `network_allowed` to false. Provider
construction occurs only after a missing historical range has been found and
authorization has been validated with non-empty actor and reason. Current
acquisition always requires that authorization.

`NumericAcquisitionBudget` requires positive limits for requests, returned logs,
and scanned blocks plus a nonnegative retry count. Provider-reported usage is
checked against the remaining operation budget before persistence.

`market.fact_acquisition_coverage` stores immutable interval evidence keyed by:

- series and source IDs;
- binding ID and full manifest hash;
- the binding source's `adapter_version` as interface identity;
- confirmation depth;
- half-open time range and source positions;
- `complete`, `partial`, or `failed` status;
- optional ingestion run and structured evidence.

Only matching `complete` intervals satisfy historical cache lookup. Adjacent or
overlapping complete intervals may cover a request without a provider call.
Partial and failed intervals remain diagnostic evidence but stay missing for
future acquisition. Manifest, interface, source, binding, series dimensions, or
confirmation-depth changes deliberately form a different cache identity.

## Dataset And Runtime Integration

Requirements carry exact fact type, contract version, instrument role,
dimensions, alignment, staleness, and gap policy. Dataset planning resolves the
canonical series; it does not resolve a provider.

When required coverage is missing, preparation fails by default. With explicit
`acquire_missing`, it may acquire candles through their existing intake and
exact numeric facts only when the request also supplies:

- enabled manifest/binding references matching each required canonical series;
- explicit network authorization with actor and reason;
- one bounded numeric acquisition budget.

Preparation rechecks canonical coverage after acquisition. A partial numeric
result or remaining required gap stops freeze. A successful freeze records the
selected numeric series and its complete revision chain through the shared
market commit watermark in `market_dataset.v1`. The frozen `row_count`, material
hash, and provenance hash cover every retained revision, including corrections
and invalidations; they are not hashes of only the latest visible event rows.
Runtime reads that frozen chain locally and applies the causal known-at selector
for each decision time. Startup, worker execution, replay, strategy/indicator
evaluation, reports, and repeated runs cannot construct a provider.

## Persistence And Migration Ownership

The additive manual migration creates or extends only these numeric surfaces:

- `market.series.dimensions` and its object constraint;
- `market.numeric_fact_versions` with unbounded numeric value, raw value,
  event-revision primary key, causal fields, provenance, hashes, shared commit
  clock indexes, and mutation-rejection trigger;
- `market.fact_acquisition_coverage` with bounded lookup identity, status/range
  constraints, evidence, and mutation-rejection trigger.

It reuses `PG_DSN`, `market.sources`, `market.series`,
`market.ingestion_runs`, `market.gap_evidence`, `market.fact_commit_seq`, and
the existing frozen dataset tables. There is no provider-specific DSN or table.

These new tables are an explicit-migration exception to generic startup DDL.
Startup does not create or repair them. It fails with the migration path if the
tables or dimensions column are absent; if the numeric-fact primary key,
unbounded numeric type, required numeric-fact checks/indexes, required coverage
indexes, or either immutable trigger are missing or drifted. The migration also
owns the dimensions-object constraint, coverage keys/checks, and commit-clock
defaults; startup does not currently re-derive every one of those definitions.
Generic bootstrap behavior for all legacy/model-owned schema objects is
unchanged.

## OI And Funding Consolidation Status

**`NUMERIC_FACT_CONSOLIDATION_DEFERRED`** is the current gate result.
`derivatives.open_interest.v1` and `derivatives.funding_rate.v1` remain
specialized facts because their Coinbase adapter and database rows already use
binary floats and do not retain the original raw decimal strings. Funding
interval is also row-scoped rather than part of its v1 series identity. Frozen
v1 datasets pin the specialized series, revisions, commit scopes, ingestion
provenance, row hashes, and material hashes; rerouting the same identity would
change old evidence.

A bounded v2 follow-up must preserve provider decimal text and exact values at
the adapter boundary, make funding interval a series dimension, introduce new
contract/series identities, migrate every explicit consumer, and prove existing
v1 dataset IDs, hashes, and reads unchanged. It must not fabricate exact v2
values from v1 floats. The specialized v1 tables remain immutable evidence.

## Failure Contract

- Missing migration-owned objects fail startup with the migration command.
- A disabled, malformed, mismatched, or duplicate manifest binding fails before
  network use.
- Missing endpoint environment reference fails before RPC use.
- Chain, feed metadata, archive, finality, reconciliation, or budget mismatch
  includes binding/source context and fails or records a typed gap.
- Provider failures record failed coverage and gap evidence, log an error, and
  propagate.
- Partial acquisition never satisfies required dataset coverage.
- A read miss remains a read miss; it never authorizes network access.

## Related Docs

- [Data Boundary](DATA_BOUNDARY.md)
- [Persistence Boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [ADR 0061: Provider-Neutral Exact Numeric Facts](../decisions/0061-use-provider-neutral-exact-numeric-facts-and-bounded-acquisition.md)
- [Chainlink Numeric Facts Operator Guide](../../guides/chainlink-numeric-facts.md)
- [Data Layer](../../engineering/data-layer.md)
