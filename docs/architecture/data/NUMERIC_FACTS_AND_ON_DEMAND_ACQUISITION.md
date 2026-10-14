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
  - src/market_data/canonical.py
  - src/market_data/fact_registry.py
  - src/market_data/requirements.py
  - src/market_data/store.py
  - src/data_providers/numeric_facts.py
  - src/data_providers/providers/chainlink.py
  - src/data_providers/providers/coinalyze.py
  - src/data_providers/registry.py
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
  - tests/test_data_providers/test_coinalyze_provider.py
---
# Numeric Facts And On-Demand Acquisition

## Purpose

This component admits provider-published scalar observations whose precision,
units, dimensions, event identity, and causal provenance must remain exact. It
adds a provider-neutral storage and dataset path without making providers part
of consumer reads or creating a table per upstream feed.

The registered numeric contracts and adapters include:

| Fact contract | Subject | Required dimension | Unit rule | Value domain |
| --- | --- | --- | --- | --- |
| `market.reference_price.v1` | canonical instrument | `quote_currency` | equals `quote_currency` | greater than zero |
| `market.reserve_balance.v1` | canonical instrument | `reserve_unit` | equals `reserve_unit` | greater than or equal to zero |
| `derivatives.liquidation_volume.v1` | canonical instrument | liquidated side, bucket seconds, volume unit, exchange, product | exact catalogue denomination | nonnegative; missing is not zero |
| `chainlink_aggregator_v3.v1` | acquisition adapter | manifest-bound | provider value scaled by verified decimals | read-only public EVM evidence |

This component does not implement arbitrary multi-variable response feeds, sequencer
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

At the acquisition boundary, the typed `NumericFact` projection carries:

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
timeframe, and normalized contract dimensions. These numeric contracts
forbid a candle timeframe. A liquidation observation carries its distinct bucket
duration as a contract dimension; it is never implicitly resampled or carried
forward as a candle. Currency/unit dimensions are uppercased. A dimension that
changes fact meaning therefore produces a different series ID.

## Event Identity, Revisions, And Reorgs

The source event key identifies the same logical observation across retries.
The row hash includes exact value material, raw value, dimensions, causal times,
source-event keys, state, and the source-event material hash. The material hash
captures source identity that can change under a reorg, including Chainlink
phase/round, block hash, transaction hash and position, log position,
confirmation block/hash, answer, and feed metadata.

Repository ingest canonicalizes that projection into the registered payload and
one `CanonicalFact` envelope. It has three outcomes:

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
- an endpoint reference: Chainlink uses an environment-variable name; registered
  fixed-endpoint adapters use their declared opaque ID, never a secret URL;
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

## Coinalyze liquidation history (candidate implementation)

`coinalyze_liquidation_history.v1` uses the existing provider registry/factory,
encrypted credential schema and `NumericFactAcquisitionService`. It is an
additive adapter and payload registration; there is no new table, collector,
Check evaluator or migration. This section describes the branch implementation,
not evidence of deployment or authenticated source qualification.

Each reviewed binding admits one BTC product and one liquidated side (`long`
or `short`). Series identity includes the source exchange code, native product,
bucket duration and volume unit as well as the canonical instrument. Longs and
shorts share a source event group but have separate component keys. Two bindings
therefore require two bounded acquisitions; this deliberately reuses scalar
Facts rather than introducing a new paired storage path. Existing event-fact
Checks already support numeric inputs, causal selection and gap exclusions.

The adapter verifies the pinned product against `/future-markets` before reading
`/liquidation-history`. Config contains exactly `product`, `interval`, opaque
`credential_ref` and `confirmations: 0` (the existing coverage field; REST has no
block finality). Product pins `symbol`, `exchange`, `symbol_on_exchange`,
`base_asset`, `quote_asset`, `is_perpetual`, `expire_at` and
`oi_lq_vol_denominated_in`. Perpetual absent/zero expiry normalizes to null; a
dated contract requires its millisecond expiry. Unit is the catalogue's base
asset, quote asset or `CONTRACTS`, never an assumed USD conversion. Metadata
changes require a reviewed binding; changed product/unit/duration creates a
separate series. No live product or instrument mapping is checked in as verified.

`endpoint_ref` is the registered fixed ID `coinalyze.v1`; source identity is
`COINALYZE / COINALYZE_AGGREGATE / rest_history / coinalyze_liquidation_history.v1`.
The aggregate venue names the API source namespace, not the execution venue.
The exchange and native product remain explicit dimensions and provenance.
The credential schema field `COINALYZE_API_KEY` is an encrypted-store field,
not a supported environment-variable fallback. Construction and binding
validation do not read credentials or use the network. Only explicitly authorized
acquisition loads the referenced credential and sends it in the `api_key` header.

A call requests at most 1,500 closed, aligned buckets for one symbol and charges
two requests (catalogue plus history). The caller's `max_logs` bounds returned
bucket count; `max_blocks` is unused and reports zero. The existing required
positive block budget may be one. Ranges remain half-open in QT; the provider's
inclusive `to` is `end - 1 second`. There is no pagination or automatic retry.
HTTP timeouts, elapsed body-read limits, 16 MiB catalogue and 1 MiB history caps
bound work. Calls in one process are paced at least 1.6 seconds apart; other
processes must still use the coordinated acquisition window. HTTP 429 exposes
sanitized `Retry-After` seconds for an explicit later retry. Errors never retain
headers, keys or upstream error bodies; redirects are rejected.

The provider's `t` is bucket start. Bucket end remains provenance, while the
exact JSON numeric token is retained beside the Decimal value. Source publication
time is unavailable. `received_at` records response receipt and canonical
`known_at` is at least acceptance time (`platform_receipt`), never bucket start
or close. Re-observation of unchanged material is a no-op; changed totals append
a correction with reconciliation time. The prior Fact and frozen commit scope
remain available. Changing response-wide hashes alone does not revise a Fact.

Missing buckets produce coalesced gaps, including an entirely empty response;
returned zero totals remain real observations. Even a full set of returned
buckets currently carries `provider_completeness_unverified` and `partial`
coverage. Bucket presence cannot certify exchange/provider capture completeness.
Partial responses never invalidate older observations and never become durable
complete-range cache hits. This conservative candidate is not an admitted
historical causal research input. The explicit v13 `complete_paired_rows`
retrospective consumer may admit its recorded partial range custody while
retaining every gap and completeness warning; that does not certify coverage. Do not remove its gaps or backdate known-at to
make a Check run. Old intraday history may already be unavailable; daily data
cannot substitute for intraday liquidation bursts.

Before real acquisition, qualify one exact product/instrument mapping, requested
range and actual candle overlap, retention/use rights and a bounded request/row
budget. These are acquisition/science decisions, not extra implementation
frameworks. No key was inspected and no authenticated request was made for the
candidate qualification. Once the registered integration is deployed, the human
can use the existing hidden prompt:

```sh
qt providers credentials add --provider COINALYZE --venue COINALYZE_AGGREGATE --ref coinalyze-research
```

Do not put the key in chat, command arguments, manifests or logs. This handoff
requires the existing encrypted store to be configured; it does not rotate or
replace its master key. Registration readiness does not authorize credential
entry, acquisition or deployment. Rollback disables acquisition/new reads of this
payload on the older release while preserving all retained facts; old releases
do not know the newly registered payload schema. No cleanup is required.

Primary source semantics: [official API documentation](https://api.coinalyze.net/v1/doc/)
and [OpenAPI specification](https://api.coinalyze.net/v1/doc/api-spec.json), reviewed
2026-10-13. The rolling intraday retention and rate limit are documented provider
behavior; receipt-time availability and unverified completeness are QT's explicit
conservative interpretation where the provider makes no historical guarantee.

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

Exact numeric observations now persist only in `market.fact_versions` under a
registered payload schema. `market.series.dimensions`, `market.sources`,
`market.ingestion_runs`, `market.gap_evidence`, the shared commit clock, and the
frozen Dataset tables remain shared canonical infrastructure.
`market.fact_acquisition_coverage` remains separate operational evidence for
bounded historical acquisition. There is no provider-specific DSN or table.

`manual_migration_numeric_fact_store_v1.sql` and the retired
`market.numeric_fact_versions` definition remain immutable migration history,
not supported runtime architecture. The canonical migration transformed all
8,016 retained numeric rows, validated exact values/raw text, event revisions,
known-at, provenance, gaps, coverage links, and Dataset hashes, then deleted the
old table during the hard cutover.

Startup validates the canonical registry/store and coverage relation. It fails
loudly on missing or drifted schema and never recreates a retired fact table.

## OI And Funding Consolidation Status

Physical consolidation is complete. All retained
`derivatives.open_interest.v1` and `derivatives.funding_rate.v1` rows live in
the canonical Fact store, and the specialized tables are absent. Their v1
payload schemas deliberately preserve accepted binary-float evidence and the
historical material/hash algorithms. Typed OI/funding projections remain API
conveniences over canonical records; they are not alternate persistence or
fallback readers.

The v2 payload schemas are registered for a future source path that preserves
provider decimal text and exact values. Existing v1 floats are never relabeled
as exact. Moving an active provider to v2 requires an explicit series/schema
change and consumer proof, not an in-place reinterpretation.

## Failure Contract

- Missing migration-owned objects fail startup with the migration command.
- A disabled, malformed, mismatched, or duplicate manifest binding fails before
  network use.
- Missing endpoint environment reference fails before RPC use.
- Chain, feed metadata, archive, finality, reconciliation, or budget mismatch
  includes binding/source context and fails or records a typed gap.
- Provider failures record failed coverage and gap evidence, log an error, and
  propagate.
- Partial acquisition never proves complete coverage or becomes a cache hit.
  The explicit retrospective complete-paired Check policy may freeze and consume
  recorded partial custody with gaps and uncertainty preserved; ordinary causal
  consumers remain strict.
- A read miss remains a read miss; it never authorizes network access.

## Related Docs

- [Data Boundary](DATA_BOUNDARY.md)
- [Persistence Boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [ADR 0061: Provider-Neutral Exact Numeric Facts](../decisions/0061-use-provider-neutral-exact-numeric-facts-and-bounded-acquisition.md)
- [Chainlink Numeric Facts Operator Guide](../../guides/chainlink-numeric-facts.md)
- [Data Layer](../../engineering/data-layer.md)
