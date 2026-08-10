---
component: generalized-fact-data-plane
subsystem: data
layer: boundary
doc_type: architecture
status: active
tags:
  - market-data
  - canonical-facts
  - typed-payloads
  - schema-registry
  - provider-neutral
  - known-at
  - provenance
  - datasets
  - replay
  - migration
code_paths:
  - src/market_data/contracts.py
  - src/market_data/canonical.py
  - src/market_data/canonical_adapters.py
  - src/market_data/fact_registry.py
  - src/market_data/store.py
  - src/data_providers
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market
  - portal/backend/service/research
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - cli/mcp_server.py
  - scripts/db
  - tests/test_market_data
---
# Generalized Fact Data Plane

## Status

ADR 0063 is accepted and this is the active contract. The schema registry,
canonical envelope, append-only `market.fact_versions` store, causal selector,
Dataset freeze, and frozen read binding own every active market Fact family.
The 17 superseded family version tables are absent after the hard cutover; no
runtime dual write, fallback read, or compatibility flag remains.

Level 2 validity intervals, reconstruction state, raw archives, checkpoints,
acquisition coverage, and gap evidence remain typed operational evidence around
canonical Facts. They are not alternate Fact stores. The migration discovery
report is now a historical pre-cutover inventory.

## Boundary

```text
external provider or deterministic derivation
  -> provider/source-specific acquisition and decoding
  -> CanonicalFact[RegisteredPayload]
  -> append-only canonical Fact storage
  -> Dataset planning and freeze
  -> provider-disabled replay
  -> Indicator / Check / research consumer
```

Provider identity is provenance after canonicalization. It is not a payload
decoder, storage router, or research semantic below the Fact boundary.

## Canonical Envelope

Every durable Fact version carries the following normalized fields:

| Field | Meaning |
| --- | --- |
| `fact_version_id` | stable immutable revision identity |
| `series_id` | canonical subject, fact type, contract version, timeframe, and dimensions |
| `observation_key` | schema-owned provider-neutral observation identity within the series |
| `revision` | positive append-only revision for that observation |
| `market_commit_seq` | shared accepted-order watermark |
| `fact_type` | canonical semantic family, matching the series |
| `payload_schema_id` | exact immutable payload interpretation |
| `observation_time` | schema-declared event/effective/sample/interval identity clock |
| `observation_time_method` | how that clock was established |
| `source_published_at` | optional source publication clock |
| `received_at` | optional QT receipt clock |
| `accepted_at` | QT persistence acceptance clock |
| `known_at` | causal availability clock |
| `known_at_method` | how causal availability was established |
| `source_id` | canonical source/provenance identity |
| `ingestion_run_id` | optional acquisition/derivation run identity |
| external event keys | optional provider object/group/component identities |
| `state` | active or explicit invalidation revision |
| `payload` | strict schema-registered atomic value |
| provenance/quality | versioned evidence documents and hashes |
| hashes | payload, material, provenance, quality, and causal row identity |

Envelope fields are not duplicated inside new payload schemas. Historical
schema codecs may read retained source fields during migration, but canonical
serialization emits one owner for every clock and identity.

## Payload Registry

The code and database share an immutable registry. Each row contains at least:

- payload schema ID and fact type;
- strict field contract and contract hash;
- observation identity/time rules;
- unit and dimension rules;
- exactness and canonical encoding rules;
- invalidation and dataset eligibility;
- material/row hash algorithm versions;
- typed codec ID;
- query-field declarations and required index/projection identities.

Registration fails when the code-owned contract hash differs from PostgreSQL.
A write fails before persistence for an unknown schema, missing/unknown field,
wrong JSON kind, invalid decimal/timestamp representation, domain violation,
non-deterministic encoding, series mismatch, or causal-clock violation.

Payload JSONB is bounded by that registry. It is not a general metadata bag.
Provider-specific or operational detail belongs in versioned provenance.
Array fields are also schema-owned: their item object declares exact required,
nullable, enum, numeric-domain, and additional-property rules. Python and
PostgreSQL validate the same nested contract before accepting a Fact.

## Initial Payload Schemas

The migration must register the real retained families, including:

| Schema | Shape | Notes |
| --- | --- | --- |
| `candle.ohlcv.v1` | structured OHLCV interval | preserves retained float evidence and v1 hashes |
| `derivatives.open_interest.v1` | scalar quantity | preserves retained float evidence |
| `derivatives.open_interest.v2` | exact scalar quantity plus raw value/unit | registered for sources that preserve exact provider text; not fabricated from v1 floats |
| `derivatives.funding_rate.v1` | rate, funding time, interval, unit | preserves retained float evidence |
| `derivatives.funding_rate.v2` | exact rate/raw rate, funding time, interval, unit | registered for sources that preserve exact provider text; not fabricated from v1 floats |
| `market.reference_price.v1` | exact scalar/reference unit | migrates current Chainlink rows |
| `market.trade.v1` | structured trade | retains provider delivery/source position evidence |
| `market.trade_flow.v1` | structured causal aggregate | retains coverage and completeness evidence |
| `market.l2_book.v1` | atomic snapshot/update with strict ordered price-level entries | operational and not directly dataset-eligible; full state remains reconstructable from typed entries and raw archive evidence |
| `market.bbo.v1` | atomic best-bid/offer state | retains book validity, state hash, exact quantities, and input fingerprint |
| `market.depth_band.v1` | one fixed-band depth observation | retains exact bid/ask depth, notional, imbalance, and book evidence |
| `market.trade_flow_feature.v1` | derived flow/CVD feature | retains the aggregate witness and exact flow measures |
| `market.futures_spot_basis.v1` | paired futures/spot state | retains exact mids, staleness, basis, and source witnesses |
| `market.derivative_state.v1` | OI/funding state | keeps nullable source samples and causal commit watermarks explicit |
| `market.market_response.v1` | structured flow/liquidity response | keeps direction, depth, replenishment, impact, and ordered source evidence atomic |
| `market.normalized_feature.v1` | typed scalar/invalid feature | retains spec and input evidence |
| `asset.reserve_state.v1` | atomic reserve report | exact reserve quantity, reserve asset/unit, and report identity; attestation time remains the envelope observation time |

`asset.reserve_state.v1` is the first production structured-provider contract.
It intentionally models only fields established by the selected feed. It does
not add liability, NAV, collateralization, or supply fields that the source does
not publish. Those meanings require separate schema versions backed by current
provider evidence.

## Queryability

Every Fact is addressable by series, observation time/key, revision, known-at,
commit sequence, source, schema, and state through relational columns and
B-tree indexes. Payload/provenance GIN indexes support bounded inspection.

Schema declarations identify hot payload fields. Exact decimals use canonical
strings and receive numeric expression indexes where range/filter operations
are supported. Timestamp fields receive timestamp expression indexes when they
drive window queries. Large repeated payload children use typed child tables
with a parent Fact/hash foreign key.

A payload schema is incomplete until its intended query plan is declared and
tested. Consumers do not issue arbitrary provider-shaped JSON queries.

## Determinism And Hashes

Canonical encoding sorts object keys, normalizes timestamps to UTC, normalizes
exact decimals without exponent ambiguity, forbids non-finite numbers, and
preserves schema-owned list ordering. The payload contract hash participates in
new fact identity.

The schema owns material and row hashing. Migrated historical schemas retain
their v1 algorithms exactly so existing datasets remain reproducible. New
schemas use the generalized Fact hash contract. Hash behavior never changes
in-place.

Frozen datasets pin schema ID and contract hash alongside the existing series,
range, watermark, material, provenance, quality, source, archive, and gap
evidence. Provider-disabled replay validates the registry contract before
decoding payloads.

## Causal Selection

For an evaluation time and frozen commit watermark, the canonical selector:

1. restricts to the required series and half-open observation window;
2. restricts to `market_commit_seq <= watermark`;
3. restricts to `known_at <= evaluation_time` when evaluating causally;
4. chooses the greatest visible revision per observation key;
5. excludes observations whose greatest visible revision is invalidated;
6. orders by schema-declared observation time/key and stable version identity.

This replaces numeric-, candle-, OI-, funding-, trade-, and feature-specific
selection paths. Schema codecs interpret payloads; they do not alter causal
visibility.

## Provenance And Gaps

The source relation retains provider, venue, source kind, adapter/transformation
version, and lineage. Envelope external keys and versioned provenance identify
the external feed/object/request, raw value or response hash, source positions,
finality/revision evidence, and the transformation applied.

Known-at is never reconstructed from an unrelated payload time. Missing clocks
remain missing with an explicit method/limitation. Funding time, attestation
time, valuation time, and source publication remain separate semantic fields.
Provider observation/publication clocks may lead QT's clock because of bounded
clock skew. QT therefore enforces `accepted_at >= received_at` and receipt-based
`known_at >= accepted_at`, but does not fabricate a causal ordering between an
external clock and `known_at`; the original values and skew remain evidence.

Gap evidence remains an append-only interval relation linked to series/source
and detection watermark. Gaps are frozen beside facts. Absence is not encoded
as a made-up payload.

## Acquisition And Collection

Historical acquisition remains explicit and default-deny. Dataset preparation
may request an authorized bounded acquisition only through the existing
operator contract. Cached complete coverage can satisfy the request without a
provider; a mutable read never causes acquisition.

Provider capability determines the mode per fact family:

- practical bounded history: acquire missing ranges, persist canonical Facts,
  then freeze;
- latest/current only: append every meaningful update through a durable
  restart-safe collector and record gaps;
- hybrid: backfill the proven range, record the boundary, then collect forward.

Collectors never overwrite latest state. A new update or correction appends a
Fact revision. No collector is enabled merely because a schema exists.

Chainlink AggregatorV3 scalar feeds use practical bounded historical acquisition
when archive RPC and phase history are available. Chainlink Multiple-Variable
Response feeds expose only their latest bundle through the proxy contract; QT
therefore classifies the selected reserve feed as current-only and accumulates
history with the durable scheduled collector. The checked-in manifest is
reviewed and enabled, but a collection definition is created disabled unless an
operator explicitly passes `--enabled`.

## Consumer Rules

- Dataset planners specify fact type/schema/dimensions and source policy, never
  provider endpoints.
- Indicators declare typed payload inputs or schema-owned scalar projections.
- Checks consume frozen typed projections/Indicator outputs and preserve
  selected Fact evidence hashes.
- Observations derived from Checks retain dataset, schema, Fact, and result
  hashes without provider dependencies.
- UI, CLI, API, MCP, and reports render canonical projections; none calculate
  an alternate reconstruction.
- Provider comparisons may inspect provenance explicitly, but provider identity
  cannot choose payload meaning.

## Scalar And Structured Examples

A scalar reference observation remains a typed payload:

```json
{
  "fact_type": "market.reference_price",
  "payload_schema_id": "market.reference_price.v1",
  "payload": {"value": "118432.125", "raw_value": "118432125000", "unit": "USD"}
}
```

One reserve report remains one atomic observation:

```json
{
  "fact_type": "asset.reserve_state",
  "payload_schema_id": "asset.reserve_state.v1",
  "payload": {
    "report_id": "DE000NXTA018",
    "reserve_asset": "BTC",
    "reserve_quantity": "514.32323119",
    "unit": "BTC"
  }
}
```

The reserve payload contains no Chainlink address, chain ID, provider name, or
RPC detail. Those values remain in source identity and versioned provenance.
Frozen Dataset manifests pin the payload schema ID and contract hash, so the
same record replays without the provider adapter or endpoint.

## Hard-Cutover Rule

The migration may use temporary old/new comparison code only while writers are
stopped. The final repository contains no legacy readers, dual writes, fallback
tables/views, provider-specific research subsystem, or compatibility flags.
Historical schema codecs remain because frozen evidence references versioned
meaning; old physical storage does not.

See [ADR 0063](../decisions/0063-use-schema-registered-canonical-facts.md),
[Chainlink Structured Facts](CHAINLINK_STRUCTURED_FACTS.md),
[Canonical Fact Migration Discovery](../../engineering/canonical-fact-migration-discovery.md),
and [Canonical Fact Migration Backup](../../engineering/canonical-fact-migration-backup.md).
