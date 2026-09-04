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
  - src/market_data/canonical_storage.py
  - src/market_data/fact_archive.py
  - src/market_data/canonical_adapters.py
  - src/market_data/fact_registry.py
  - src/market_data/store.py
  - src/data_providers
  - portal/backend/db/market_data_models.py
  - portal/backend/db/market_storage_models.py
  - portal/backend/db/fact_storage_schema.py
  - portal/backend/db/session.py
  - portal/backend/service/market
  - portal/backend/service/research
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/candles.py
  - portal/backend/service/storage/repos/fact_storage.py
  - portal/backend/service/storage/repos/market_lifecycle.py
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

Two lineage lookups are explicit hot paths rather than generic JSON
inspection. Exact material witnesses use the `(series_id, material_hash)`
index. Derived trade-flow source validation uses `(series_id, source_id)` and
compares the indexed minimum and maximum source per upstream trade series; an
empty source set or unequal endpoints still fails loud. This preserves the
single-source contract without rescanning every historical trade for every
new aggregate bucket.

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

When a Check needs causal structured history, the frozen series identity covers
every canonical revision below the watermark, not only the latest active row
per observation. Corrections and invalidations participate in the frozen row
count, canonical material/provenance hashes, source counts, and transitive raw
archive references. The hashed source summary records
`record_selection=all_canonical_revisions.v1`. A dedicated Dataset-bound reader
loads that exact revision history; ordinary latest-state readers retain their
existing projection. Datasets created before this marker are re-frozen before
they can supply causal structured history. Book-derived archive lookup binds
the complete `(definition_id, session_id, connection_epoch, receive_ordinal)`
position; a reused ordinal after reconnect cannot be satisfied by an object
from an earlier connection epoch.

## Canonical Storage Placement

The storage-tier layout keeps the immutable revision envelope and all global
identity constraints in `market.fact_versions`. The three bulky JSON documents
(`payload`, `provenance`, `quality`) live in `market.fact_hot_payloads`, partitioned
by a database-owned UTC `storage_day`. Placement is not a market clock and never
participates in Fact identity, revision, known-at selection, or frozen hashes.
Fresh ingestion uses the database's current day; the offline cutover places old
rows by their unchanged acceptance day. Backfilled observations are not
immediately expired because their observation time happens to be old.

One transaction writes both envelope and payload. An always-on insert guard
checks copied identity fields and holds a SHARE lock on the open partition's
lifecycle row through commit. A deferred always-on constraint trigger rejects
an envelope without its payload. Sealing therefore waits for in-flight writes,
and later writes cannot enter a sealed partition. Immutable triggers protect
both relations from row updates/deletes. Provisioning creates only new empty
daily tables; it never changes existing columns or performs a runtime backfill.

`market.fact_rows` is the guarded logical projection used by remaining direct
hot SQL readers, not a second Fact store or legacy fallback. A demanded payload
absent from hot storage raises `canonical_fact_cold_read_required` rather than
dropping the Fact or returning null. Core canonical readers select envelopes
with nullable hot documents and hydrate absent documents through the immutable
cold-page catalog. The catalog's indexed fields must match its hashed descriptor;
exactly one verified/reclaimed page must cover each requested revision. Every
page is fully checked before use and every selected envelope/source field must
equal its archived copy. Missing, corrupt, overlapping, or inconsistent evidence
fails loud. Hot-only reads do not open or create the archive root. Indexed
metadata/dedupe/watermark queries stay on the permanent envelope.

Mixed pages can include spec-bound normalized features that were not selected by
the caller. Before decoding, the reader reloads every such page schema from the
immutable normalization spec and checks its identity/hash, repository-built
contract, and stored schema contract. Process-local registration is not sufficient
proof. Lookups use at most 1,000 specs per query and are read-only; a missing or
changed definition fails even if it was previously cached in that process.

Candle pages and overlapping chart windows select immutable IDs in SQL before
hydrating the selected payloads. Identity lookups deduplicate overlaps and use
at most 1,000 IDs per query. Candle availability summaries stream batches of
1,000 selected IDs and verify payload availability rather than treating an
unreadable cold object as healthy inventory. This bounds the working batch, not
the total bytes read for a long summary window. Cold summaries may cost more I/O
than hot-only SQL aggregation; measure that on the intended window before
increasing scan frequency.

Hydration never selects revisions. The SQL selector owns time, source, watermark,
revision, and invalidation constraints. Candle interval-close filtering is
applied to hydrated documents before choosing the latest eligible revision, so
a late correction cannot hide an earlier causal candle. Canonical BBO/depth
freeze lineage reuses the already decoded revisions and resolves source
positions in batches of at most 10,000; it does not reread hot-only provenance.
Object/query batching does not bound a complete in-memory frozen history: large
campaigns still need bounded dataset windows. Remaining direct SQL consumers
must be converted before destructive retention is enabled.

### Snapshot And Binding Failure Modes

Freeze acquires the shared lifecycle fence under autocommit, then establishes a
repeatable snapshot on the same connection. The fence is held through manifest
and pin commit and released on success or failure. Taking a blocking advisory
lock inside REPEATABLE READ is unsafe: PostgreSQL can establish the snapshot
before the wait, leaving it older than completed archive/reclaim work. Failed
or interrupted lock ownership cannot return a locked connection to the pool.
The concurrency regression also keeps the exclusive test connection checked out
across commits: committing an ordinary Session can return its connection to the
pool while a session advisory lock remains held, allowing a reader to borrow
that same connection and bypass the intended test wait.

Canonical numeric v2 schemas also use their schema-level history selection,
not the legacy family selector. Previously, the funding/OI family path could
select only latest state while labeling its v2 manifest
`all_canonical_revisions.v1`. New freezes bind every revision, including
invalidations. An older incorrectly bound numeric artifact is not repaired by
rewriting source Facts; normal frozen hash/count admission must reject it and
the dataset must be re-frozen. Valid existing bindings and their source clocks
are unchanged by storage movement.

`market.fact_retention_partitions` owns mutable open/sealed/verified/reclaimed
progress. The immutable `fact_archive_manifests`, `fact_archive_series`, and
`fact_archive_dependencies` relations provide ordered page descriptors, series
bounds, and raw/checkpoint holds for the executor. Their presence alone is not
proof of completeness or permission to delete. Payload partitions can eventually
be removed to return their table/index/TOAST allocation to PostgreSQL's
filesystem; ordinary row deletion does not provide that guarantee. The permanent
revision index and raw-mapping catalog continue to grow and must remain visible
in pressure/budget reporting. This design does not promise a hard cap on all
PostgreSQL bytes.

### Explicit Storage Cutover

`scripts/db/manual_migration_fact_storage_tiers_v1.py` is an offline operator
tool using only `PG_DSN`. Without flags it inspects read-only. Execution requires
both `--execute` and `--writers-stopped`; it never stops services itself. Apply
the prior canonical and operational-rollup migrations first. Capacity planning
must allow both copies plus WAL; the tool does not remove the old copy.

Preparation locks the old relation, refuses incoming foreign keys or dependent
views, moves it to `qt_fact_storage_cutover_v1.fact_versions`, blocks further
inserts, and creates clean current table definitions. A `copying` certificate
keeps runtime startup closed. Each bounded transaction copies an ordered page,
compares every canonical field against the immutable source, and commits its
resume cursor with the copied rows. Repeating the command resumes that cursor;
a failed page rolls back. Exact source/target/payload counts must match before
the certificate becomes `ready`. Readiness also checks storage enforcement.

The source remains fenced and intact for a separately reviewed rollback/cleanup.
Do not restart old code against the new layout. Before allowing new writes,
rollback can restore the retained source after removing only the new empty
catalog/copy and its projection; after new writes, restoring the old source alone
would lose those revisions and is prohibited. Never drop the retained source
until the complete rollout gate, including frozen-read equality, has passed.
The cutover and its migration tests are foundations: destructive retention stays
disabled until cold readers, archive admission, and the executor are validated.

## Canonical Archive Codec

The bounded `market.canonical_fact_archive.v1` codec preserves complete
canonical rows in Parquet/ZSTD. Its selection is always
`all_canonical_revisions.v1`; encoding does not select latest state, discard
invalidations, reconstruct facts, or change their acceptance/known-at clocks.
Envelope fields are typed columns, timestamps retain UTC microseconds, and
schema-owned payload/provenance/quality documents retain canonical JSON and
ordered child entries. Source identity, ingestion run, series dimensions,
version IDs, commit order, and existing hashes survive the round trip. The
PostgreSQL and archive paths share `canonical_storage` decoding and validation;
there is no archive-specific interpretation of a fact family.

Each object descriptor binds the exact byte checksum, ordered full-row content
fingerprint, row and logical-byte counts, first/last commit-and-ID cursor, and
per-series observation/known-at/acceptance bounds, source IDs, and payload
contracts. Its persisted JSON has a separate manifest hash. Reload rejects
changed descriptors and unsupported format/selection contracts. Files are
checked against their descriptor before any decoded rows are returned, with
schema, ZSTD, Parquet page-checksum, canonical envelope, content-fingerprint,
row-count, and index-bound verification. Encoding and publication both read
back their completed bytes; object-store acknowledgement alone is insufficient.

Default pages are limited to 10,000 rows, 64 MiB of logical content, and 128 MiB
of file/declared uncompressed Parquet data, with 512-row groups. Limits are
explicit codec inputs, not truncation policies: an oversized, duplicate,
unordered, malformed, or empty page fails and removes only its own staging
file. These limits bound one object, not a complete retention run.

This codec is a retention building block, **not retention admission**. It
preserves the lineage supplied in canonical provenance; it does not prove that
every referenced raw mapping/object exists. Manifest registration, raw-lineage
dependency/pin checks, hot/cold reader integration, resumable execution, and
actual hot-space reclamation remain separate lifecycle responsibilities.
Destructive canonical retention stays disabled until those boundaries and the
operator-run schema cutover are implemented and validated together.

`verify_canonical_fact_archive_rows` supplies the shared exact source-page
comparison for admission: it checks all columns, ordering, counts, bounds, and
content fingerprint against the independently verified object descriptor.
The lifecycle repository must choose that source page authoritatively; a
caller-supplied subset is not proof that a partition is completely archived.
Read-only object-store handles never create roots, publish, or delete objects.

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
- Level 2 Checks initially consume only `market.bbo.v1` and
  `market.depth_band.v1` numeric `query_fields`, at exact bucket boundaries.
  `market.l2_book.v1` remains operational, archive-backed reconstruction input
  and is never admitted directly to a research Dataset.
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
