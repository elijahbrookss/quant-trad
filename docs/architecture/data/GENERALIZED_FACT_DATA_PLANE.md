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
  - src/market_data/archive.py
  - src/market_data/archive_verification.py
  - src/market_data/book_archive.py
  - src/market_data/order_book.py
  - src/market_data/canonical_adapters.py
  - src/market_data/fact_registry.py
  - src/market_data/store.py
  - src/data_providers
  - portal/backend/db/market_data_models.py
  - portal/backend/db/market_storage_models.py
  - portal/backend/db/fact_storage_schema.py
  - portal/backend/db/session.py
  - portal/backend/service/market
  - portal/backend/service/market/canonical_retention.py
  - portal/backend/service/market/backtest_dataset_service.py
  - portal/backend/service/research
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/candles.py
  - portal/backend/service/storage/repos/collector_operations.py
  - portal/backend/service/storage/repos/normalization.py
  - portal/backend/service/storage/repos/fact_storage.py
  - portal/backend/service/storage/repos/fact_book_prefix.py
  - portal/backend/service/storage/repos/fact_book_admission.py
  - portal/backend/service/storage/repos/fact_derived_admission.py
  - portal/backend/service/storage/repos/fact_derivative_admission.py
  - portal/backend/service/storage/repos/fact_flow_admission.py
  - portal/backend/service/storage/repos/fact_flow_feature_admission.py
  - portal/backend/service/storage/repos/fact_response_admission.py
  - portal/backend/service/storage/repos/fact_dependencies.py
  - portal/backend/service/storage/repos/fact_archival.py
  - portal/backend/service/storage/repos/fact_lineage.py
  - portal/backend/service/storage/repos/fact_references.py
  - portal/backend/service/storage/repos/fact_reclamation.py
  - portal/backend/service/storage/repos/fact_retention.py
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

Collector recent-Fact inspection applies its existing latest/active selection,
ordering, and maximum 500-row limit to permanent headers before hydrating the
same public payload/provenance/quality fields. Normalization source admission
and latest-output conflict checks, book-state source checks, recovery rollup
folds, replay reconciliation, and trade-flow status use the shared reader too.
They no longer demand hot-only payloads from the guarded SQL view.

`stream_rows_by_ids` keeps the caller's SQL identity order and hydrates batches
of 128 IDs by default (configurable within 1..1,000). Its context closes the
server cursor on completion, early match, and failure. A batch bounds selected
IDs, not total scan work or a strict byte-memory ceiling. Multi-query spec
inspection, archive status, and replay reconciliation acquire the lifecycle
fence before establishing their repeatable snapshot. Replay inspects retained
canonical events; it does not introduce another book reconstruction engine.

Material-alias indexes are candidate locators, not evidence: the reader verifies
the selected archived row and its actual source witness before admission. The
existing generic object-valued legacy witness predicate still applies. If no
indexed candidate exists, an explicitly logged cold scan checks older,
unindexed provenance keys. Retired normalization-spec quarantine likewise
checks exact hot/cold provenance references before declaring a legacy spec
unreferenced; an external-event name alone is not such a reference. Missing or
corrupt pages fail these operations, rather than silently dropping evidence.

Performance follow-up: these cold scans bound payload batches but not elapsed
time, total I/O, or the replay result's required ID list. A page can be verified
again in later batches. Broad trade-flow status windows, missing-witness probes,
and legacy-spec reference audits need representative measurements and explicit
caller windows before frequent unattended use. A complete alias catalog or
verified page reuse is a future optimization, not permission to omit checks.

`test_cold_consumers_db.py` exercises collector inspection after physical
reclamation and other consumers against verified cold-reader fixtures. Those
fixtures prove read compatibility, not admission of L2/normalized dependency
graphs for destructive retention. The executor's supported-family gate remains
unchanged until those separate dependency proofs are complete.

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

### Canonical Retention Planning

`market_data_lifecycle.canonical_retention` is the typed policy for generalized
hot payloads. The default hot window is 30 complete UTC placement days, with
exact `hot_days_by_fact_type` overrides. A daily partition waits for the longest
window of **every** family present. A late correction starts its own hot lifetime
from `storage_day`, not its older observation or known-at clock. A day must be
strictly older than database-today minus its window; the current and pre-created
next day are never eligible. These are reviewable defaults, not production
retention activation.

The existing `qt data market-structure lifecycle-plan` and dry `lifecycle-run`
responses include `canonical_retention`. Its next-phase candidates distinguish
seal, page publication, page verification, whole-partition verification, and
physical reclamation. Persisted page/receipt progress selects the next phase;
file presence alone does not. Unproven dependency families block the entire day.
Older verifier receipts select `restart_verification`, not reclamation: the
executor withdraws the old admission and resumes current verification. All eligibility is metadata-only and
`requires_execution_recheck`: it does not replace fresh bytes, complete lineage,
pin checks, mount admission, or the final physical-relation gate.

Planning reads a PostgreSQL read-only transaction and filesystem metadata. It
does not create directories, publish/read archive objects, seal progress, acquire
the lifecycle mutation fence, or delete data. Database UTC time owns the cutoff.
Missing/wrong/read-only mounts appear as explicit blockers. Canonical metadata
planning runs outside the raw lifecycle's exclusive fence, including on an
explicit raw execution run; raw planning/execution retains its existing fence.

The complete hot-partition inventory defaults to a 4,096-partition limit, while
family/progress inspection defaults to 16 candidate days per response. Inventory
overflow fails rather than reporting partial storage totals. Continue candidate
inspection with `--canonical-after-storage-day` using `next_after_storage_day`;
`candidate_scan_complete` describes that scan, not an evidence verification.
Physical hot-byte totals still cover every hot partition on every page. SQL
statements default to 5 seconds, reduced by the remaining 15-second checked
inventory budget. These bound query work, not connection checkout, filesystem
system calls, or a hard wall-clock completion guarantee. Distinct-family scans
still need production-scale measurements; timeouts fail instead of guessing.

Optional `hot_payload_budget_bytes` and `archive_filesystem_budget_bytes` default
to null until reviewed against actual capacity. The former measures attached
payload table/index/TOAST allocation only. The latter measures **all used bytes
on the archive filesystem**, not just QT objects. `archive_min_free_bytes`
defaults to 1 GiB. The plan separately reports whole-database bytes, permanent
canonical-header bytes, and raw-mapping bytes: reclaiming payloads is not a cap
on PostgreSQL, WAL, or the NVMe filesystem. The independently provisioned
filesystem alerts remain necessary.

Hot pressure prioritizes already-verified, window-eligible reclamation within
the inspected candidate page. HDD budget/reserve pressure blocks new publication
but need not prevent reclamation of a fully verified copy already there. A bad
mount blocks both. If safe work cannot restore headroom, the output requests
operator review/capacity; pressure never shortens windows, skips evidence, deletes
unarchived data, changes collection policy, or treats pin release as permission
to break cold lineage. Candidate byte estimates are explicitly metadata-only.

The executor is wired through the same lifecycle API, CLI and supervisor. Plans
report `execution_available: true` separately from `execution_enabled`. The
canonical `execution_enabled` flag defaults false, independently of the existing
outer lifecycle flag: existing raw-retention enablement must not activate this
new path. Complete transitive dependency admission and the production rollout
checks remain required before activation.

### Bounded Canonical Execution

`CanonicalFactRetentionExecutor` invokes the existing archive and reclamation
owners **after** the raw lifecycle exclusive-lock context exits. It does not
upgrade that lock or invent a second archive/deletion implementation. Manual
mutation requires `lifecycle-run --execute`, the outer lifecycle execution flag,
and the canonical execution flag. The supervisor uses those same policy gates.
With either mutation gate off, canonical work remains read-only or disabled.

Every iteration reads fresh candidate/progress metadata and performs at most
one next-phase action. Defaults are four iterations and a 60-second cooperative
run budget. Empty/blocked candidate-page scans consume an iteration too. A
fresh per-partition guard, under the owner's row lock, repeats database-time
cutoffs, **all** family windows/admission, and writable filesystem/headroom
checks; a previously returned plan cannot authorize a write. The final physical
reclaimer independently repeats its exact-relation, pin and current-byte gates.

The default page bounds are 10,000 rows and 64 MiB logical bytes, with a
128 MiB encoded-file ceiling. Publication requires reserve for both the encoder
file and atomic object-store temporary (256 MiB at defaults), in addition to
the configured free-space reserve. The plan reports insufficient publication
headroom before attempting the operation. Byte budgets are admission targets,
not filesystem quotas: concurrent collectors/other programs can consume space.
An out-of-space operation must fail without acknowledging a partial object.

Each deep-dependency/final-verification operation defaults to 4 GiB of hashed
objects, 10,000 distinct objects, and 1,000 canonical pages; existing raw decoder
row/logical-byte/group bounds still apply. These are per-operation limits, not
a claim that decoding, encoding, and repeated verification use a single byte
counter. The iteration limit bounds their repetition. All execution fields have
typed `QT_MARKET_DATA_LIFECYCLE_CANONICAL_*` environment bindings. Lowering limits
below existing page sizes fails explicitly; it does not partially verify pages.

Archive SQL defaults to a 5-second statement timeout, capped by the remaining
run budget when starting a step. Reclamation retains its stricter 1-second SQL
and 10-second exclusive-handoff bounds. Deadline/stop checks run between phases,
decoded raw records, canonical envelopes, catalog pages and checksum blocks.
The supervisor supplies its stop request, allowing an in-flight transaction to
roll back before acknowledgement/drop commit. This is cooperative cancellation:
connection checkout, kernel I/O, codec calls and commit/fsync are not hard
wall-clock interrupt guarantees. An interrupted unacknowledged publication is
reused safely on retry; acknowledged pages/receipts survive worker restarts.

The worker retains only an in-process **scan hint**, never an executable plan.
It revisits a partially processed day, advances beyond a failed/busy day or
fully blocked candidate page so later days are not starved, and wraps at scan
completion. A process restart may repeat the bounded inventory scan, but resumes
archive work from committed partition/page/verification records. Manual callers
can supply `lifecycle-run --canonical-after-storage-day YYYY-MM-DD`; returned
`next_after_storage_day` and supervisor snapshots expose this hint. Losing it
does not lose storage progress or weaken admission.

Run outcomes distinguish step/time bounds, stop requests, deferred conflicts,
blocked scans and failures. Genuine step failures log their storage day/action,
return an error outcome and make the lifecycle result degraded; `lifecycle-run`
prints that result and exits nonzero. Progress remains
in the existing storage tables; the raw `lifecycle-events` ledger does not claim
to contain canonical phase events. No new scheduling/persistence authority is
introduced, and no ingestion definition or enablement is changed.

### Canonical Archive Staging

`PostgresCanonicalFactArchiveRepository` seals only placement days older than
the database's current UTC day. Its lifecycle shared fence protects dependency
verification and hold creation against raw-object expiration; a per-day
transaction advisory lock excludes competing archive workers. Sealing waits
for payload writers, checks that all envelopes have attached hot payloads, and
records the source count and physical allocation.

Each staging call reads the next ordered source page after the last acknowledged
cursor. Both row count and a conservative SQL-side JSON byte allowance bound
transfer to Python; a first row outside the byte allowance fails explicitly and
never advances the cursor. Exact canonical hashes are checked before dependency
resolution. Object count and cumulative bytes bound raw dependency verification.
Publication, complete object read-back, and exact source-page verification
precede an atomic catalog commit. A crash after publication but before commit
leaves no acknowledged progress; retry reuses identical immutable bytes.

`fact_archive_material_aliases` indexes legacy typed material hashes retained in
canonical provenance. Aliases are derived from verified page rows, not invented
identities. Typed lineage combines hot provenance lookup with the cold alias
index and verifies every claimed hash against the hydrated payload. Missing or
conflicting witnesses fail; an index entry alone cannot prove lineage. Derived
book traversal includes the connection epoch: a repeated receive ordinal in a
later connection cannot be satisfied by an earlier connection's archive.

Raw/checkpoint retention status, dry-run planning, and execution include
`canonical_dependency_count`. Such holds cannot be released by user-pin release
or disappear when no frozen dataset currently references the data. Compacted
raw source objects with these holds also remain protected; this can increase
HDD retention compared with ordinary age-only expiration.

Staging deliberately leaves the partition `sealed`; exhausting its source is
not permission to drop it. This owner does not perform reclamation. A normalized page currently fails with
`canonical_archive_dependency_proof_required`; it is never admitted with an
empty dependency set. These gates must be completed before retention activation.

### Exact Raw-Revision Evidence

#### Hot Backlog And Reference Lifetime

Ordinary raw/checkpoint expiration reports `canonical_backlog_present` as well
as permanent `canonical_dependency_count`. An attached hot payload can still
need its raw source before any archive page or frozen dataset exists. Collector
session provenance, L2/BBO/depth source positions, and trade-flow coverage bind
session-scoped backlog protection; exact trade/L2 raw-ID mappings also cover
generic canonical imports without collector-session metadata. Book/coverage
scope deliberately protects earlier frames, not only the last source position.
These checks use the existing JSON-containment GIN indexes and do not decode
cold files. Planning lists `canonical_hot_backlog`; final expiry repeats the
check. User-pin release cannot remove either class of canonical protection.

The final check alone is insufficient. `archive_expiration_lock` holds an
`UPDATE` row lock on the exact immutable raw/checkpoint manifest through the
final status check, verified unlink, and recorded completion. It uses `NOWAIT`:
an in-flight reference writer defers expiry with a visible skipped/retry outcome.
The existing lifecycle fence still excludes competing expiry/compaction/pin
operations. No manifest is updated by this row-lock protocol.

The canonical writer binds declared direct raw references before publishing a
genuine new revision. `fact_references` resolves raw IDs, exact book positions,
and exact coverage revisions to acknowledged mappings, locks manifest rows in
deterministic order with `KEY SHARE`, then checks expiration in a **separate**
READ COMMITTED statement. If expiry completed while the writer waited, a new
reference cannot be published to deleted evidence. Each raw record needs a live
placement; a surviving compacted copy can satisfy it, but an unrelated live
object cannot. This catalog/lifetime check does not replace deep byte, mapping,
causal, or complete-chain verification during archive admission.

For a book reference, an existing immutable hot L2/BBO/depth row in the same
definition/session already protects the session's raw objects. The writer's
backlog lookup retains PostgreSQL relation locks until commit, preventing that
holder's partition from being dropped before its successor is published. This
keeps ordinary collection from rescanning an ever-growing connection prefix on
every update. A committed immutable book-prefix chunk is also a session lifetime
anchor after all hot holders leave. Its existence protects that definition/session's
raw objects and checkpoints, including trailing control frames and later
checkpoint publications not named by an earlier page's exact edges. Checkpoint
scope is resolved through its immutable `source_manifest_ids`; mutable stream
configuration is not ownership evidence. An anchor is never treated as a byte,
mapping-completeness, or reconstruction proof for the new row.

Without either a hot holder or that durable anchor, late book admission checks every ordinal
from one through the new position, locks all candidate placements, and rejects
a missing, ambiguous, expired or unfinished-expiration prefix. Repeated roots
share their largest definition/session/epoch scope. The existing mapping budget
still applies; a large unprotected import fails rather than scanning unboundedly.

Checkpoint backlog protection uses the checkpoint's recorded **session**, not a
stream-definition field that exists only on raw manifests. It conservatively
holds a checkpoint while any hot book/coverage reference in that session still
needs preservation. This may retain extra evidence when sessions overlap, but
does not consult mutable stream configuration or weaken raw definition/session
scoping. RCA: the earlier shared target query selected `definition_id` from
checkpoint manifests, where no such column exists. A real-database regression
reproduced `UndefinedColumn` through retention status, then verifies both the
unreferenced checkpoint and its protected state after book publication.

Cold session protection is a non-releasable canonical hold. When no exact
object edge exists, `canonical_dependency_count` reports one logical session
hold; it is not a count of newly inserted per-object edges. Status and expiration
candidates use the same predicate. This deliberately retains the complete book
session, not only the last canonical prefix. RCA: handing off from a hot
session hold to only exact raw edges left checkpoints and trailing control
frames unprotected. It also made a cold-only writer rescan its whole prefix.
The immutable session anchor closes both lifetime and writer-scaling gaps.

The disposable cold-book regression exercises real source parsing, reconstruction,
checkpoint publication, BBO/depth freeze, physical payload removal, cold reads,
refreeze, and full/checkpoint-delta replay through the production admission gate.
Missing/corrupt checkpoint objects and corrupt cold source pages block removal.
It covers shared and separate placement days, including a source moving to cold
during the final handoff, which must force a retry. Raw L2
events remain replay inputs rather than directly dataset-eligible observations;
the frozen research series in this test are BBO and depth. A one-mapping writer
budget proves that a cold-only session no longer rescans three historical frames.

The full service diagnostic also exposed a replay-scope defect before any hot
data was removed: execution-trade initialization had been placed inside pair
feature materialization instead of book replay. Replay raised `NameError` for
`execution_trade_records`, while pair materialization raised it for
`replay_states` after ingestion. Initialization now belongs to replay, retaining
the existing source time window and known-at cutoff. Separate regressions cover
replay with/without a configured trade series and pair materialization; no
alternate reconstruction path or new fingerprint version is introduced.

An execution `planned` event also makes that placement unavailable for new
references, even without `completed`: a process can stop after unlink but before
recording completion. A failed/skipped event cannot prove the bytes survived.
Admission reports `canonical_raw_reference_expiration_pending` and requires
resuming the recorded expiration; an independently live compacted copy can still
satisfy the reference. A dry-run plan writes no execution intent and does not
cause this exclusion.

The implemented stream lane already requires archive acknowledgement before
canonical publication. Generic writes declaring raw references now share that
same admission; a missing declared mapping or a set of wholly expired copies
fails explicitly. Source fields, identities, ingestion schedules and collection
enablement are not rewritten. A caller-supplied repeatable-read snapshot is
rejected for reference writes because it could hide an expiry completion after
the snapshot. Canonical no-ops remain envelope-only and do not create new pins.
Pending revisions are identified under the existing series fence with one
batched header query, and only their reference set is locked. Reference mapping
work is limited to 50,000 rows per canonical batch; overflow requests a smaller
batch rather than partially admitting evidence. Repeated observation keys are
evaluated in batch order, so an A-to-B-to-A correction admits both new revisions
instead of mistaking the final A for an existing-state no-op.

RCA: the prior expiry gate knew about user/frozen pins and already-created cold
holds, but not hot source facts awaiting cold archival. It could therefore treat
their raw objects as unpinned. A second gap was the interval between checking
acknowledged mappings and committing a new canonical reference. Backlog checks
close the first gap; mutually conflicting, object-specific manifest row locks
plus post-lock expiry admission close the second. The real-database regression
checks both transaction orderings and preservation across the hot-to-cold hold
handoff, plus an injected completion-write failure after actual unlink and a
successful resume. It also keeps an unrelated raw reference writable while
another object is locked. This is not a claim that every legacy normalized/composite dependency
dependency is now admitted for reclamation; those gates remain explicit.

Performance follow-up: hot and durable session holds are conservative and can retain
more raw/checkpoint bytes than a minimal per-root proof, including later objects
in a long-running session. A missing exact edge adds one indexed session-anchor
lookup during candidate/status checks; exact held objects skip that lookup. Candidate checks still
need large-history query-plan measurements. Only writers referencing an object
being expired wait on its row lock; unrelated objects do not acquire a global
ingestion fence. Very large historical batches and same-series writer queues
need bounded-latency measurements before unattended activation.

#### Deep Raw Admission

Canonical archive admission resolves trade and L2 raw IDs from each archived
revision's own provenance. BBO/depth use their exact definition/session/epoch/
receive-ordinal position. `fact_lineage` joins **record mappings**, not a
manifest's min/max ordinal range: a range can contain a hole and cannot prove
that a particular received frame is present.

For each witness, admission chooses one current acknowledged placement; an
original with a completed expiry event is not eligible. Initial selection is
deterministic by object size and manifest ID. A resumed page check is restricted
to its already acknowledged dependency IDs, so later compaction cannot retarget
an immutable page. A missing/corrupt selected copy fails without silently
switching to another copy. Multiple different raw IDs at the same claimed book
position are ambiguous and rejected.

Every selected object receives a fresh byte checksum and bounded streaming
decode. The complete object's row count, order, source session/epoch, and ordinal
bounds must agree with its manifest. Each requested physical row must match its
stored raw ID, original segment ID, frame checksum, session, epoch, and ordinal.
The decoder independently recomputes raw identity from frame bytes. Each root
revision must also agree on product and source position. Trade/L2 roots additionally
bind the provider, venue, and receipt time. Derived book features retain QT as
their author; their declared input position binds the exchange frame without
relabeling the derived source. Mapping row offsets are global within a v1 object;
the writer's `object_row_group=0` field is a placeholder, not random-access proof.

Canonical staging requests **complete book raw prefixes**: for each
definition/session/epoch it takes the greatest L2/BBO/depth source ordinal on
the page and proves every physical position from one through it. Coinbase
receive ordinals restart at one per connection. Each position must have one
unambiguous raw identity in an acknowledged placement, with exact mapping,
product and requested-Level-2 scope. This includes subscriptions, heartbeats and
other frames that produced no canonical mutation. The chosen object IDs become
permanent cold dependency holds; rechecking cannot silently switch placements.
`fact_book_prefix_chunks` records immutable, hash-bound verification progress;
`fact_book_prefix_dependencies` keeps each chunk's chosen raw objects alive even
before a canonical page is published. A staging call verifies at most one next
dense interval (by default 12,500 positions, leaving room within the 50,000-row
mapping budget for alternate placements), returns `book_prefix_verified`, and
commits that interval and its holds together. A per-scope transaction lock
serializes extensions from different days. Restarted workers resume at the
last committed ordinal; no in-memory cursor or partially written proof counts.
The real-database test commits the opening interval, removes an intervening
heartbeat object, and verifies that the next step fails without losing the
opening proof or any hot payloads. An interruption after flushing a new chunk
rolls it and its holds back together; restoring the object permits the same
ordinal to resume.

Before publishing or verifying a canonical page, admission validates the whole
contiguous chunk chain, its scope, descriptor hashes, and permanent dependency
catalogs, then checks current object checksums. This reuses deep raw decoding,
not old filesystem stamps. Each exact canonical root binds to objects from its
own certified interval: a later placement cannot substitute another raw frame
at the same position. A previously certified larger interval may conservatively
hold additional frames for a smaller root; it grants no access to future Facts
and changes no known-at selection. Receipts and holds are append-only, and user
pin release cannot remove them. New placements do not rewrite admitted lineage.

The object's ordered `content_fingerprint` is independently recomputed from
**all** stored raw IDs and frame hashes, not just the page's requested witnesses.
Writer and verifier share the unchanged v1 canonical-JSON serialization. Two
bounded identity-column passes preserve that format without retaining all IDs
or decoding large raw frames twice. The same physical-schema, row-group and
file/logical-byte bounds apply, with cooperative cancellation during hashing.
The surrounding checksum and file-stability gates still bind those columns to
the fully decoded object; a fingerprint alone is not raw-frame verification.

Default per-call bounds are 50,000 mapping candidates, 1,000,000 decoded raw
records, and 2 GiB logical data. Individual files are limited to 1 GiB and
declared row groups to 256 MiB; decoding uses 128-row batches, with additional
decoded-byte checks. The archive owner's dependency object/byte limits also
apply. These explicit limits can be supplied to the repository; no bound is
automatically enlarged or treated as permission to skip evidence. The existing
list-returning raw reader remains available to existing callers, but retention
uses the shared streaming decoder and exact writer-owned Parquet schema.

Performance follow-up: separate intervals and exact roots can still require
repeated decoding of the same large compacted raw object. Per-object proof reuse
could reduce that work, but must not turn file timestamps into content proof
or remove final fresh checksums. Chunk-chain inspection is capped at 5,000
receipts per call; page dependency object/byte and final verification budgets
still apply. Shared interval progress removes full-prefix re-decoding on every
retry, not all size limits. Exact-position SQL also needs representative
plan/capacity measurements before activation; small disposable fixtures do not
prove production-scale throughput.

RCA: the initial staging implementation reused a latest-by-material-hash lookup.
Trade material hashes intentionally exclude delivery details. Two immutable
revisions could therefore have identical trade values but different raw IDs;
the older page incorrectly held only the newer delivery's archive. The real
database regression reproduces this mismatch. Exact-root mapping/physical-row
admission fixes it without changing Fact values, clocks, or ingestion policy.
Version v2 closed that exact-revision gap. Version
`market.canonical_archive_verification.v3` additionally recomputes the raw
content fingerprint: previously a valid object SHA could accompany an incorrect
manifest fingerprint without rejection. A regression reproduces that mismatch,
including fingerprints that omit raw rows not requested by the canonical page.
Earlier receipts cannot satisfy the stronger gate. Old incomplete draft catalogs are
not silently rewritten or blessed and require explicit review before reuse.
Version `market.canonical_archive_verification.v4` additionally requires the
complete raw book-prefix proof. Earlier last-frame receipts cannot satisfy it.
Version `market.canonical_archive_verification.v5` binds the shared immutable
prefix-chain proof and each root's own interval. Its two new clean metadata
tables are included in the storage layout and explicit offline cutover; no raw
or canonical history is backfilled, changed, or presumed verified. Old page
receipts still need current-version verification before reclamation.
When an older complete page lacks shared prefix receipts, `verify_next_page`
builds one bounded interval per call using only that page's already-bound raw
objects, then resumes page verification. It does not stall waiting for the
publication phase or rewrite the old page's dependency catalog. Missing or
conflicting historical bindings still fail rather than being replaced silently.

The same bounded prefix engine now exposes a trade-channel proof under
`market.trade_prefix_verification.v1`. The verifier version is part of the
immutable scope and descriptor hash, so book and trade certificates cannot
satisfy each other's admission. The existing metadata tables and book descriptors
remain unchanged; no schema migration or rewriting of earlier certificates is
needed for this reuse. Trade coverage endpoints can bind exact raw IDs to their
own certified chunks, including control frames without canonical trade rows.
Uncovered or out-of-scope endpoints fail. This is supporting proof machinery;
trade-flow reclamation still requires the separate canonical-input and coverage
admission below and is not enabled by this prefix API alone.

Version `market.canonical_archive_verification.v9` wires this proof into flow
archive staging and verification. The exact immutable coverage revision must
match the bucket's source/instrument/product/channel and causal clock; its
metadata hash and exact opening/last/closing raw witnesses are checked. Shared
prefix receipts retain every raw position through the declared coverage and
archive/canonicalization watermarks, including control frames and deliveries
without their own canonical rows. Existing session holds also protect trailing
raw evidence. Missing or corrupt selected bytes fail; reverification cannot
silently substitute newly bound raw objects.

Flow v1 does not name canonical input IDs or a source trade series. Bounded
captures can use a live update whose provider trade identity was already
canonicalized from a snapshot or another coverage interval. `ingest_trades`
deliberately deduplicates that delivery; requiring a matching canonical delivery
revision would reject genuine retained evidence. Flow archive closure therefore
keeps **every** candidate canonical trade revision in the exact instrument,
source and bucket window below the root's commit and known-at clocks, plus the
complete raw coverage proof. Candidates include other coverage labels, snapshots,
corrections and invalidations. Each canonical candidate independently binds its
own exact raw delivery; it is not relabeled as belonging to the root's session.
The root's clock, not coverage registration time, bounds canonical inputs:
bounded capture registers coverage before accepting translated trades.

This is deliberately conservative evidence preservation, not an assertion that
every retained candidate was used or that historical market quality is correct.
No raw replay becomes a synthetic canonical revision, and no completeness flag
or known-at timestamp is repaired by storage movement. Explicit zero and partial
buckets retain their original evidence too. An uncovered historical bucket has
no raw-session witness and additionally requires reconciliation through the
existing aggregation owner; retention does not search arbitrary correction
subsets to make it match. The same batched edge and pre-hydration byte budgets
apply. Broad/repeated bucket windows can retain more source edges and raw bytes
than minimal exact-input provenance; pressure must expose that cost.

The disposable flow regression uses the actual parser and ingestion deduplication,
then stages partial/complete revisions and a zero bucket after the canonical
snapshot source has physically moved to cold storage. Corrupt cold source bytes
and a corrupt raw update with no separate canonical row both block publication.
Trade and flow Dataset freezes now bind `all_canonical_revisions.v1`, including
explicit invalidations and historical partial flow revisions. Canonical rows
own material/provenance identity and source counts; typed decoding is used only
to render the original trade/flow quality fields. Every root's exact raw
delivery, causal candidate window and immutable coverage revision participate
in archive admission. A historical `archive_complete=false` is not rewritten
or mistaken for today's physical archive availability: the original flag stays
visible, while the current raw evidence must independently pass verification.
Uncovered flow requires the existing owner reconciliation. Missing bytes or
unprovable lineage still fail, and old latest-only datasets must be re-frozen
before they can provide canonical revision history.

Freeze reuses valid prefix certificates and directly checks only a genuinely
unverified tail plus exact root/source witnesses. It never writes retention
progress or silently bypasses a corrupt certificate. Per requested series,
50,000 root/source-edge/mapping rows and 64 MiB canonical dependency JSON bound
admission; current-object checks allow 10,000 objects and 4 GiB, and the existing
raw decoder's row/file/logical-byte bounds apply independently. An oversized
unverified tail fails rather than partially freezing it. Existing bounded
retention prefix work can supply reusable proof for long sessions. These are
per-operation limits, not a claim that a whole multi-series Dataset is held in
constant memory or that repeat hashing has no cost.

Physical flow reclamation now passes the same canonical/current-byte gate as
its trade inputs. Disposable regressions compare frozen histories, original
partial quality, typed latest and known-at reads, and re-freeze identity across
actual removal of both source and flow payload partitions. They cover sources
cooling before or after flow verification, and explicit v8-to-v9 reverification
that adds required edges/prefix proof without modifying old pages or receipts.

RCA: the Dataset validator appended trade/flow/feature quality notes to the
already-complete frozen quality document a second time. This changed its hash
and rejected valid typed bindings; canonical trade history also exposed the
validator's assumption that every record had legacy typed attributes. Validation
now checks the exact pinned quality document once, alongside the independent
canonical/typed material and provenance hashes. Tampered quality still fails;
no source data, historical hashes or quality records are rewritten.

Version `market.canonical_archive_verification.v10` extends the same canonical
edges to trade-flow features. The declared source flow series and aggregate
material hash select **all** causally available matching aggregate revisions,
using hot provenance or cold aliases only as locators. Hydrated rows must prove
the hash and clocks; immutable series must agree on instrument and interval.
The feature's bucket and aggregate input fingerprint must match too. Each
aggregate retains its complete causal trade window and coverage/raw-prefix
proof through the existing flow admission owner. Later-known aggregate revisions
cannot enter an earlier feature's dependency set.

The existing `derive_trade_flow_feature` owner must reproduce the feature's
payload, legacy material hash and combined trade fingerprint from a historical
producer selection: latest active covered inputs, latest active material, or
bounded deliveries. Delivery-independent trade material permits a deduplicated
snapshot to witness the same update content without fabricating a revision.
Arbitrary correction subsets are not searched, and missing/nonmatching material
fails before reclamation. Every causal candidate remains retained even when
only one producer selection reconciles; the proof does not claim those are
individual original input IDs or repair original quality flags.

Feature freezing binds `all_canonical_revisions.v1`, including invalidations,
with the same read-only aggregate/trade/raw closure. Old latest-only datasets
require re-freezing. Root, source-match, transitive-edge and total derivation
input visits are separately bounded to 50,000 rows for each frozen series;
64 MiB bounds each canonical source hydration stage. Repeated feature revisions
cannot multiply an unbounded input scan: reduce the requested/archive page
window when the work limit is exceeded. The existing raw-object and prefix
limits still apply. Repeated owner decoding and repeated freeze closure lookup
are a bounded performance opportunity, not a second source of truth.

Disposable tests remove trade, aggregate and feature hot partitions, compare
frozen history/known-at/latest reads and re-freeze identity, and reject corrupt
cold aggregate bytes before feature DROP. v9 feature pages must be explicitly
reverified under v10 to gain canonical source edges; original page bytes and
older receipts remain unchanged. This family joins the default-disabled gate;
response and normalized-window proofs still gate their physical days.

Version `market.canonical_archive_verification.v11` strengthens response archive
publication with canonical source preservation. The declared flow-feature hash
resolves every causal matching revision and its aggregate/trade closure. The
three named book positions use the same bounded exact-position owner as
BBO/depth inputs, including state/validity witnesses and rechecked source clocks.
Response, flow and book series must agree on instrument; response and flow use
the one-second contract, and the named book positions share a stream scope.
The first and last directed trade IDs must exist at their exact recorded raw,
sequence, receive, event and trade positions. No synthetic source Facts are
constructed to reuse the book proof.

Response v1 records no trade series/source ID. Retain the full canonical trade
window for that instrument and bucket under the response's own clocks, including
all source candidates, corrections and invalidations. The response clock can be
later than its aggregate's input clock; preserving only the aggregate closure
would miss this evidence. Likewise retain the inclusive book observation-time
envelope of every causal named-state revision, not just three extrema. Exact
raw prefix proof independently retains the preceding connection history needed
to reconstruct those states. The common canonical-window reader now also owns
flow's narrower source-bound lookup. It uses bounded batches and pre-hydration
byte checks; repeated root-to-source edges count toward the limit, not just
distinct documents. Current source placement and raw/checkpoint bytes remain
subject to the existing final archive-verification handoff.

Response Dataset freezing also binds `all_canonical_revisions.v1`, including
invalidations, through this same source resolver. Its raw trade/flow and book
windows use read-only prefix owners: reuse verified history and decode only a
bounded absent tail, without publishing certificates or changing collection.
Older latest-only response datasets must be re-frozen for causal history.
A reused prefix certificate can extend beyond the requested Dataset position.
Every certificate dependency still undergoes current-byte verification, but
wholly later objects do not enlarge the Dataset's raw-reference identity. An
object spanning the boundary remains indivisible. This keeps a repeated freeze
stable when retention subsequently certifies a larger prefix; certificate and
hold corruption cannot be bypassed by trimming its returned reference set.

This is source preservation, **not yet response deletion admission**. Full
response-owner calculation/input-fingerprint reconciliation and the response
partition's own physical-retention proof remain required before opening that
family gate.
The disposable source-closure regression archives response revisions after
trade/book/flow inputs have physically moved to cold storage, retains an
intervening non-extremum book event and a later causal trade revision, excludes
future-known inputs, and rejects corruption of the intervening raw event.
All-revision frozen history, known-at reads, validation and repeated Dataset
identity remain unchanged after those source partitions are reclaimed, even
when a source certificate includes a wholly later raw object. The response
partition itself must still reject reclamation.

Version `market.canonical_archive_verification.v6` adds immutable canonical
source-revision edges and book metadata/checkpoint admission. BBO/depth evidence
identifies an L2 position and state rather than a delivery revision ID. Admission
therefore preserves every matching-position source revision within the derived
record's commit and known-at bounds, verifies the declared state/interval, and
binds exact IDs and row hashes in `fact_archive_canonical_dependencies`. SQL
selection is batched; row/mapping and logical-byte limits bound the closure before
hot JSON is fetched or cold pages are decoded. Immutable product definitions and
validity openings are checked by ID/scope, never from mutable stream settings.

Existing relevant checkpoints must match a causally eligible canonical L2 state
at their exact source position, in addition to exact raw mappings and restoration
through the single book-state owner. Those source revisions and object keys/hashes receive permanent
holds. A book without a saved checkpoint remains valid; checkpoints accelerate
replay and are not fabricated to permit retention. Reverification binds the
committed checkpoint set rather than replacing it with future publications.
Before the first v6 receipt, verification can append newly required source and
checkpoint edges atomically; it never rewrites existing edges, old receipts,
raw-object bindings, or canonical bytes. The whole raw prefix remains required.

This admitted L2/BBO/depth in addition to the six standalone source families.
Version `market.canonical_archive_verification.v7` additionally admits
`market.futures_spot_relationship` through exact canonical BBO inputs and their
complete L2/raw/checkpoint closure. Each declared material witness binds **all**
matching revisions at or before the root's commit and known-at clocks, not the
latest alias. Requests are batched at 128; total matched edges are bounded by
the canonical source row budget. Hot lookup uses containment supported by the
existing provenance GIN index; cold aliases are candidate locators only. Hydrated rows must
prove their actual material, series, family, observation and causal clocks.

The existing BBO decoder and basis derivation owner validate the declared pair,
mids, staleness, input fingerprint and retained typed material hash. Immutable
role-mapping metadata must exist with matching instruments, `spot_reference`
role and effective range. This preserves, rather than revises, the existing basis
known-at contract: mapping registration time does not retroactively change a
canonical fact's clocks. Basis publication can advance the source books' prefix
receipts even if those source facts remain hot on another day. Cold source
movement and corruption use the same final placement/current-byte gate.

Basis Dataset freeze also binds `all_canonical_revisions.v1`, including
corrections and invalidations, with canonical material/provenance identity and
raw lineage collected from every retained root. Ordinary typed/latest basis
readers remain unchanged. Previously frozen basis datasets had latest-only typed
identity; they are not relabeled in place and must be re-frozen to supply causal
revision-history research. The physical-retention regression checks frozen
history, typed latest reads, known-at reads and re-freeze identity before/after
DROP, including old source deliveries beside later hot revisions.

Old v6 receipts cannot authorize this stronger composite admission. Reverification
retains the original page and raw bindings, appends required canonical/checkpoint
edges only, and fails if the originally bound raw evidence cannot prove the full
closure. It does not silently substitute later material deliveries or archives.
Response and normalized-window closure remain
separate gates before complete retention activation. Individual objects and final
current-byte checks must fit their budgets even when a connection spans many
resumable intervals.

Structured reserve reports (`asset.reserve_state`) are also admitted as a
self-contained source family. Their exact response bundle and provider metadata
are retained inline in canonical provenance, alongside the report payload and
immutable source/acquisition metadata; there is no separate raw-stream Parquet
object to invent or require. Archival and reclamation share the explicit
self-contained-family classification. The reserve regression physically removes
hot payloads and checks unchanged provider response evidence, corrections,
invalidations, frozen binding validation and known-at reads. Corrupt page bytes
block removal just as they do for other source families.

Version `market.canonical_archive_verification.v8` admits derivative state through
its explicit OI/funding commit references. The previous OI commit is not a payload
field, but is bound by the retained input fingerprint. Admission bounds and loads
all eligible predecessor revisions at the producer's 60-second interval, then
uses the same fingerprint owner to select exactly one. An equal-valued later
revision is not interchangeable. Immutable series/instrument scope, active source
state, observation time and both root clocks constrain every candidate; missing,
ambiguous or unsupported inputs fail before reclamation. The current producer
uses legacy v1 OI/funding. Admission does not pass exact-numeric v2 through a float
decoder or invent a predecessor when the original gap/window selection omitted it.
The existing derivation owner recomputes the declared output from those inputs;
canonical source edges retain only the exact selected commits. Input placement is
not part of immutable identity and is freshly checked at destructive handoff.

Derivative Dataset freeze now also binds all canonical revisions, with unchanged
ordinary typed/latest reads. Older latest-only freezes must be re-frozen rather
than relabeled. The physical-retention regression covers hot and cold inputs,
same-valued competing predecessor revisions, source movement/corruption, frozen
binding validation, known-at reads, invalidations and re-freeze identity. V7 pages
require explicit reverification: it may append newly required material-alias and
canonical-source edges, never rewrite the page, existing bindings or old receipt.

This admission exposed a retained-v1 reader defect: the OI log change is computed
at 38 decimal digits, while the existing canonical serializer stores the exact
28-digit half-even representation. The typed validator now accepts either that
specific stored representation or the original full-precision calculation. It
preserves the supplied value and hashes; it does not apply an arbitrary tolerance,
rewrite historical precision, or change the global decimal serialization schema.

Performance follow-up: material-source and book-source waves each enforce their
logical-byte limits before hydration, but may decode the same whole cold page
again across waves or resumed steps. These are bounded per-wave costs, not a
single shared memory/I/O allowance. Measure representative HDD windows before
raising budgets or scheduling frequent runs; verified page reuse is an
optimization opportunity, not permission to skip source checks.

### Checkpoint File Admission

The shared checkpoint reader uses physical Parquet files and 128-row Arrow
batches, with explicit bounds on file bytes, levels, decoded bytes, row groups,
and decimal text length. It checks the exact non-null column schema, ZSTD,
metadata and per-row identities, homogeneous provider units, contiguous
per-side ordinals, strictly increasing positive Decimal prices, positive
quantities, and both sides' complete counts. Non-finite values and exponent text
are rejected before formatting can amplify a tiny input into a huge string.
The checkpoint writer and reader share the unchanged v1 content-fingerprint
serialization; verification recomputes it across every sorted level.

Replay and collector recovery pass the recorded checkpoint manifest to this
reader. It streams the current SHA-256, checks every byte/count/unit/format/hash
binding, and rejects a file that changes during verification. These callers no
longer load a whole checkpoint file into a bytes buffer merely to hash it. A
read still returns one complete bounded checkpoint because reconstruction needs
that state; bounded Arrow batches are not a claim of constant total state memory.

Collector recovery and retention share `restore_book_checkpoint_parquet` for
immutable manifest/validity hydration and `Level2BookReconstructor.from_checkpoint`
for state validation. This verifies checkpoint state consistency, not a second
raw-history reconstruction path. Full raw prefixes are independently admitted;
real cold-book regressions compare full and checkpoint-delta replay. No checkpoint
is fabricated, re-frozen, or rewritten. The source-edge metadata requires the
explicit schema upgrade below; the checkpoint wire format is unchanged.

### Resumable Canonical Verification

`verify_next_page` fully decodes one unverified archive page and checks every
retained envelope/source field against PostgreSQL, including document hashes,
identity, revision, and causal clocks. It reads immutable headers, not another
copy of the hot JSON documents. Its inclusive ordered header range must match
every archived row. The series, legacy-alias, and dependency catalogs must equal
the sets derived from the decoded page; all referenced raw bytes are rechecked.

Only that successful transaction inserts an immutable, versioned
`fact_archive_verifications` receipt. It binds the page descriptor, placement,
ordinal, and exact catalog hash. A failed transaction leaves no receipt, so a
restarted worker retries that page. Receipt updates/deletes are prohibited.
Additional catalog entries after a receipt invalidate its binding rather than
silently becoming trusted lineage.
The verifier version names the complete deep-admission rules. Strengthening
lineage/dependency checks requires a new version; an older receipt must never
bypass a new check simply because its unchanged file checksum still matches.

For an already-verified hot partition with older receipts,
`restart_partition_verification` explicitly returns progress to `sealed` and
clears only its deletion-admission hash/time under the existing fences. It
retains every payload, manifest, dependency hold and immutable old receipt, and
logs the previous hash and new verifier version. The bounded executor reports
this as a separate `restart_verification` step; subsequent runs verify missing
current-version pages and then the whole partition. Reclamation stays blocked
through interruptions or corrupt/missing evidence. Empty partitions follow the
same versioned-admission rule. Current-version proof changes are not repaired
by this path, and a reclaimed partition cannot be reopened. No schema migration
or rewriting/re-freezing of Fact identities is involved in receipt renewal.

`verify_partition` checks contiguous page ordinals, disjoint ordered ranges,
all current receipt/catalog bindings, and exact equality of the sealed source
count, current header count, and sum of page counts. Full range proofs plus
nonoverlap and equal cardinality exclude gaps before, between, or after pages.
Page metadata is fetched in batches of at most 100; each page's metadata has
separate row/dependency limits. Full canonical payload decoding is not repeated
on every final-pass retry.

The final pass nevertheless streams a **fresh checksum of every distinct
canonical/dependency object**, verifies canonical file sizes, and checks that
no dependency has an expiration event. Shared objects are read once per pass.
Default final-pass bounds are 10,000 pages, 100,000 objects, and 64 GiB; exceeding
a bound fails rather than skipping evidence. These are explicit work limits,
not claims about production throughput or the appropriate daily storage budget.
File device/inode/mode/size/mtime/ctime are checked around reads and again before
admission. These short-handoff checks never replace a fresh checksum on a later
run. The object-store mount/root guards apply to every path lookup.

On success, the partition becomes `verified` with a hash of the ordered page
proofs. **All hot payloads remain intact.** This state is cold-copy admission,
not deletion authorization. The separate default-disabled reclamation primitive
repeats the current-byte gate, safely hands off to the exclusive lifecycle fence,
rechecks evidence and file identity, and commits physical reclamation atomically.
No scheduled or CLI retention entrypoint is enabled by these verification methods.

Verification holds the lifecycle **shared** fence and the chosen partition's
worker fence. Expiry and a competing same-day worker cannot pass, while shared
dataset work and current-day collection can proceed. The lifecycle service
must not invoke this phase from inside its existing exclusive-lock context on
another connection: that would wait on its own lock. Expensive shared work and
the final short exclusive reclamation phase must be orchestrated separately.

### Default-Disabled Physical Reclamation

`PostgresCanonicalFactReclamationRepository` is a one-day execution primitive,
not an activated retention policy. Both constructor `enabled=True` and call
`execute=True` are required. The default call performs admission reads only:
it does not seal a partition, publish files, update progress, or delete anything.
An explicit `eligible_before` placement-day cutoff must be no later than the
database's current UTC day; the target must be strictly older than that cutoff.
Per-family hot windows and budget/pressure planning now have an inspectable CLI
surface. The lifecycle executor supplies these windows under the partition lock.
Complete dependency admission and reviewed production activation remain rollout
gates; wiring the executor is not itself production permission.

Standalone candle, funding, open-interest, reference-price, reserve-balance, structured reserve-report,
trade, trade-flow buckets/features, L2/BBO/depth, futures/spot basis and derivative-state facts are currently admitted. Any other family in the
physical day blocks the **whole day**, including remaining composite and normalized facts
whose complete dependency closures are not yet proven. This is a temporary
fail-closed compatibility gate, not permission to omit those rows or expire
their evidence. It cannot be lifted merely because a partition is `verified`.

Reclamation repeats current-byte verification under the shared lifecycle fence,
then releases that transaction before trying an exclusive transaction fence.
Existing frozen-read/pin workers cause a retryable busy failure; there is no
blocking shared-to-exclusive upgrade. The exclusive phase repeats receipt,
catalog, source-count and dependency-expiry checks, compares the admitted hash,
and checks file identity again. Canonical source edges recheck exact headers and
current placement; each cold source page is freshly hashed too. A separate
ephemeral placement hash detects a hot-to-cold move between the two phases and
requires a retry. Placement is deliberately excluded from immutable receipt
hashes so a legitimate move cannot invalidate a dependent forever.
It verifies the exact generated table's OID,
regular relation kind, parent, and one-day partition bounds. Parent and child
`ACCESS EXCLUSIVE` locks use `NOWAIT`; a reader or collector holding a conflicting
table lock causes a safe retry rather than a queued ingestion stall.

The only destructive statement is `DROP TABLE` for that single daily hot-payload
relation, without `CASCADE`. Table/index/TOAST allocation is reclaimed at commit.
The drop and `reclaimed` progress/byte count commit together. File stamps and the
handoff budget are checked after the drop but before commit too; a failure rolls
back both DDL and progress. An interrupted or unacknowledged commit can be retried:
an already-reclaimed day must have no remaining relation and still pass current
cold evidence checks. A reappearing relation or changed proof fails explicitly.

The configured HDD mount must remain writable at initial admission, after byte
verification, and around the destructive handoff, including after transactional
DROP. Research reads intentionally permit read-only media; reusing that weaker
read admission for reclamation would miss a read-only remount with unchanged
file stamps. Reclamation therefore repeats the existing configured UUID/root
guard with `require_writable=True`. A failing mount check rolls back the drop.

Canonical headers, archive objects, aliases, dependency holds and frozen dataset
bindings are never deleted by this primitive. A frozen range protects evidence
from expiry, not from a byte-equivalent tier move. Complete all-revision archive
coverage preserves every pinned subset; overlapping dataset-range counts are
reported for audit and refreshed during the exclusive handoff.

Each SQL statement defaults to a 1-second timeout. The exclusive phase has a
10-second checked handoff budget, with checks between pages/dependencies and
before committing deletion. These are conservative failure bounds, not measured
production capacity or a promise that transaction commit/fsync completes within
10 seconds. Large catalogs can fail these bounds safely and need measured tuning.
No production activation is justified by small disposable fixtures alone.

Performance follow-up: final catalog/source checks run twice and temporarily
exclude frozen-read/pin admission during the exclusive pass. Large header counts,
many dependency edges, and busy parent-table readers need representative sizing
and retry/backoff measurements before enabling unattended retention. Expensive
file reads stay outside the exclusive phase; no freshness checks are removed to
make the benchmark pass.

### Explicit Storage Cutover

`scripts/db/manual_migration_fact_storage_tiers_v1.py` is an offline operator
tool using only `PG_DSN`. Without flags it inspects read-only. Execution requires
both `--execute` and `--writers-stopped`; it never stops services itself. Apply
the prior canonical and operational-rollup migrations first. Capacity planning
must allow both copies plus WAL; the tool does not remove the old copy.

For an already-ready older tiered layout, inspection reports missing book-prefix
tables as `book_prefix_metadata_required`, the canonical-source edge table as
`canonical_dependency_metadata_required`, or both as `proof_metadata_required`.
It creates nothing. Explicit execution and stopped-writer acknowledgement create
only the missing empty tables and their append-only guards in one transaction.
Startup refuses the missing tables; it cannot install this upgrade. Existing
Facts, storage placement, and page receipts are untouched, and no prefix is
presumed verified. A partially present prefix pair or an unfinished older cutover must
be inspected/completed before this upgrade; the command does not adopt it.

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
