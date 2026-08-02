---
component: market-structure-data-plane
subsystem: data
layer: design
doc_type: architecture
status: active
tags:
  - market-data
  - market-structure
  - trades
  - level2
  - replay
  - object-storage
  - datasets
  - known-at
  - active
code_paths:
  - src/data_providers/streams
  - src/data_providers/providers/coinbase.py
  - src/market_data
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/market_collection.py
  - portal/backend/workers
  - cli/main.py
  - docker/docker-compose.yml
  - cli/mcp_server.py
  - config/defaults.yaml
  - scripts/db
  - docs/architecture/data/DATA_BOUNDARY.md
---
# Market Structure Data Plane

## Status And Campaign Boundary

This phased design is implemented through Phase 4: provider proof, bounded
futures/spot trades, Level 2 archive/reconstruction, typed market-state
features, causal normalization, and frozen typed datasets. No phase or document
authorizes production collector enrollment, cloud resources, strategy changes,
live trading, or frontend work. The 24-hour capacity proof and explicit budget
approval remain post-Phase-4 gates and are mandatory before production
enrollment.

The allowed live provider boundary is Coinbase Advanced Trade REST and
WebSocket using the existing provider credential boundary. Public channels may
run without credentials when that behavior is proven. Direct Coinbase
Derivatives Exchange (CDE) FIX, SBE, UDP, multicast, Participant Firm, and DCC
credential surfaces are outside the implementation boundary. CDE documentation
may define venue semantics, but it is not evidence that Advanced Trade exposes
the same fields.

The design extends, and does not replace, the contracts in:

- [Data Boundary](DATA_BOUNDARY.md)
- [ADR 0050: One Canonical Append-Only Market Store](../decisions/0050-use-one-canonical-append-only-market-data-store.md)
- [ADR 0051: Frozen Datasets For Canonical Backtests](../decisions/0051-require-frozen-datasets-for-canonical-backtests.md)
- [ADR 0052: Typed Fact Collectors And Instrument Roles](../decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md)
- [ADR 0044: Known-At Prefix Invariance](../decisions/0044-enforce-known-at-prefix-invariance.md)
- [ADR 0046: Exact Input Fingerprints And Separate Quality](../decisions/0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md)
- [ADR 0020: Budgeted Market-Stream Reconnect](../decisions/0020-use-budgeted-market-data-stream-reconnect-policy.md)
- [ADR 0024: Provider Credential References](../decisions/0024-use-provider-credential-references.md)
- [ADR 0053: Tiered Market-Structure Archive And Replay Boundary](../decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md)
- [Market Structure Phase 4: Normalization And Frozen Datasets](MARKET_STRUCTURE_PHASE_4_NORMALIZATION_DATASETS.md)

## Decision Summary

1. Assign every accepted WebSocket frame a stable `raw_record_id` and
   `spool_segment_id`, then fsync it before publishing canonical facts. Object
   upload may follow, but immutable archive acknowledgement is mandatory before
   the record is archive-complete or eligible for a frozen dataset.
2. Use local block storage only as a bounded durable spool/WAL. Use object
   storage for compressed immutable archives. Keep typed hot query surfaces in
   the existing PostgreSQL/Timescale `market` schema under `PG_DSN`.
3. Reuse the current collector's fencing, provider ownership, provenance,
   quality, revision, and shared commit-clock semantics. Add a continuous-stream
   lifecycle rather than forcing WebSocket sessions into scheduled poll
   attempts.
4. Treat Coinbase Advanced Trade `market_trades.side` as maker side. Preserve it
   verbatim and derive aggressor side only through an explicit versioned
   inversion after Phase 0 proves that CDE futures use the documented schema.
5. Reconstruct Advanced Trade L2 from its snapshot plus absolute-quantity
   updates. A book is valid only inside an explicit validity interval. Gaps,
   reconnects, divergent duplicates, invalid quantities, or crossed post-batch
   state end the interval and suppress book-derived facts until a new snapshot.
6. Store typed source facts separately from reproducible derived facts,
   versioned normalization specifications, operational materializations, and
   dataset-frozen values.
7. Freeze all research/backtest inputs into the existing dataset boundary.
   Backtests never read providers, live streams, mutable book state, or an
   unfrozen operational feature table.
8. Start with a strict three-pair candidate allowlist and admit pairs in order
   only after measured capacity: BIP/BTC, ETP/ETH, then SLP/SOL. No wildcard
   enrollment exists.

## Repository-First Findings

### Reuse And Extension Map

| Existing boundary | Reuse unchanged | Required extension | Boundary violation to avoid |
|---|---|---|---|
| Typed collectors | provider handler registration, pacing, retries, leases, fencing, attempts, gap evidence | continuous stream definitions, sessions, receive ordinals, spool and archive acknowledgement | one bot-owned collector or an unfenced daemon |
| Canonical market store | sources, series, shared fact commit sequence, append-only revisions, provenance, immutable quality | typed trade, L2, book, product, relationship, and feature tables | universal JSON fact table or provider read fallback |
| Provider contracts | implemented-feature declarations and provider-owned credential resolution | declare only implemented trade/L2/product operations and their auth mode | advertising every Coinbase capability or copying credentials into collector definitions |
| Dataset planning | exact range, commit watermark, material/provenance/quality hashes, immutable manifest | registry-driven fact planning, archive/spec references, funding and market-structure delivery | execution-time acquisition or mutable feature reads |
| Runtime delivery | known-at selection, required/optional/staleness/gap policy, engine snapshot delivery | market-structure fact types only when a consumer is implemented | feeding mutable book internals directly to a strategy |
| Paper stream | reconnect budget, heartbeat staleness, provider stream abstraction | acquisition must move to independently owned durable producer sessions | treating the paper bot's in-memory candle stream as archival truth |
| CLI/API | `qt` as operational workflow, API-backed commands | stream/archive/replay/quality/dataset operations | frontend-owned workflow truth or direct operator SQL |
| Persistence | one PostgreSQL boundary, strict schema drift, manual migrations | one object archive authority plus manifest references in PostgreSQL | another relational DSN or runtime schema patching |

### Concrete Current Limitations

- Phase 1 supports `market_trades` plus heartbeats and Phase 2 supports `level2`
  plus heartbeats on one product per connection. Both remain bounded and
  production-unenrolled.
- Market-structure archive identity is deterministic through `raw_record_id`
  and `spool_segment_id`; the generic `CanonicalMarketEvent.event_id` remains
  unsuitable as raw evidence identity and is not used for that purpose.
- observed Advanced Trade sequence tracking is connection-epoch scoped. The
  bounded collector uses one product per connection and never claims a stronger
  cross-product ordering contract.
- the paper runner is candle-specific, owned by a bot run, and publishes only
  after candle persistence. It is not an independently recoverable data plane.
- OI and funding have typed append-only storage. Phase 3 causally aligns both
  into derivative-state facts; Phase 4 registers and freezes them through the
  same typed dataset boundary.
- shared commit-clock, hypertable, and immutability setup enumerates Phase 1–4
  source, feature, normalization, and frozen-reference tables without creating
  another clock.
- the live catalog contains BIP, ETP, and SLP Coinbase futures. Phase 1 pair
  configuration registered canonical direct Coinbase BTC-USD. ETH-USD and
  SOL-USD remain explicit on-demand registrations and are not enrolled.

## Source And Coverage Inventory

Status means:

- **confirmed**: official Advanced Trade contract is sufficient for the stated
  use, or the current repository has observed the field through that surface;
- **conditional**: a bounded Phase 0 capture must prove product access or
  semantics before implementation;
- **unsupported**: outside the allowed provider boundary or lacking a stable,
  unauthenticated publication contract.

| Source/channel | Auth | Role/products | Snapshot/incremental and batching | Time and ordering | Duplicate/gap/recovery | Historical value | Retention value | Status |
|---|---|---|---|---|---|---|---|---|
| Advanced Trade WS `market_trades`, spot | public; authenticated CDP recommended | BTC-USD, ETH-USD, SOL-USD | initial `snapshot`, then 250 ms `update` batches containing one or more trades | trade `time`; envelope `timestamp`; Phase 0 observed one connection-wide `sequence_num` across subscribed channels, reset on reconnect; local receipt/acceptance becomes known-at | dedupe by provider trade ID; a connection-sequence gap affects every subscription on that connection; typed coverage intervals detect gaps and reconnect; explicit zero requires the complete rule below; recent REST trades validate only | no complete event-level backfill documented | irreplaceable event evidence | confirmed for BIP/BTC proof scope |
| Advanced Trade WS `market_trades`, CDE futures | same surface/auth contract | BIP, ETP, SLP product IDs | captured schema is the same; Phase 0 proved product access and contract units | same fields; maker-side semantics documented and captured | same typed coverage policy; BIP Phase 1 live capture passed | no complete event-level backfill documented | irreplaceable event evidence | confirmed semantics; only BIP Phase 1 live-verified |
| Advanced Trade WS `level2`, spot | public; authenticated CDP recommended | matching spot allowlist | `snapshot` then `update`; each event contains ordered absolute level quantities | update `event_time`; envelope `timestamp`; Phase 0 observed the same connection-wide `sequence_num`; receipt/known-at locally assigned | channel is documented as guaranteed; validate the connection sequence; reconnect resets it and requires a new snapshot | none documented | irreplaceable book evidence | confirmed for BIP/BTC proof scope |
| Advanced Trade WS `level2`, CDE futures | same surface/auth contract | BIP, ETP, SLP | Phase 0 captured the absolute-quantity contract and product units | same fields and local times | same validity contract; no assumed native retransmit | none documented | irreplaceable book evidence | implemented and live-verified for bounded BIP; ETP/SLP unenrolled |
| Advanced Trade WS `heartbeats` | public | every stream session | one-second heartbeat and counter | server current time, envelope time, receipt | counter discontinuity is transport evidence, not a product-book sequence substitute | none | session/gap evidence, low volume | confirmed |
| Advanced Trade WS `ticker` | public | futures and spot allowlist | snapshot/update, may batch cascading matches | envelope/event receipt; no replacement for trade event time | validation only; not canonical trade recovery | no event history | BBO/trade cross-check only | confirmed |
| Advanced Trade WS `status` | public | subscribed products | periodic product/currency snapshots | provider/envelope and receipt | revision by material hash | no reliable history promised | product-state cross-check | confirmed, optional |
| Advanced Trade REST public product(s) | docs call it public; individual pages show Authorization | futures and spot metadata | point-in-time snapshot | provider fields plus receipt/known-at; no publication time assumed | material hash revisions; retry using existing provider pacing | expired products may be listable; not an event history | contract multiplier, increments, session state, OI/funding source | confirmed with existing CDP; unauth mode conditional |
| Advanced Trade REST product book | same public/auth ambiguity | one product, bounded depth | point-in-time bids/asks, no documented sequence in Advanced Trade response | book `time`, receipt/known-at | comparison/resync evidence only; cannot splice safely into WS without a common sequence | no | validation, not primary reconstruction | conditional |
| Advanced Trade REST public market trades | same public/auth ambiguity | one product, recent ticks | recent snapshot, bounded result | provider trade time and ID, receipt | reconcile recent IDs after reconnect; must not claim completeness | recent only, no range pagination contract | bounded validation/reconciliation | conditional |
| Existing Advanced Trade product OI | current repository public path | BIP, ETP, SLP | scheduled point-in-time poll | no provider event/publication time; sample schedule and receipt/acceptance known-at | existing typed revisions, retries, gaps, fence | no supported backfill in repository | durable contracts, minute-level | confirmed |
| Existing Advanced Trade product funding | current repository public path | BIP, ETP, SLP | scheduled point-in-time poll | provider `funding_time` preserved; meaning not assumed; receipt/acceptance known-at | existing typed revisions, retries, gaps, fence | no supported backfill in repository | durable rate observation | confirmed; final/predicted meaning conditional |
| CDE REST historical funding `/rest/funding-rate` | live unauthenticated proof returns `401`; requires CDE request credentials | native CDE symbol such as BIPZ30 | trading-session query is documented, but unavailable through Advanced Trade/CDP credentials | provider `event_time`; publication/known-at unavailable historically | direct CDE auth and mapping would be required | unavailable inside this campaign's provider boundary | semantic reference only | unsupported under provider boundary |
| CDE public finalized-funding webpage/files | no stable documented public machine endpoint found; the public historical table does not expose funding | CDE perpetual-style contracts | no admissible source contract | unavailable | no stable identity, schema, revision, pagination, or symbol-mapping contract | not verified | none | unsupported; explicit coverage gap |
| CDE public daily price/volume/OI/settlement page | public human webpage; rows are loaded through an undocumented website-internal CMS token | CDE contracts including BIP, ETP, and SLP | observed rolling daily rows include price, settlement, volume, block volume, and OI; no stable documented download/API contract | trading date is present; publication time is visible only as mutable CMS metadata and is not a contractual known-at rule | page may be challenged; underlying token, schema, rolling window, and correction behavior are not provider contracts | useful only for bounded manual sanity checks | no canonical retention | unsupported as an automated or dataset source; explicit coverage gap |
| CDE public derivatives dashboard | public webpage | venue-level and filtered aggregates | current/daily chart values | displayed dates; exact release time and revision policy unverified | no collector contract inferred from page internals | 7/30/90-day UI windows visible | venue-level sanity checks only | unsupported as canonical source |
| CDE public block-trade webpage | public webpage | reported CDE blocks | table with date/time/product/type/symbol/qty/price | Central Time display; publication latency/ID/revisions absent | no stable machine identity or pagination contract proven | recent page history visible | optional off-book context; not central-book flow | conditional and excluded from Phases 0-4 |
| CDE REST block-trade API | CDE auth/firm permissions despite OpenAPI ambiguity | firm-owned/permissioned block trades | query/booking surface | native fields | permissioned | bounded by permission | not public market-wide evidence | unsupported |
| Direct CDE FIX/SBE/UDP/multicast | Participant Firm/institutional connectivity | native CDE | native incrementals, snapshots, retransmits, richer definitions | native venue ordering | native recovery channels | not through Advanced Trade | venue semantics reference only | unsupported |
| Coinbase Data Marketplace | purchased product/SFTP | purchased datasets | files/manifests | marketplace-defined | purchased manifest checks | yes | potential future external source | unsupported for this campaign |

Official evidence used for this inventory:

- [Advanced Trade WebSocket channels](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels)
- [Advanced Trade WebSocket overview and sequence numbers](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)
- [Advanced Trade REST endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api)
- [Advanced Trade public product contract](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/public/get-public-product)
- [Advanced Trade public product book](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/public/get-public-product-book)
- [CDE REST authentication](https://docs.cdp.coinbase.com/api-reference/derivatives-api/rest-api/authentication)
- [CDE historical funding endpoint](https://docs.cdp.coinbase.com/api-reference/derivatives-api/rest-api/funding-rate/get-historical-funding-rates)
- [CDE public block-trade page](https://www.coinbase.com/derivatives/block-trade-data)
- [CDE historical-data page](https://www.coinbase.com/derivatives/historical-data)
- [CDE public funding-data page](https://www.coinbase.com/derivatives/funding-rates-data)

## Initial Product Allowlist

The candidate allowlist follows the currently registered CDE futures and their
root units. The mapping is explicit data; no runtime symbol parsing is allowed.

| Admission order | Futures instrument | Futures product | Contract size currently observed | Matching spot product | Catalog state |
|---|---|---|---|---|---|
| 1 | `b2deb0a0-f292-408a-876d-3dadd8e3819b` | `BIP-20DEC30-CDE` | 0.01 BTC/contract | `BTC-USD` | direct Coinbase spot instrument missing |
| 2 | `44226144-fb38-4566-92c4-580734d76d3c` | `ETP-20DEC30-CDE` | 0.1 ETH/contract | `ETH-USD` | direct Coinbase spot instrument missing |
| 3 | `bead556e-22e2-4ac0-8ee0-0d8c5310e9a0` | `SLP-20DEC30-CDE` | 5 SOL/contract | `SOL-USD` | direct Coinbase spot instrument missing |

Phase 0 captured BIP/BTC first, then proved ETP/ETH and SLP/SOL access and units
through bounded spot checks. All three pairs may be represented by Phase 1–4
implementation and tests. Production collector enrollment remains sequential
and blocked until the post-Phase 4 capacity gate proves total storage, CPU,
replay, and backlog budgets with a 3x observed-p99 safety factor.

## End-To-End Data Path And Authorities

```text
provider frame
  -> fenced stream session
  -> assign raw_record_id + spool_segment_id
  -> fsynced local spool record
       -> deterministic parser/validator
       -> typed canonical facts referencing raw_record_id (archive pending)
       -> valid book/trade coverage + derived facts
  -> sealed spool segment -> immutable object -> acknowledged manifest
       -> append manifest-to-record mappings (archive complete)
  -> versioned causal normalization
  -> immutable dataset manifest/materialization (acknowledged records only)
  -> research, backtest, or runtime resolver
```

Authority is intentionally split:

- the fsynced spool record is durable pending receipt evidence; it is not the
  long-term archive authority;
- acknowledged raw object bytes plus manifest-to-record mappings are
  authoritative archive evidence for what the WebSocket client received;
- PostgreSQL typed revisions are authoritative for canonical query facts;
- book reconstruction code plus version is authoritative for reproducible book
  state, never an in-memory map by itself;
- normalization specifications are authoritative for feature semantics;
- frozen dataset manifests and objects are authoritative for backtest inputs;
- runtime snapshots remain authoritative for what a strategy could observe.

## Identity And Time Semantics

Every persisted record declares these times where applicable:

- `provider_event_time`: time assigned to the trade/update by Coinbase; nullable
  when absent and never fabricated;
- `effective_time`: the canonical market time at which the fact applies;
- `provider_message_time`: top-level Advanced Trade envelope timestamp;
- `received_at`: local wall-clock time immediately after a complete frame is
  received;
- `accepted_at`: time the platform durably admits the parsed fact;
- `known_at`: earliest causal visibility, never earlier than `received_at` or
  `accepted_at`; for historical files, publication/discovery time unless the
  source proves an earlier publication time;
- `receive_ordinal`: monotonically increasing integer inside one connection
  epoch, assigned before spool append; it is an acquisition order, not venue
  event order;
- `spool_segment_id`: UUID written to the segment header before its first
  record and preserved through recovery, upload, and compaction lineage;
- `raw_record_id`: deterministic UUIDv5 over stream definition, session,
  connection epoch, receive ordinal, and exact raw-frame SHA-256; assigned
  before parsing and unchanged by upload or compaction;
- `fact_commit_seq`: the existing database-wide ordering of admitted canonical
  revisions.

When product metadata has no provider event/publication time, its effective and
known-at boundary is platform receipt/acceptance. It is not backdated to an
exchange session open or contract-listing date.

Decimals use exact `NUMERIC`, not binary floating point. Hash inputs use
canonical UTF-8 encodings, normalized UTC timestamps, exact decimal strings,
explicit nulls, and sorted field/level order as specified by a contract version.

## Canonical Data Model

### Shared Rules

- All source and derived fact versions use `market.fact_commit_seq`.
- Materially different fact families have typed physical tables.
- Append-only tables receive immutability triggers. Mutable rows are restricted
  to operator configuration, leases, and disposable projections.
- `source_id`, `series_id`, `ingestion_run_id`, `raw_record_id`,
  `contract_version`, `provenance_hash`, and relevant quality references are
  present on canonical facts. Canonical facts do not require or later acquire a
  manifest ID.
- A provider message hash is SHA-256 of the exact raw frame bytes. A canonical
  material hash is independent of receipt/provenance/quality.
- Timescale hypertables partition high-volume facts by effective time; ordinary
  PostgreSQL tables hold definitions, manifests, specifications, and datasets.
- Foreign keys enforce ownership for control/configuration data. High-volume
  child rows use validated parent IDs and deferred/batched FK checks only if
  measured ingestion proves ordinary FKs are too expensive.

Ownership and foreign-key direction is fixed:

```text
market.sources
  -> market.stream_definitions
      -> stream_session_events -> raw_archive_manifests -> raw_archive_ranges
      -> raw_archive_record_mappings
      -> stream_coverage_interval_versions
      -> stream_quality_events
  -> market.series
      -> source/derived fact revisions
          -> L2 snapshot levels / mutation batches / mutations
          -> book validity intervals / checkpoints
          -> BBO / depth / flow / relationship facts
              -> normalized features <- normalization specs
market.datasets
  -> dataset_series
  -> dataset_archive_refs
  -> dataset_normalization_refs
```

The dominant queries are append by fenced source position, time/known-at range
selection by series, latest accepted revision at a commit watermark, replay by
session/epoch/ordinal, book resume by latest checkpoint, and dataset pin lookup.
No hot query scans raw frame bytes.

### Control, Session, Archive, And Quality Tables

| Table | Purpose and grain | Key/idempotency | Important columns and time | Mutability/revision | Partition/index, writes, retention |
|---|---|---|---|---|---|
| `market.stream_definitions` | one allowlisted provider/venue/channel/product group | PK UUID; unique material identity over provider, venue, ordered product IDs, channels, contract version | source, auth mode, product IDs, channel set, enabled, reconnect/spool/archive budgets, generation, created/updated; no secret or credential reference | mutable operator definition; generation increments on ownership-relevant change | B-tree enabled/provider; low write; retain while referenced |
| `market.stream_lease_state` | current disposable ownership projection per definition | PK/FK definition; fence `(definition_id, lease_generation, token_hash)` | owner, token hash, generation, claimed/heartbeat/expires | mutable and reconstructable from session evidence; never a source fact | claim/expiry indexes; hot only |
| `market.stream_session_events` | one immutable lifecycle event per connection epoch | PK UUID; unique `(session_id, event_ordinal)`; idempotency includes event type/material hash | session, definition, owner generation, connection epoch, connected/disconnected/resync/error event, occurred/received/known-at, reason, counts | append-only | monthly by occurred_at; session/event index; retain indefinitely because low volume |
| `market.raw_archive_manifests` | one sealed immutable object | PK UUID; unique object URI and object SHA-256; content fingerprint over ordered record hashes | definition/session/epoch, object URI, format/schema/compression, byte/record counts, first/last ordinal, time bounds, uploaded/acknowledged at, checksum, content fingerprint | insert only after upload verification; append-only | date/provider/channel/product indexes; manifests indefinite; object follows tier retention |
| `market.raw_archive_ranges` | per manifest/product/channel ordering summary | PK `(manifest_id, product_id, channel)` | first/last sequence when present, min/max provider event/message/receipt time, count, gap count | append-only child of manifest | product/time indexes; same as manifest |
| `market.raw_archive_record_mappings` | one immutable placement of one preassigned raw record in one acknowledged object | PK `(raw_record_id, manifest_id)`; unique `(manifest_id, object_row_index)`; idempotency includes raw hash | spool segment, session/epoch/receive ordinal, manifest, object row group/index, raw SHA-256, mapped/known-at; compaction may append another placement | append-only; facts are never updated; mapping exists only after object verification | raw-record and manifest indexes; append-heavy; retain with manifest/pins |
| `market.raw_archive_compaction_sources` | one ordered immutable source-manifest edge for a compacted replacement | PK `(replacement_manifest_id, source_manifest_id)`; unique replacement/source ordinal | replacement fingerprint, source ordinal, compacted/known-at | append-only; source objects remain authoritative until all replacement manifest, mappings, and lineage commit | source/replacement indexes; low write; retain indefinitely |
| `market.archive_retention_pin_versions` | one explicit operator pin or release revision for a raw manifest or book checkpoint | PK version ID; natural `(pin_id, revision)` | target kind/ID, owner kind/ID, status, reason, effective/known-at | append-only revisions; latest revision determines active explicit pin | target/known-at index; indefinite control evidence |
| `market.stream_coverage_interval_versions` | one revision of product/channel delivery coverage, including trade-stream validity | PK UUID; natural `(definition_id, session_id, connection_epoch, product_id, channel, interval_id, revision)` | opening/closing session-event and raw-record evidence, first/last sequence and ordinal, provider/message/receipt time bounds, ordering assurance, coverage status, archive status, canonicalization watermark, gap/quality refs, known-at | append-only; opening, archive completion, closure, or invalidation appends a revision | product/channel/time/status indexes; indefinite quality evidence |
| `market.stream_quality_events` | exact session/product/channel anomaly, invalidation, or recovery evidence | PK UUID; natural identity `(session_id, product_id, channel, receive_ordinal, classification, evidence_hash)` | sequence before/after, heartbeat counter, invalid reason, detected/known-at, raw record/manifest refs, related coverage/book interval, series, generic gap ID | append-only; correction is a new event | monthly by detected_at; classification/product index; indefinite or 7 years |

`stream_quality_events` supplements rather than replaces existing
`market.gap_evidence`. Transport-specific evidence is recorded first; a gap that
affects a canonical series also creates immutable generic gap evidence with a
reference back to the transport event.

Its classification is a closed versioned enum, initially `sequence_gap`,
`out_of_order`, `duplicate`, `divergent_duplicate`, `heartbeat_gap`,
`disconnect`, `decode_error`, `archive_loss`, `book_invalid`,
`resync_started`, and `resync_snapshot_accepted`. Sequence, ordinal, state-hash,
and reason fields are typed columns; this is not an opaque JSON error bucket.

A raw record becomes `archive_complete` only when at least one acknowledged,
non-expired or dataset-pinned manifest mapping matches its exact SHA-256. A
mapping appended after upload does not revise the canonical fact. A lost
unmapped spool record creates `archive_loss` evidence and can never become
dataset-eligible.

### Product And Relationship Tables

| Table | Purpose and grain | Key/idempotency | Important columns and time | Mutability/revision | Partition/index, writes, retention |
|---|---|---|---|---|---|
| `market.product_definition_versions` | one material provider-product definition revision | PK UUID; natural `(source_id, provider_product_id, effective_time, revision)`; idempotency material hash | canonical instrument, product/type/venue, status/session state, base/quote/root unit, increments, contract size/expiry, funding interval, raw record ID when streamed, provider/received/known-at, provenance | append-only revision; unchanged polls dedupe | product/effective/known-at indexes; low write; indefinite |
| `market.instrument_role_mapping_versions` | explicit futures-to-spot/benchmark role mapping | PK UUID; natural `(primary_instrument_id, role, effective_from, revision)` | related instrument, mapping reason/source, effective interval, received/known-at, material/provenance hash | append-only revision; overlapping latest mappings forbidden | primary/role/effective index; indefinite |

Mappings are operator- or provider-metadata-approved source facts. Product-root
similarity may propose a mapping but cannot publish it automatically.

### Trade Tables

| Table | Purpose and grain | Key/idempotency | Important columns and time | Mutability/revision | Partition/index, writes, retention |
|---|---|---|---|---|---|
| `market.market_trade_versions` | one provider trade revision | PK UUID; natural `(source_id, provider_product_id, provider_trade_id, revision)`; first material hash is idempotency | series/instrument, price, `provider_size`, `provider_size_unit`, maker side, nullable aggressor side + transform version, nullable contract/base quantity and quote notional, provider event/message time, receipt/acceptance/known-at, sequence, receive ordinal, raw record ID, coverage interval ID, provenance/quality | append-only; duplicate identical material no-op; conflicting same ID appends correction/revision evidence or fails per provider proof | hypertable by provider event time; product/time, known-at, trade ID indexes; append-heavy; hot 180d, raw/frozen longer |

No field is populated by guesswork. Contract quantity, base quantity, and quote
notional remain null until the exact size unit and applicable product-definition
revision are known.

### Level 2 And Book Tables

| Table/dataset | Purpose and grain | Key/idempotency | Important columns and time | Mutability/revision | Partition/index, writes, retention |
|---|---|---|---|---|---|
| `market.l2_snapshot_versions` | one accepted provider snapshot event per product | composite PK `(id, effective_at)` for Timescale; natural source position plus effective time; raw/event hash idempotency | series, sequence, event/message/receipt/known-at, level count, state hash after snapshot, raw record ID, provenance/quality | append-only | hypertable by effective time; series/time and commit indexes; hot 7d |
| `market.l2_snapshot_levels` | one typed side/price level in a snapshot | PK `(snapshot_version_id, snapshot_effective_at, side, price)`; parent ownership is atomic repository enforcement because installed Timescale rejects FKs to hypertables | absolute quantity, provider unit, update event time, ordinal | append-only child | snapshot/side/price index; one typed recordset insert per snapshot; hot 7d |
| `market.l2_mutation_batches` | one provider update event applied atomically | composite PK `(id, effective_at)`; natural source position plus effective time; raw/event hash idempotency | series, sequence, provider message/event bounds, receipt/known-at, mutation count, before/after state hash, validity interval, raw record ID | append-only; exact duplicate no-op; divergent duplicate invalidates | hypertable by effective time; series/time/commit and source-position indexes; hot 7d |
| `market.l2_mutations` | one ordered absolute level mutation in a batch | PK `(batch_id, batch_effective_at, mutation_ordinal)`; transactional parent ownership | side, exact price, new absolute quantity, provider event time, provider unit | append-only child; order is semantic | batch/side/price index; one typed recordset insert per batch; hot 7d |
| `market.book_checkpoint_manifests` | one deterministic reconstructable book checkpoint | PK UUID; natural `(series_id, reconstruction_version, checkpoint_time, source_position_hash)` | source session/epoch/sequence/ordinal, validity interval, object URI/checksum, sorted-level state hash, counts, created/known-at, source manifest range | append-only | series/time index; metadata indefinite; objects 90d or dataset-pinned |
| `book_checkpoint_levels.v1` object dataset | typed sorted levels for one checkpoint | natural `(checkpoint_id, side, price)` | exact quantity and unit; schema/reconstruction version | immutable Parquet/ZSTD object | ordered by side then numeric price; replay read; tiered retention |
| `market.book_validity_interval_versions` | one version of a valid or invalid reconstruction interval | PK UUID; natural `(series_id, interval_id, revision)` | start/end source positions and event/receipt/known-at times, status, reason, opening snapshot, closing quality event, reconstruction version | append-only; open revision has null end, closure appends final revision | series/time/status indexes; indefinite quality evidence |
| `market.book_quality_event_links` | typed relationship from a stream quality event to the affected book interval | PK `(quality_event_id, validity_interval_id, link_role)` | link role and known-at | append-only | interval and quality-event lookups; indefinite quality evidence |
| `market.book_reconstruction_state` | disposable current operational projection | PK series | current checkpoint/sequence/ordinal/state hash/validity/fence/updated | mutable; never dataset truth; rebuilt from archive/checkpoint | hot only |

### Derived, Reconciliation, Normalization, And Dataset Tables

| Table | Purpose and grain | Key/idempotency | Important columns and time | Mutability/revision | Partition/index, writes, retention |
|---|---|---|---|---|---|
| `market.bbo_feature_versions` | one-second best bid/ask from the last complete valid state in the bucket | natural `(series_id, bucket_start, revision)`; material hash idempotency | bid/ask price/qty, mid, spread absolute/bps, source state/position, validity interval, provider units, effective/known-at, input fingerprint | append-only revisions | hypertable; series/time/known-at; hot 400d, frozen longer |
| `market.depth_feature_versions` | one-second depth/imbalance observation for one fixed band | natural `(series_id, band_bps, bucket_start, revision)`; material hash idempotency | 5/10/25 bps band, bid/ask quantity/base/notional, bounded imbalance, source BBO/state/position, validity, known-at | append-only revisions | hypertable; series/time/band; hot 400d |
| `market.trade_flow_aggregate_versions` | causal 1s or 1m trade bucket | natural `(series_id, interval_seconds, bucket_start, aggregation_version, revision)` | counts, maker/aggressor buy/sell quantities, contracts/base/notional, CVD delta/cumulative anchor, OHLC/last, first/last trade source position, coverage interval/revision, coverage opening/closing positions, complete/late/archive flags, known-at | append-only bucket revisions; late trades or archive completion append later-known revision | hypertable; series/interval/time/known-at; hot 400d |
| `market.trade_flow_feature_versions` | validated flow/CVD projection from one complete aggregate | natural `(series_id, interval_seconds, bucket_start, revision)` | aggregate material/input hashes, buy/sell base and notional, CVD delta/share, known-at and input fingerprint | append-only; incomplete and zero-denominator aggregates emit no numeric feature | hypertable; series/interval/time; hot 400d |
| `market.derivative_fact_reconciliations` | comparison evidence between live OI/funding and a historical/public reference | natural `(left_series_id, right_source_id, fact_type, effective_time, reconciliation_version)` | left/right fact refs, unit transform, absolute/relative delta, tolerance, status, compared/known-at, evidence hash | append-only; never overwrites either source | fact/time/status index; indefinite quality evidence |
| `market.futures_spot_relationship_versions` | paired causal futures/spot observation | natural `(mapping_version_id, effective_time, relationship_contract_version, revision)` | future/spot source fact refs, mids, basis absolute/bps, staleness each side, alignment policy, known-at, quality | append-only revisions | hypertable; mapping/time/known-at; hot 400d |
| `market.derivative_state_versions` | one causal OI/funding relationship observation | natural `(series_id, effective_at, revision)` | exact OI/funding source series, sample times and commit sequences, OI level/previous/log change, provider-reported funding and interval, known-at/input fingerprint | append-only; a gap blocks OI change instead of imputing it | hypertable; series/time/known-at; hot 400d |
| `market.market_response_feature_versions` | one direction-specific flow/price/depth response horizon | natural `(series_id, direction, horizon_seconds, effective_at, revision)` | trade-flow and pre/trough/post book source refs, response bps, consumed/replenished selected-side depth, impact, validity, known-at | append-only; invalid/cross-interval evidence suppresses output | hypertable; series/time/direction; hot 400d |
| `market.normalization_specs` | immutable executable feature definition | PK UUID; unique `(feature_name, semantic_version, spec_hash)` | typed inputs, formula AST/identifier, units, window/partition, minimums, missing/validity/staleness policy, warmup, materialization mode, created/approved | immutable; new semantics require new version/hash | feature/version index; indefinite |
| `market.normalized_feature_versions` | one materialized operational normalized value | natural `(output_series_id, spec_id, effective_time, input_fingerprint, revision)` | exact numeric/enum value, input range/count/watermark, warmup/valid flags, effective/known-at, provenance/quality/input hashes | append-only; later inputs create later-known revision and never replace earlier causal visibility | hypertable; series/spec/time/known-at; operational hot 400d |
| `market.dataset_archive_refs` | immutable set of source archive objects pinned by a dataset | PK `(dataset_id, raw_archive_manifest_id)` | inclusion role, object checksum/content fingerprint | append-only with dataset; manifest must be acknowledged | dataset index; dataset lifetime |
| `market.dataset_normalization_refs` | immutable normalization/materialization binding | PK `(dataset_id, spec_id, output_series_id)` | input watermark/range/count, material/provenance/quality hashes, frozen object URI/checksum/row count | append-only with dataset | dataset/spec index; dataset lifetime |

Existing `market.datasets` and `market.dataset_series` remain the manifest and
typed-series authority. The two proposed reference tables add replay evidence;
they do not permit an execution path around the dataset admission contract.

## Level 2 Reconstruction Contract

### Source Position

The deterministic source position is:

```text
(stream_definition_id, session_id, connection_epoch,
 product_id, channel, sequence_num?, receive_ordinal, event_ordinal)
```

For the BIP/BTC Phase 0 proof, `sequence_num` is a connection-epoch counter
shared by subscription acknowledgements, heartbeats, trades, L2, and ticker; it
reset to zero after reconnect. It is not a per-product or per-channel counter.
The initial topology therefore keeps one product on each connection. A sequence
gap invalidates every active channel/coverage interval on that connection.
`receive_ordinal` and `event_ordinal` always provide a total local order inside
an epoch. They do not upgrade a receipt-contiguous stream into proof of lossless
venue delivery.

### State Machine

| Current state | Evidence | Deterministic action | Next state |
|---|---|---|---|
| disconnected/unknown | connection opens | archive immediately; subscribe to one channel per message and heartbeat | awaiting snapshot |
| awaiting snapshot | `update` | archive and record `update_before_snapshot`; do not apply or derive | awaiting snapshot |
| awaiting snapshot | valid `snapshot` | replace the entire map atomically, validate, hash, create validity interval and checkpoint | valid |
| valid | contiguous valid `update` | apply all mutations in array order atomically; validate final batch state; persist batch/state hash | valid |
| valid | exact duplicate frame/event | record duplicate evidence; no state change and no duplicate feature | valid |
| valid | divergent duplicate, gap, connection loss, invalid batch, or state mismatch | archive; close valid interval at last valid source position; emit quality/gap evidence; suppress book features | invalid/awaiting snapshot |
| invalid | any `update` | archive and persist evidence only; do not apply to the canonical state | invalid |
| invalid | valid fresh `snapshot` | replace all state, start a new validity interval/checkpoint | valid |

### Required Behavior

1. **Initial snapshot.** Accept only a provider event explicitly typed
   `snapshot`. A snapshot is complete for the product event, not for the entire
   top-level frame. Empty, duplicate-price, negative-quantity, invalid-decimal,
   unknown-side, or crossed snapshots are rejected and recorded.
2. **Absolute updates.** Coinbase `new_quantity` is a new absolute aggregate at
   the price level, never a delta. Positive values set/insert; zero deletes.
3. **Message atomicity.** A provider event's mutation array is ordered. Apply
   every mutation to a working copy in array order and validate only the final
   result. Do not emit intermediate BBO/depth states from inside a batch.
4. **Multiple provider events in one frame.** Process `events` in array order
   and assign `event_ordinal`; each event is its own atomic batch. The raw frame
   is archived once and referenced by all parsed facts.
5. **Unknown level.** A positive absolute update creates the level. A zero for
   an absent level is an idempotent no-op plus `unknown_zero_delete` quality
   evidence; it does not alone invalidate the book.
6. **Duplicates.** Equality is exact raw event hash plus source identity. An
   exact duplicate is a no-op. The same non-null sequence/source identity with
   different material is `divergent_duplicate` and invalidates the interval.
7. **Ordering.** When a sequence is present, the expected next value is the
   Phase 0-proven connection-epoch successor across every received channel. A
   forward jump ends every validity/coverage interval on that connection. A
   lower value is ignored only if it matches exact previously applied evidence;
   otherwise it is out-of-order evidence and ends validity.
8. **Absent sequence.** Without a usable sequence, the strongest honest claim
   is `receipt_contiguous`: valid from a complete snapshot while the same TCP
   connection remains open and no local decoder, spool, heartbeat, or resource
   anomaly occurs. Such intervals carry `ordering_assurance=receipt_contiguous`,
   not `lossless`.
9. **Connection loss.** Any close, timeout, heartbeat stale condition, process
   restart, or deliberate backlog disconnect ends validity. Updates from a new
   connection are not spliced onto old state. Resubscription must yield a new
   accepted snapshot.
10. **Crossed or invalid state.** After a full batch, best bid must be strictly
    below best ask; prices and quantities must conform to the effective product
    definition. Failure ends validity. The raw batch remains archived.
11. **Resynchronization.** Resubscribe with the existing budgeted reconnect
    policy. Do not use the Advanced Trade REST book as a splice unless Phase 0
    proves a common sequence/position contract. It may be comparison evidence.
12. **Checkpoints.** Write a checkpoint immediately after every accepted
    resynchronizing snapshot, then at the earlier of five minutes or 100,000
    applied mutations. Capacity measurements may lower frequency, but a
    contract-version change is required to raise either bound.
13. **Replay.** Choose the latest valid checkpoint whose source position is at
    or before the requested point, verify checksum and state hash, then apply
    ordered batches. If no checkpoint exists, replay from a valid snapshot.
14. **Validity and feature suppression.** Every BBO/depth/imbalance row names a
    valid interval and source state hash. No book-derived row is emitted in an
    invalid interval or across an unhealed gap. A downstream join propagates
    invalid/missing state rather than forward-filling it.

### State Fingerprint

`book_state_hash.v1` hashes:

```text
reconstruction_version
product_definition_version_id
source_position
ordering_assurance
bids sorted by numeric price ascending: (price_decimal, quantity_decimal)
asks sorted by numeric price ascending: (price_decimal, quantity_decimal)
```

Decimals are canonical non-exponent strings. A replay of identical evidence
must reproduce the checkpoint object, state hash, derived feature input hashes,
and quality events byte-for-byte except explicitly excluded operational fields
such as worker identity and wall-clock duration.

### Honest Limitation

Advanced Trade documents L2 as guaranteed delivery. Phase 0 observed a
connection-wide sequence across multiplexed channels, with a reset on every
reconnect, for BIP and BTC-USD. It did not prove native retransmission, a
FIX-equivalent venue sequence, order-level identity, queue priority, individual
order count, or lossless reconstruction across connections. ETP/ETH and SLP/SOL
subsequently proved access, unit semantics, reconnect snapshots, and exact book
replay in a bounded spot check. They remain production-unenrolled pending the
post-Phase 4 BIP/BTC capacity gate. Advanced Trade L2 is an aggregated
price-level book.

## Trade Semantics And Aggregation

### Canonical Trade Translation

Coinbase documents `market_trades.side` as the **maker side**. The canonical
record therefore always stores `maker_side=BUY|SELL`. The optional transform
`coinbase_maker_to_aggressor.v1` is:

| Coinbase maker side | Canonical aggressor side |
|---|---|
| `BUY` | `SELL` |
| `SELL` | `BUY` |

The transform is enabled for a product only after Phase 0 confirms the futures
messages use the documented contract. Unknown values fail parsing and do not
become unsigned trades. If semantics become ambiguous, `aggressor_side` is null
and every aggressive-flow/CVD feature is unavailable, not guessed.

### Quantity And Notional

- `provider_size` and its observed representation are always preserved.
- spot `base_quantity` is populated only after BTC/ETH/SOL capture agrees with
  product increments and REST recent-trade samples.
- futures `contract_quantity` is populated only after size-unit proof.
- when provider size is contracts and multiplier metadata is effective,
  `base_quantity = contract_quantity * contract_size`.
- for linear USD-quoted products,
  `quote_notional = price * base_quantity`; this remains null for a product
  whose payoff/unit contract has not been proven linear.
- product-definition revision ID is part of every derived quantity fingerprint.

### Identity, Duplicates, Corrections, And Ordering

- provider identity is `(source_id, provider_product_id, provider_trade_id)`.
  The trade ID remains a string; numeric coercion is forbidden.
- identical repeated material is idempotent. A conflicting same-ID material
  value creates `provider_trade_conflict` evidence. If Coinbase later documents
  corrections/busts, append a typed revision with that provider action;
  otherwise fail admission rather than invent correction semantics.
- canonical event order is provider event time, then proven provider sequence,
  then receive ordinal/event/trade ordinal. Equal event times remain distinct.
- a late trade is admitted with its original event time and later known-at. It
  appends a later-known aggregate revision; it never changes what was visible
  at an earlier decision time.

### Trade Stream Coverage Contract

Book validity and trade delivery coverage are separate contracts. A valid book
does not prove that all trades were received, and a healthy trade subscription
does not prove that an L2 book is valid.

`market.stream_coverage_interval_versions` scopes one interval to exactly one
stream definition, session, connection epoch, provider product, and channel.
For `market_trades`, an interval opens only after all contract-version opening
evidence exists:

1. the product/channel subscription is acknowledged;
2. the channel's Phase 0-proven initial baseline is accepted (snapshot or other
   documented first-message behavior);
3. a usable sequence/delivery-assurance baseline is recorded; and
4. the connection heartbeat policy is healthy.

Opening evidence stores the subscription session-event ID, baseline
`raw_record_id`, sequence when present, receive ordinal, provider message time,
receipt time, and known-at. An interval closes or becomes invalid at the first
disconnect, heartbeat stale condition, sequence gap, divergent duplicate,
decode error, fenced-owner loss, spool loss, or deliberate backpressure stop.
The closing revision names the exact session/quality event and last trustworthy
raw record/sequence/ordinal. A reconnect starts a new interval; intervals never
span connection epochs.

`ordering_assurance` is a closed versioned enum:

- `provider_sequence_contiguous`: Phase 0 proved the connection-epoch sequence
  scope and every expected successor was observed; a gap invalidates all
  coverage/validity intervals sharing that connection;
- `provider_delivery_guaranteed`: Coinbase explicitly guarantees the channel
  behavior needed for coverage and Phase 0 verified it for this product class;
- `receipt_contiguous`: only local receipt order is known;
- `connection_health_only`: subscription/heartbeat health exists without proof
  of message completeness.

The latter two values never justify an explicit zero-trade bucket in v1.
Heartbeat continuity alone proves connection health, not the absence of trades.
If an inactive channel emits no provider evidence that brackets a bucket and no
verified delivery guarantee covers that silence, the bucket is missing or
incomplete—not zero.

For bucket `[b0,b1)`, `aggregate_complete=true` if and only if all conditions
below hold in the aggregate revision's known-at prefix:

1. one latest coverage-interval revision has the exact product/channel scope,
   opens at or before `b0`, and has a trustworthy closing coverage watermark at
   or after `b1`;
2. its status is valid (open-valid or closed-valid) and its ordering assurance
   is allowed by `market.trade_flow.v1`: `provider_sequence_contiguous`, or
   `provider_delivery_guaranteed` only after that assurance is Phase 0-approved;
3. no sequence, connection, heartbeat, decode, ownership, spool, archive-loss,
   or canonicalization gap intersects the bucket or its bracketing evidence;
4. every raw record from the opening through closing coverage positions has an
   acknowledged manifest-to-record mapping with matching SHA-256;
5. the canonicalization watermark is at or beyond the closing coverage
   position, and every admitted/rejected record in the range is reconciled; and
6. the bucket end has passed. A later provider trade creates a later-known
   aggregate revision and cannot change an earlier decision-time selection.

The closing coverage watermark is the position of a later product/channel
record whose assurance contract covers the preceding silence. A heartbeat may
serve as that record only when the Phase 0-approved
`provider_delivery_guaranteed` specification explicitly proves that the same
heartbeat scope and continuity guarantee trade-channel delivery. Otherwise a
heartbeat is health evidence only and cannot close a silent bucket.

An observed-trade aggregate may be persisted with `complete=false` for
diagnostics/research policies that explicitly allow partial data. A zero-trade
row is emitted only when the same rule returns complete and observed count is
exactly zero. Archive upload acknowledgement or interval closure appends a new
aggregate revision; it never mutates the earlier row.

### Causal Buckets

One-second and one-minute buckets are half-open UTC intervals `[start, end)`.
Each revision contains:

- total and side-specific trade counts;
- contract, base, and quote-notional volume when units are known;
- maker-side volumes always, aggressor-side volumes only when justified;
- first/high/low/last trade price in deterministic trade order;
- `cvd_delta = aggressor_buy_volume - aggressor_sell_volume` in one declared
  unit;
- a cumulative CVD anchor/reset identity rather than an unbounded implicit sum;
- first/last trade source positions, coverage interval/revision, bracketing
  coverage positions, archive/canonicalization watermarks, completeness,
  late-trade count, and known-at.

Operational emission occurs after bucket end with no hidden future grace. A
later trade appends a correction revision known only at its later acceptance.
Datasets choose revisions using both commit watermark and decision-time
known-at, preserving prefix invariance.

### Price Impact And Futures/Spot Alignment

Trade response uses the last valid midpoint at or before the trade and a later
valid midpoint at a declared horizon. A one-second response for a trade at `t`
is only effective/known at `t+1s`; it is never backdated to `t`.

Futures and spot alignment uses the explicit mapping revision and last-known
values independently constrained by a maximum staleness. The paired row's
known-at is the maximum of both input known-at values and computation
acceptance. Missing or invalid either-side evidence produces no numeric pair.

## Feature And Normalization Contracts

### Layer Separation

| Layer | Examples | Retention/authority |
|---|---|---|
| Irreplaceable raw facts | raw frames, provider trades, L2 snapshot/mutations, product definitions, OI/funding polls | raw object and typed source facts |
| Reproducible derived facts | BBO, depth bands, spread, trade buckets, paired basis | recomputable; typed hot rows for operations |
| Versioned normalization specs | rolling windows, percentiles, time-of-day baselines, staleness and missing policies | immutable spec registry |
| Materialized operational features | bounded recent normalized rows | cache-like but append-only and provenance-complete |
| Dataset-frozen values | exact admitted feature rows/objects and fingerprints | immutable dataset authority for research/backtests |

### Global Causal Rules

For a feature effective at `t`, inputs must have `effective_time <= t` and
`known_at <= decision_time`. `feature.known_at` is the maximum input known-at and
computation acceptance. Rolling windows are trailing and exclude future rows.
Later source revisions can append a later-known feature revision but cannot
change a value selected at an earlier decision-time prefix.

No feature silently imputes missing inputs. Each spec chooses `suppress`,
`emit_null_with_reason`, or an explicit causal carry policy with a maximum
staleness. V1 uses suppression for book/flow/relationship features and explicit
null diagnostics for research tables. Warmup is not valid data.

### Deliberately Small V1 Operational Catalog

These are stored continuously after Phases 2-3:

| Feature | Inputs/formula and units | Window/partition/minimum | Missing, validity, known-at, warmup | Version/materialization |
|---|---|---|---|---|
| `bbo` | best bid/ask quantities; `mid=(bid+ask)/2`; native price/quantity | after every valid batch, optionally downsampled to 1s per series | valid book required; suppress otherwise; no rolling warmup | `market.bbo.v1`, continuous |
| `spread_bps` | `10000*(ask-bid)/mid`; bps | 1s per series; one valid state | valid uncrossed book; known after source batch | `market.spread_bps.v1`, continuous |
| `depth_band` | sum quantity and notional on each side within 5, 10, and 25 bps of mid | 1s, series × band; at least one side level; empty valid band is zero | valid book and known units; no carry | `market.depth_band.v1`, continuous |
| `book_imbalance` | `(bid_depth-ask_depth)/(bid_depth+ask_depth)`, bounded [-1,1] | 1s, series × 10 bps; positive denominator | suppress on invalid/empty denominator | `market.book_imbalance.v1`, continuous |
| `trade_flow` | count, quantity, notional, aggressor buy/sell and `CVD_delta=buy-sell` | 1s and 1m per series; complete/zero status uses the exact trade coverage contract | aggressor fields require side transform; gaps/archive-pending state mark the revision incomplete; no warmup | `market.trade_flow.v1`, continuous |
| `cvd_volume_share` | `CVD_delta/(buy+sell)` in [-1,1] | 1m per series; denominator > 0 | suppress if aggressor semantics/gap unavailable | `market.cvd_volume_share.v1`, continuous |
| `basis_bps` | `10000*(future_mid-spot_mid)/spot_mid` | 1s per mapping; both mids no older than 2s | both books valid; suppress stale side; known-at max inputs | `market.futures_spot_basis.v1`, continuous |
| `oi_log_change` | `ln(OI_t/OI_prev)`; log fraction | 1m series; two positive accepted OI observations, no gap between scheduled samples | null/suppress for zero, stale, missing, or gap; one-sample warmup | `market.oi_log_change.v1`, continuous after alignment support |
| `funding_level` | exact provider-reported rate, fractional rate per provider interval | latest causal value per futures series; positive interval required | label remains `provider_reported` until final/predicted semantics proven | `market.funding_level.v1`, continuous after funding delivery |

### Research/Freeze-First Normalizations

These are computed on demand or during dataset preparation until repeated
consumers justify operational materialization.

| Feature | Inputs/formula and units | Window/partition/minimum | Missing, validity, known-at, warmup | Version/materialization |
|---|---|---|---|---|
| `funding_acceleration` | `(rate_t-rate_prev)/elapsed_intervals`; fraction/interval | two consecutive funding observations per series | suppress across gap or semantic-status change | `funding_acceleration.v1`, frozen/on demand |
| `funding_persistence` | signed count of consecutive same-sign, nonzero rates | per series, reset on gap/zero/sign change; min 1 | provider-reported values only unless finalized status proven | `funding_persistence.v1`, frozen/on demand |
| `funding_percentile` | causal empirical rank of current rate among prior values | trailing 30 calendar days per series; min 30 observations | current row is ranked against prior rows only; warmup invalid | `funding_percentile_30d.v1`, frozen/on demand |
| `relative_volume_tod` | current 1m notional divided by median prior complete days for same UTC minute-of-day | prior 28 days, series × UTC minute; min 10 prior observations | excludes current day and incomplete buckets | `relative_volume_tod_28d.v1`, frozen/on demand |
| `vol_adjusted_return` | log midpoint return divided by trailing realized volatility | 1m return; volatility from prior 60 complete 1m returns per series | valid prices; 60-row warmup; no current-return leakage into denominator | `vol_adjusted_return_60m.v1`, frozen/on demand |
| `liquidity_adjusted_impact` | signed response bps divided by pre-trade depth notional in chosen band, scaled per USD million | response horizon 1s; series × 10 bps band; valid pre-trade book | effective/known at response horizon; suppress gaps | `liquidity_adjusted_impact_1s.v1`, frozen/on demand |
| `price_response_per_flow` | interval return bps divided by signed aggressive notional in USD millions | 1s/1m per series; nonzero flow and valid boundary mids | requires aggressor proof and valid books; effective at interval end | `price_response_per_flow.v1`, frozen/on demand |
| `aggressive_flow_share` | aggressor buy or sell notional divided by total notional | 1s/1m; denominator > 0 | requires aggressor proof and complete trade bucket | `aggressive_flow_share.v1`, frozen/on demand |
| `depth_replenishment` | direction-specific restoration of consumed depth: aggressive buys measure ask depth; aggressive sells measure bid depth; formula below | 1s horizon, 10 bps band; positive measured depletion | pre/trough/post states must be in one valid book interval and exact trade/book positions are stored; known at post position | `depth_replenishment_1s.v1`, frozen/on demand |
| `absorption_primitive` | `abs(flow_share) * (1-clamp(abs(response_bps)/R,0,1)) * clamp(replenishment,0,1)` | 1s; fixed `R` in spec; all three inputs valid | descriptive bounded [0,1], no label/alpha claim; known at horizon | `absorption_primitive.v1`, frozen only |
| `exhaustion_primitive` | `max(0,1-|flow_t|/median(|flow| prior 60s)) * max(0,|return_t|/V)` clipped [0,1] | 1s; prior 60 complete seconds; fixed `V`; min 30 | valid trade and price evidence; 30s warmup | `exhaustion_primitive.v1`, frozen only |

Percentiles, medians, and rolling volatility must use a deterministic numeric
algorithm named by the normalization spec. Changing interpolation, window
closure, reset, staleness, or unit semantics requires a new semantic version.

`depth_replenishment_1s.v1` emits two independent directional values; mixed
aggression is never netted into one ambiguous side:

- aggressive buys select ask-band depth;
- aggressive sells select bid-band depth.

For each direction, store `first_trade_source_position`,
`last_trade_source_position`, `pre_book_source_position`,
`trough_book_source_position`, and `post_book_source_position`. `D_pre` is the
last valid selected-side 10 bps depth state known before the first qualifying
trade. `D_trough` is the minimum selected-side depth at valid states after that
trade through the one-second response horizon. `D_post` is the first valid
state at or after the horizon. All three must belong to one book-validity
interval and satisfy the spec's maximum staleness.

```text
consumed_depth = max(D_pre - D_trough, 0)
replenished_depth = max(D_post - D_trough, 0)
depth_replenishment = replenished_depth / consumed_depth
```

The value is nonnegative and may exceed one when more depth appears than was
observably consumed. It is suppressed when `consumed_depth=0`, any position is
missing/invalid, or trade coverage is incomplete. The absorption primitive
uses `clamp(depth_replenishment,0,1)` and preserves the unclipped source feature.

## Bounded Storage And Retention Architecture

### Storage Roles

| Storage | Allowed use | Forbidden use |
|---|---|---|
| PostgreSQL/Timescale hot transactional store | control/fencing, immutable manifests, typed canonical recent facts, operational derived features, quality, dataset manifests | exact raw-frame archive, unbounded book history, a second provider cache |
| Object storage | immutable raw frames, compacted typed archives/checkpoints, frozen dataset objects | leases, mutable coordination, unacknowledged sole copy |
| Local block storage | fsynced spool/WAL, sealed upload staging, bounded replay cache | long-term archive or canonical query authority |
| Frozen research dataset | immutable selected typed values/specs/fingerprints | live mutable feature reads or provider access |
| Temporary replay materialization | disposable isolated validation output | source truth or dataset identity |

### Raw Object Contract

Raw files use Parquet with ZSTD and schema `coinbase_ws_raw_frame.v1`:

```text
stream_definition_id UUID
session_id UUID
connection_epoch INT64
spool_segment_id UUID
segment_record_ordinal INT64
raw_record_id UUID
receive_ordinal INT64
received_at TIMESTAMPTZ
channel_hint STRING nullable
product_ids ARRAY<STRING>
raw_frame BINARY
raw_frame_sha256 FIXED_BINARY(32)
```

`raw_frame` is the exact complete WebSocket text frame bytes. `raw_record_id`
must recompute from the recorded identity fields and `raw_frame_sha256`.
Record order is `receive_ordinal`; replay never relies on Parquet physical row
order without sorting/checking that ordinal. The object path is:

```text
market-structure/v1/provider=coinbase/venue=coinbase_direct/
definition=<uuid>/channel=<channel>/date=YYYY-MM-DD/hour=HH/
session=<uuid>/epoch=<n>/part=<first>-<last>-<sha256>.parquet
```

One connection is scoped to one futures/spot pair and one high-volume channel
plus heartbeats, following Coinbase's recommendation to spread high-volume
products. This keeps raw partitions bounded without duplicating frames.

Rotate at the first of 128 MiB uncompressed, five minutes, disconnect, or
shutdown. Compact acknowledged small objects by definition/hour into 256-512
MiB compressed objects without changing the ordered-record content
fingerprint. Compaction appends a replacement manifest relation; it never
silently changes dataset references. Object checksum covers stored bytes;
content fingerprint covers ordered raw record hashes and survives compaction.

Archives use server-side encryption, least-privilege write/read roles, object
versioning or equivalent overwrite denial, and retention/pin policy. Only
inbound market-data frames are archived; outbound subscriptions, JWTs, provider
secrets, and credential-store material are forbidden from raw objects and
manifests.

### Local Spool, Acknowledgement, And Recovery

1. Create `spool_segment_id` in the segment header. For each frame, compute its
   raw SHA-256 and deterministic `raw_record_id`, then append a length-delimited
   record containing both IDs, segment/receive ordinals, receipt timestamp, and
   raw bytes to `.partial`; fsync according to the measured policy, never later
   than one second or 4 MiB.
2. Only after append acknowledgement may the parser publish the frame to the
   canonicalization queue.
3. Rotate, fsync, checksum, and atomically rename to `.sealed`.
4. Upload with an idempotent object key containing bounds/checksum.
5. Verify object size and checksum, then transactionally insert the archive
   manifest/ranges and append one `raw_archive_record_mappings` row per object
   record. Only this commit makes those records archive-complete.
6. Delete the sealed local object only after PostgreSQL manifest commit and a
   successful object HEAD/checksum check.
7. On crash, scan partial/sealed segments. Truncate a partial segment only to
   its last checksum-valid record, emit recovery evidence, preserve ordinals,
   and resume upload. Never invent missing records.

### Backpressure And Degradation

Phase 0 starts with a per-host spool cap of `min(50 GiB, six hours of measured
p99 compressed input)` and reserves 20% disk for non-spool operation. Final
values are configuration, not hard-coded behavior.

- 70%: warn and accelerate upload/compaction; record backlog metrics.
- 85%: stop nonessential replay/compaction reads and new pair admission.
- 95%: deliberately close the affected stream, seal/archive what is present,
  end book validity with `spool_capacity`, and retry only after backlog drains
  below 70%.

Frames are never dropped while the session remains marked valid. Trades and L2
are not sampled to disguise overload. A deliberate disconnect and visible gap
is preferable to incomplete data labeled complete.

### Retention Tiers

| Material | Hot PostgreSQL | Object/archive | Frozen behavior |
|---|---|---|---|
| session/quality/manifests/product/mappings | indefinite | raw refs as below | included by fingerprint/reference |
| raw trades | canonical rows 180d | 400d default | referenced partitions copied/pinned for dataset lifetime |
| raw L2 snapshots/mutations | typed rows 7d | 90d initial default | selected raw partitions copied/pinned only when a dataset requires raw replay |
| book checkpoints | metadata indefinite; projection current | 90d or matching source L2 retention | referenced checkpoint copied/pinned |
| BBO/depth/trade aggregates/basis | 400d | compact typed Parquet after hot expiry when approved | exact rows/objects frozen for dataset lifetime |
| normalized operational features | 400d | optional compact typed archive | exact spec + values frozen |
| frozen dataset objects/manifests | manifest indefinite | retained until explicit dataset retirement | never removed by ordinary compaction |
| temporary replay output/cache | none/campaign schema only | local block cache <= 7d | never referenced by a dataset |

Retention deletion is an explicit job over acknowledged, unpinned objects and
expired typed partitions. It writes deletion evidence. A manifest remains and
reports `object_retention_state=expired`; a missing expired object cannot appear
replayable. No frozen dataset result may depend on an object eligible for normal
retention deletion.

### Storage Budget And Measurement

Do not approve three-pair L2 rollout from estimates alone. Measure a continuous
24-hour BIP/BTC capture including a high-volume period and record:

- messages, trades, L2 mutations, and raw bytes per second at p50/p95/p99/max;
- encoded canonical bytes per row and index amplification from actual tables;
- raw and typed Parquet compression ratio;
- checkpoint bytes, build CPU, full replay rows/sec, and checkpoint replay
  rows/sec;
- upload latency, maximum local backlog, reconnect/resnapshot count, and gap
  rate.

On 2026-08-02 the operator accepted the completed public and existing-CDP
one-hour proofs as sufficient to begin Phase 1–4 implementation and explicitly
deferred this 24-hour measurement until after Phase 4. This is an implementation
sequencing decision, not a production-capacity waiver. No production collector
may be enrolled before the measurement and budget approval below pass against
the implemented archive, hot-store, replay, and feature paths.

Annualized formulas use measured rates:

```text
raw_archive_GiB_year = compressed_raw_bytes_second * 31,536,000 / 2^30
hot_fact_GiB = rows_second * bytes_per_row_with_indexes * hot_seconds / 2^30
checkpoint_GiB_year = checkpoints_day * bytes_checkpoint * 365 / 2^30
derived_GiB_year = sum(feature_rows_second * bytes_feature) * 31,536,000 / 2^30
peak_spool_GiB = p99_input_bytes_second * tolerated_outage_seconds / 2^30
```

For each next pair, use its own 24-hour measurement or the larger of its
observed rate and the existing-pair p99. Admission requires:

- projected hot/object totals below an explicitly approved monthly and annual
  byte/cost budget;
- replay of one day in less than one hour on the intended worker class;
- spool capacity for the configured outage at 3x observed p99 input;
- no database write-latency breach or unbounded small-file growth;
- no silent quality loss.

## Pipeline Topology And Operational Ownership

See [market-structure-data-plane.mmd](diagrams/market-structure-data-plane.mmd)
for the component diagram.

The pipeline is at-least-once between durable boundaries with idempotent
effects. It does not claim distributed exactly-once delivery.

| Component | Input/output contract | Delivery/idempotency/retry | Ownership/recovery/failure evidence | Scaling unit and authority |
|---|---|---|---|---|
| Stream supervisor | enabled stream definition -> fenced session lifecycle | claim definition, increment generation, bounded reconnect; session event ordinal is idempotent | existing PostgreSQL lease pattern; expired owner cannot publish; startup recovers spool before reconnect | one definition; authority is definition + lease state |
| Advanced Trade acquisition | exact WS frame -> stable raw record/spool segment identity + receive ordinal + spool record | durable-spool-first at-least-once; subscription within provider deadline; retry under ADR 0020 | one connection epoch per owner; disconnect, stale heartbeat, decode failure and counters are session events | one pair/channel connection; authority for receipt order |
| Local durable spool | frame record -> partial/sealed segment | length/checksum framing, fsync, deterministic ordinal; retry upload indefinitely within backlog budget | current fenced session may append; crash scan/truncate only invalid tail; capacity disconnect visible | per host/definition; temporary authority until upload ack |
| Raw archive uploader | sealed segment -> immutable object + manifest | content-addressed object key; repeat PUT/HEAD safe; manifest insert after checksum verification | upload ownership follows segment/session; partial upload never acknowledged; attempt failures logged | per sealed segment; object bytes authoritative raw evidence |
| Deterministic parser/canonicalizer | acknowledged/spooled frame -> typed trade/L2/product facts | at-least-once consumption, raw/source keys dedupe; schema-versioned parser; malformed input fails that event visibly | fence checked again in append transaction; replay may republish identical facts; parser error quality event | product/channel partition; typed store authoritative query truth |
| Trade aggregator | ordered canonical trade revisions -> 1s/1m aggregate revisions | source-position watermarks; duplicate invariant; late input appends later-known revision | lease by output series/window; restart from last committed watermark; incomplete bucket/gap flags | series × interval; aggregate rows authoritative derived truth |
| Book reconstructor | snapshot/mutation facts + product definitions -> validity, state hashes, BBO/depth | state machine above; exact duplicate no-op; invalid evidence closes interval | fenced per L2 series; restart from checkpoint/archive; never publishes from invalid state | one product book; archive + reconstruction version authoritative |
| Checkpoint generator | valid deterministic state -> checkpoint object/manifest | source-position content identity; repeat generation yields same state/content fingerprint | same book fence or read-only replay job; checksum mismatch invalidates checkpoint | series/checkpoint; checkpoint is acceleration, not independent truth |
| Relationship joiner | explicit mapping + futures/spot facts -> paired relationship revisions | causal as-of join with staleness and input fingerprints; retries idempotent | lease per mapping/spec; gap/invalid side suppresses output and records diagnostic | mapping × spec; typed relationship rows authoritative |
| Feature materializer | typed source/derived rows + normalization spec -> normalized revisions | input fingerprint dedupe; bounded trailing recomputation after late revisions; append only | lease by output series/spec/range; restart from input watermark; invalid/warmup evidence preserved | series × spec × time range; spec + inputs authoritative |
| Historical reconciler | admitted public file/reference + live facts -> reconciliation evidence | source object checksum and comparison spec identity; retry read/parse safely | scheduled collector ownership/fencing; auth/access failures propagate; never mutates source fact | source/product/day; reconciliation table authoritative comparison |
| Dataset planner/freezer | requirements/range/as-of -> admitted series, specs, archive refs, immutable objects/fingerprints | prepare may acquire only through producer workflow; freeze transaction is idempotent on dataset identity | existing dataset service ownership; failure leaves no partially admitted dataset; object pin/copy acknowledged before manifest commit | one dataset; dataset manifest authoritative for execution |
| Replay service/job | archive/checkpoint + parser/reconstruction/spec versions -> isolated replay outputs/report | read-only source; output namespace includes replay request hash; safe retries | fenced async job; checksum/schema/version mismatch fails loud; temporary output expires | definition/product/range; never becomes truth without canonical admission/freeze |
| Retention/compaction | acknowledged unpinned objects/expired hot partitions -> replacement or deletion evidence | content fingerprint invariance; compaction idempotent; deletion requires eligibility recheck | single fenced job per partition; partial compaction leaves old object active | partition/hour; manifests/pins authoritative eligibility |
| Health/quality monitor | sessions, manifests, backlog, gaps, validity, lag -> structured health | read-only projection; bounded alerts/logs; never repairs silently | no source ownership; alerts include provider/venue/product/session/series | definition/product; evidence tables remain authority |

### Service Boundaries

- Acquisition and raw archiving form one failure domain: a frame is not eligible
  for canonical publication until locally durable. Object upload may lag.
- A canonical fact parsed from the fsynced pending spool carries a deterministic
  raw record identity even before its object manifest exists. Archive-complete
  and dataset-eligible status require an acknowledged manifest-to-record
  mapping whose checksum covers that record. If the only pending spool copy is
  lost, `archive_loss` quality/gap evidence makes that source range ineligible;
  no derived or frozen surface may report it complete.
- Canonical parsing and derivation are replayable consumers. They may run in the
  acquisition process initially, but their contracts and checkpoints cannot
  depend on in-process-only state.
- Book reconstruction is one writer per L2 series under a generation fence.
  Replay jobs write only to isolated temporary namespaces unless an explicit
  canonical backfill campaign is later approved.
- A historical source is admitted through a typed provider handler and normal
  provenance/quality contract. A downloaded CSV is not canonical merely
  because it exists.
- Runtime/paper consumers read canonical facts through
  `RuntimeMarketDataResolver` and engine snapshots. They do not subscribe to
  the archive service or inspect `book_reconstruction_state`.
- Canonical research/backtests resolve only frozen datasets. Paper/live runtime
  may resolve persisted hot facts causally after acceptance, as it does for OI
  today; it never calls a provider on read and it fingerprints the exact facts
  delivered into engine snapshots.

## Causal Replay, Normalization, And Freeze

### Deterministic Raw Replay

1. Resolve immutable manifests by definition/product/channel/range and verify
   object checksum plus ordered content fingerprint.
2. Sort records by session/epoch/receive ordinal; verify no duplicate ordinal,
   unexplained ordinal hole, or manifest-range mismatch.
3. Parse with the exact parser contract version stored in the replay request.
4. Reproduce canonical fact material hashes and quality events. Operational
   timestamps may be excluded only if the hash contract explicitly excludes
   them; replay known-at uses original receipt/acceptance evidence, not replay
   wall time.
5. Reconstruct each book with the exact product definitions and reconstruction
   version effective at the original known-at prefix.
6. Materialize derived facts/specs in an isolated namespace and compare hashes,
   counts, gaps, and state transitions to persisted outputs.

### Dataset Freeze Algorithm

1. Resolve all typed requirements, instrument-role mappings, normalization
   specs, range, and as-of fact commit watermark.
2. Fail if a required direct Coinbase spot instrument/mapping is absent, a
   required source range is unavailable, trade coverage is incomplete, book
   validity does not satisfy policy, aggressor semantics are unproven, any raw
   record lacks an acknowledged manifest-to-record mapping, or a feature is
   still in warmup.
3. Select source/derived revisions using effective time, `known_at`, revision,
   and commit watermark. No provider calls are allowed in freeze.
4. Compute separate material, provenance, and quality fingerprints using the
   current dataset rules. Add normalization spec hashes and source archive
   content fingerprints without collapsing the three identities.
5. Write exact typed values to immutable dataset Parquet/ZSTD objects when the
   existing row manifest would otherwise depend on expiring hot storage.
6. Copy/pin only raw/checkpoint objects explicitly required for future raw
   replay. Operational backtests need frozen typed values, not the raw feed.
7. Verify all object checksums and pins, then commit dataset series/archive/spec
   references in one manifest transaction.
8. Execution resolves only the dataset. Missing frozen material fails before
   engine initialization.

## Determinism And Correctness Proof Plan

Every later phase must add fixtures made from captured, sanitized exact frames.
No test substitutes imagined Coinbase fields for a proof-spike fixture.

| Proof | Test procedure | Required result |
|---|---|---|
| Duplicate-delivery invariance | replay each raw frame/fact twice and in retry-sized duplicate batches | identical canonical rows, state hashes, features, quality classifications, and dataset fingerprint |
| Restart/reconnect invariance | stop after every possible spool, upload, parse, batch, checkpoint, and feature commit boundary | no missing/extra admitted material; reconnect closes validity and requires snapshot; same final hashes |
| Full replay equality | rebuild typed facts/features from acknowledged raw objects in an empty isolated namespace | material/provenance/quality hashes and row counts equal persisted outputs |
| Checkpoint-plus-delta equality | reconstruct sampled endpoints both from initial snapshot and latest prior checkpoint | identical sorted levels, state hash, BBO/depth, validity, and source position |
| Truncation/no-lookahead invariance | compute on prefix `0..t`, then on `0..T`; compare values selectable at `t` | all earlier known-at selections identical; response features appear only at their response horizon |
| Stable archive fingerprint | upload, retry, and compact the same ordered records | object checksums may differ after compaction, ordered content fingerprint and canonical results do not |
| Stable raw identity and mapping | assign identities before parse, publish from fsynced spool, then upload/compact | canonical facts retain the same `raw_record_id`; only append-only manifest mappings change; facts are never updated to attach an object manifest |
| Stable dataset fingerprint | freeze same admitted inputs twice on separate workers | identical dataset identity, material/provenance/quality/spec fingerprints and object content hashes |
| Late-data causality | insert a trade/revision known after original bucket/decision | later-known aggregate/feature revision appears; earlier decision-time selection unchanged |
| Trade coverage discrimination | exercise a truly silent proven stream, dropped message, unhealthy connection, pending upload, and lagging canonicalizer | only the proven, ordered, archive-complete and canonicalized silent interval emits `complete=true` with zero trades; all other cases remain incomplete with typed evidence |
| Gap/invalidation propagation | remove one sequence/frame, inject heartbeat gap, disconnect, corrupt quantity | explicit session quality + generic gap evidence; validity closes; downstream book features suppressed |
| No invalid-book emission | feed updates after invalidation and before fresh snapshot | no BBO/depth/imbalance/basis using that book; diagnostics remain queryable |
| Raw-to-derived reconciliation | recompute counts/volumes/notional and source-position ranges from raw trades | exact equality or typed, explainable rejected-record counts |
| Maker/aggressor translation | replay documented BUY/SELL fixtures and unknown side | exact inversion for proven products; unknown fails/suppresses aggressive features |
| Quantity/multiplier correctness | reconcile futures provider size, product multiplier, base quantity and notional against proof fixtures | exact Decimal equality and correct product-definition revision reference |
| Futures/spot alignment | vary event/known-at order and staleness independently | pair known-at is max inputs; stale/invalid side suppresses output; explicit mapping only |
| Provider-free backtest | disable network/provider registry and execute frozen dataset | execution succeeds with zero provider call; missing component fails before initialize |
| Persisted-feature agreement | recompute feature through dataset path and compare runtime-visible snapshot value | exact spec/input/value fingerprint equality |
| Retention/compaction safety | compact/delete only eligible source objects, then rerun a pinned dataset | frozen results unchanged; pinned object cannot be deleted; expired unpinned object reports unreplayable |
| Partial archive upload | terminate during multipart/upload/HEAD/manifest stages | no manifest points at incomplete bytes; sealed spool survives and retry is idempotent |
| Consumer failure | crash parser/book/feature worker after source durable but before/after DB commit | restart catches up with idempotent effects and visible lag; acquisition remains bounded |
| Fence safety | expire owner and let new generation start while old worker attempts append | old append/manifest/session completion rejected transactionally |

Phase 2 is not accepted unless the L2 property suite also generates random
valid absolute mutation sequences, injected duplicates/gaps, and checkpoint
cuts and proves the state-machine invariants above.

## Research Integration And Falsifiable Evidence

These are observation studies, not strategy or alpha claims.

| Hypothesis | Required causal evidence | Supporting observation | Falsifying/limiting observation |
|---|---|---|---|
| Spot-confirmed breakouts differ from futures-only breakouts | synchronized frozen future/spot returns, basis, flow, depth, candles, existing breakout event | outcome distribution differs when both venues confirm within declared causal window | no stable difference across time splits, or result vanishes after staleness/gap controls |
| OI distinguishes new-position expansion from covering/unwinding | price return, OI change, volume/flow, funding, known-at aligned | price up/OI up differs from price up/OI down; price down/OI down consistent with deleveraging | OI sampling latency/gaps dominate or quadrant labels do not produce repeatable conditional differences |
| Failed aggression/absorption is observable | proven aggressor flow, valid pre/post book, price response, replenishment | extreme flow with small response and replenishing opposing depth recurs before bounded outcomes | side semantics invalid, book invalid, or pattern is indistinguishable from normal low-volatility flow |
| Funding/OI crowding changes breakout behavior | provider-semantic funding, OI level/change, causal percentiles, breakout events | extreme trailing crowding state has a stable conditional outcome distribution | effect disappears under prior-only normalization or semantic status separation |
| Liquidity fragility changes price impact | spread, band depth, imbalance, valid book, signed flow, later response | low depth/wide spread predicts larger contemporaneous response per flow | response is explained by gaps/stale books or no monotonic relation exists |
| Deleveraging phases are distinguishable | negative return, falling OI, flow/volume, widening spread, depth loss, funding | joint state is more coherent than any single input and repeats across episodes | OI timing cannot locate the phase or results depend on future-aligned OI |
| Predicted-funding evolution is informative | a source explicitly proven predicted, its revisions, finalized rate reconciliation | acceleration/persistence before finalization relates to later state without backdating | Advanced Trade field is current/final rather than predicted, or revisions cannot be causally timestamped |
| Existing breakout performance is state-dependent | existing exact strategy runs plus frozen state features | prespecified state buckets show repeatable out-of-sample conditional metrics | no repeatable difference, excessive missing data, or feature/version sensitivity dominates |

No study may infer individual long/short positions from aggregate OI, call a
block trade central-book aggression, or promote a strategy from descriptive
conditional results.

## Phased Implementation Backlog

Each phase is independently reviewable and useful without later phases.

### Phase 0: Provider Proof Spikes And Measured Capacity

Dependencies: existing Coinbase provider access and BIP catalog entry only.

Work:

- capture one hour unauthenticated and existing-CDP BIP/BTC market trades, L2,
  ticker, and heartbeats, including deliberate reconnects;
- prove schemas, product acceptance, subscription/auth behavior, sequence scope
  and reset, snapshot/update order, duplicate behavior, batching, and errors;
- prove trade/L2 size units and multiplier conversions for BIP; spot-check ETP
  and SLP;
- test public REST product/book/recent-trade behavior with no auth and existing
  CDP credentials;
- inspect public CDE historical/funding downloads for stable unauthenticated
  URL, schema, publication/revision semantics and checksum identity;
- record the one-hour provisional capacity envelope and the operator decision
  deferring the 24-hour production measurement until after Phase 4;
- produce sanitized exact-message fixtures plus a canonical proof report and
  detached SHA-256 checksum. The repository has no configured artifact-signing
  identity, so Phase 0 must not describe a checksum as a cryptographic
  signature.

Acceptance:

- every field used by Phase 1 has captured evidence and documented semantics;
- maker-side translation and quantity units are proven or aggressive/unit-derived
  outputs remain explicitly disabled;
- sequence assurance is classified honestly and reconnect always re-snapshots;
- the one-hour provisional annualized archive, spool, checkpoint, and replay
  envelope is recorded and the operator explicitly authorizes implementation;
- production enrollment remains blocked on the deferred post-Phase 4 24-hour
  capacity measurement and budget approval;
- CDE public history is either admitted through a stable contract or marked
  unsupported; no ambiguous source remains in the Phase 1 dependency graph.

Value without later phases: verified provider contract, capacity envelope, and
fixtures that prevent speculative implementation.

### Phase 1: Futures And Matching Spot Trades

Status: implemented and live-verified for bounded BIP/BTC on 2026-08-02. See
[Market Structure Phase 1 Trades](MARKET_STRUCTURE_PHASE_1_TRADES.md). ETP/ETH
and SLP/SOL remain unenrolled, and no stream is production-admitted.

Dependencies: Phase 0 trade/auth/unit proof and operator-authorized provisional
capacity for implementation. This dependency does not authorize production
collector enrollment.

Work:

- register direct Coinbase BTC-USD, ETH-USD, SOL-USD instruments and explicit
  mapping revisions;
- add stream session/spool identity, raw archive manifests and record mappings,
  typed trade coverage intervals, and typed trade revisions;
- run BIP/BTC first, then gated ETP/ETH and SLP/SOL admission;
- add 1s/1m trade aggregates, maker/aggressor contract, recent REST
  reconciliation, quality/health operations, replay, retention, and dataset
  freezing for raw trades/aggregates;
- do not add L2 or book features.

Acceptance:

- durable-spool-first crash/retry/fence tests pass, and no canonical fact is
  mutated when its manifest-to-record mapping is appended;
- bounded raw-to-canonical trade ID/count/volume reconciliation passes within
  only explicitly rejected malformed records;
- duplicate/restart/late-trade/provider-free dataset tests pass, including
  deterministic distinction among zero trades, stream gaps, unhealthy
  connections, pending archive upload, and canonicalization lag;
- capacity metrics and production blockers are exposed for every configured
  pair; no collector is production-enrolled yet;
- `qt` exposes definitions, sessions, archive lag, gaps, replay verification,
  and dataset coverage without direct SQL.

Value without later phases: durable event-level paired futures/spot trade
research and deterministic trade-flow backtests.

### Phase 2: Raw Level 2 Archive And Validity/Reconstruction

Dependencies: Phase 1 archive/session foundation and Phase 0 L2 proof.

Implementation status: completed for bounded BIP/BTC on 2026-08-02. See
[Market Structure Phase 2 Level 2](MARKET_STRUCTURE_PHASE_2_LEVEL2.md). The
post-Phase-4 24-hour production-admission gate remains unchanged.

Work:

- add typed snapshot/mutation tables, deterministic state machine, validity
  intervals, checkpoints, replay, gap/resync evidence, and retention;
- capture BIP/BTC first and admit additional pairs only through the same gate;
- expose BBO only as reconstruction validation, not research feature catalog.

Acceptance:

- property tests, checkpoint/full replay equality, invalidation suppression,
  reconnect/resnapshot, spool pressure, and bounded replay benchmarks pass;
- no interval is marked stronger than observed sequence assurance;
- every invalid source condition creates durable evidence and no derived state;
- projected storage is visible against the provisional envelope; production
  enrollment still requires the post-Phase 4 measured L2 budget.

Value without later phases: auditable, replayable L2 evidence and valid BBO
history.

### Phase 3: One-Second/One-Minute Market-State Features

Status: implemented for bounded BIP/BTC on 2026-08-02. See
[Market Structure Phase 3 State Features](MARKET_STRUCTURE_PHASE_3_STATE_FEATURES.md).

Dependencies: valid Phase 2 books and Phase 1 trades.

Work:

- materialize the small v1 BBO/spread/depth/imbalance/trade-flow/CVD/basis
  catalog;
- add OI/funding causal alignment and reconciliation evidence;
- expose typed coverage/quality in the operational read boundary. Phase 4
  subsequently registered these facts for exact freezing and frozen runtime
  resolution.

Acceptance:

- raw-to-feature and persisted/recomputed agreement pass;
- gap and book-validity policies suppress all contaminated rows;
- output rate/storage remain bounded and spec/version fingerprints are stable;
- repeated materialization is a no-op at the same bounded input watermark and
  replayed features equal persisted features. Provider-free frozen delivery was
  subsequently accepted in Phase 4.

Value without later phases: queryable causal market-state history and deterministic
operational features.

### Phase 4: Normalization And Frozen-Dataset Integration

Status: implemented and accepted for bounded BIP evidence on 2026-08-02. See
[Market Structure Phase 4 Normalization And Frozen Datasets](MARKET_STRUCTURE_PHASE_4_NORMALIZATION_DATASETS.md).

Dependencies: Phase 3 typed facts and existing frozen dataset boundary.

Work:

- implement immutable normalization specs and research/freeze-first features;
- replace fixed candle/OI dataset conditionals with a tested typed fact registry;
- freeze exact feature values, input/spec/archive references, warmup, gaps, and
  fingerprints; complete funding dataset/runtime delivery where required.

Acceptance:

- truncation invariance, late-revision causality, double-freeze equality,
  provider-free execution, compaction/retention safety, and backtest/persisted
  feature agreement pass;
- no operational feature table is read without a dataset during canonical
  backtest execution.

Value without later phases: reusable causal feature datasets for any bounded
research question.

### Post-Phase 4 Production Admission Gate

This gate is required before any market-structure collector is production
enrolled. It is not part of the Phase 1–4 implementation sequence.

- run a continuous 24-hour BIP/BTC capture on the implemented durable spool,
  object archive, canonical trade/L2, checkpoint, and feature paths;
- prove raw-to-canonical reconciliation, one-day full/checkpoint replay under
  one hour, reconnect/resnapshot, gap propagation, and no silent quality loss;
- measure actual raw/typed compression, hot-table/index amplification, object
  upload latency, maximum local backlog, checkpoint cost, and derived growth;
- set the configured outage spool at no less than the approved 3x observed-p99
  requirement and prove bounded degradation thresholds;
- obtain explicit operator monthly/annual byte and cost budget approval in an
  immutable admission artifact referencing the report checksum;
- only then enroll BIP/BTC, followed by separately budgeted ETP/ETH and SLP/SOL.

### Phase 5: Observation Studies And Existing-Strategy Filters

Dependencies: approved Phase 4 datasets; no live-trading authorization.

Work:

- implement the falsifiable studies above using existing research/check/report
  workflows;
- prespecify windows, cohorts, missing policies, time splits, and falsifiers;
- test state-dependent filters only as research variants of the existing
  breakout strategy.

Acceptance:

- every result names dataset/spec/material/provenance/quality fingerprints and
  caveats;
- repeated runs are deterministic and no study uses future-normalized inputs;
- null/negative results are retained; no strategy is promoted or live behavior
  changed.

Value: evidence about market structure and bounded strategy context, not an
alpha or deployment claim.

## Explicit Unknowns, Limitations, And Proof Decisions

### Phase 0 Proof Decisions

| Question | Decision state |
|---|---|
| BIP/BTC public `market_trades`, L2, ticker, heartbeat and REST access | passed one-hour public and existing-CDP proofs; authenticated v2 evidence passes the v3 implementation-readiness gate |
| ETP/ETH and SLP/SOL access/units | passed bounded public access/unit/reconnect/replay spot checks; eligible for implementation configuration but production-unenrolled |
| `sequence_num` scope | resolved as one connection-epoch counter shared across all received channels; reset to zero on reconnect; not per product/channel |
| first event after subscription/reconnect | BIP and BTC L2 began with a complete `snapshot` in both public proof epochs; v1 keeps one product per connection and does not depend on multi-product frames |
| BIP futures trade/L2 size and multiplier | confirmed as contracts, 0.01 BTC/contract, from published specification, live metadata, and observed integral trade/L2 quantities |
| public REST product/book/recent trades | admitted for bounded BIP/BTC proof/reconciliation; recent trades are not complete history |
| REST book as WebSocket recovery splice | rejected: observed REST book has provider time but no compatible WebSocket sequence position |
| Advanced Trade product funding meaning | admitted only as provider-reported observation known on acceptance; API does not prove projected/finalized semantics, so those features remain disabled |
| public CDE historical/funding machine source | rejected: human page has no stable documented data endpoint; direct historical funding REST returns 401 without CDE credentials |
| actual p99/max, compression, replay, backlog, annual budget | one-hour diagnostics measured; 24-hour implemented-path capture and explicit operator budget approval deferred until after Phase 4 and mandatory before production enrollment |

### Explicit Coverage Gaps

- direct native CDE order/trade feed, retransmission, order IDs, queue position,
  order counts, implied orders, and Participant Firm data;
- native order-level L3 and lossless cross-connection continuity;
- market-wide permission-free CDE REST block-trade feed;
- liquidation events and individual-position direction;
- complete Advanced Trade historical event trades or L2 backfill;
- authenticated CDE historical funding under DCC credentials;
- historical OI/funding publication known-at unless a public file proves it;
- provider trade bust/correction semantics unless Coinbase exposes them;
- predicted-funding features until the source meaning is proven.

An unsupported fact is represented in coverage and requirements as unsupported;
it is never fabricated, inferred from a nearby field, or advertised as a
Quant-Trad feature.

## Repository Impact Map For A Later Campaign

No path below is changed by this design campaign except documentation. These are
the likely implementation sites.

| Area | Likely paths/artifacts | Later change |
|---|---|---|
| Stream contracts/adapters | `src/data_providers/streams/contracts.py`, `src/data_providers/streams/coinbase.py`, `src/data_providers/streams/__init__.py` | raw receive callback/identity, market-trade/L2 parsing, connection-epoch sequence evidence, authenticated/public subscribe support |
| Provider semantics | `src/data_providers/facts.py`, `src/data_providers/providers/coinbase.py`, `src/data_providers/registry.py` | typed product/trade unit validation and implemented feature declarations only |
| Core market contracts | `src/market_data/contracts.py`, `src/market_data/requirements.py`, `src/market_data/store.py`, proposed `src/market_data/archive/`, `src/market_data/book/`, `src/market_data/features/` | typed models, source positions, archive manifests, reconstruction/spec/fingerprint contracts |
| Database models/bootstrap | `portal/backend/db/market_data_models.py`, `portal/backend/db/session.py` | proposed tables, shared fact clock, hypertables, immutability, strict drift checks |
| Manual migrations | proposed `scripts/db/manual_migration_market_structure_v1.sql` and later phase-specific migrations | out-of-band clean definitions only; no runtime alter/backfill |
| Repositories | `portal/backend/service/storage/repos/market_data.py`, `market_collection.py`, proposed market archive/book repositories | fenced append, range reads, manifest/pin, validity/checkpoint/spec/dataset refs |
| Services | `portal/backend/service/market/collector_service.py`, `runtime_market_data.py`, `backtest_dataset_service.py`, proposed archive/replay/book/feature services | continuous session lifecycle, replay, registry-driven dataset planning and causal delivery |
| Workers | `portal/backend/workers/market_data_collector.py`, proposed stream/archive/replay/feature/retention workers | independent fenced components described above |
| Paper/runtime | `portal/backend/service/bots/paper_market_stream.py`, `src/engines/bot_runtime/live_market.py`, runtime snapshot contracts | consume canonical facts; do not own archival collection or mutable book state |
| Controllers/API | `portal/backend/controller/market_data.py`, API schemas | definitions/sessions/archive lag/quality/replay/dataset inspection; no raw secret/object mutation |
| CLI/MCP | `cli/main.py`, `cli/mcp_server.py`, `cli/api_client.py` | `qt data streams`, `archives`, `quality`, `replay verify`, `features`, and extended dataset coverage/freeze operations |
| Configuration | `config/defaults.yaml`, `src/core/settings.py`, deployment env docs | allowlist, spool path/cap, rotation, object prefix, retention, reconnect, checkpoint and capacity gates; no second DSN |
| Tests/fixtures | `tests/test_data_providers`, `tests/test_market_data`, `tests/test_portal`, `tests/integration/runtime`, proposed sanitized raw fixtures | proof fixtures and correctness matrix |
| Architecture/operations docs | `DATA_BOUNDARY.md`, this design, ADR 0053, developer audit workflow, provider guide, runbooks | accepted boundaries, operating procedures, measured budgets, supported coverage |

The likely CLI operations are intentionally operator-oriented:

```text
qt data streams list|show|enable|disable|sessions
qt data archives coverage|lag|verify
qt data quality gaps|book-validity
qt data replay verify --definition ... --start ... --end ...
qt data features coverage|specs
qt data prepare-backtest-dataset ...
qt data freeze-dataset ...
```

The frontend may later visualize these read models, but it is not part of this
campaign and never becomes the mutation or workflow authority.

## Definition Of Ready For Phase 0 And Phase 1

An implementation agent may execute Phase 0 without choosing a new provider,
credential model, raw format, storage role, identity, time model, product scope,
or validity philosophy. Phase 0 exists to fill the explicitly named empirical
fields, not to redesign the plane.

After Phase 0 acceptance, Phase 1 has fixed boundaries for durable-spool-first
delivery, pre-parse raw identity, append-only manifest mapping, session fencing,
trade coverage, trade identity, maker/aggressor translation, unit conversion,
typed storage, late revisions, aggregation, replay, retention, dataset freezing,
CLI ownership, and the three-pair gated allowlist. Any proof that contradicts
those boundaries must amend this proposed design/ADR before implementation
rather than hiding the contradiction in provider code.
