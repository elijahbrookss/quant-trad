---
component: adr-tiered-market-structure-archive-replay
subsystem: data
layer: decision
doc_type: adr
status: proposed
tags:
  - adr
  - market-data
  - market-structure
  - object-storage
  - replay
  - feature-materialization
  - datasets
  - proposed
code_paths:
  - src/data_providers/streams
  - src/market_data
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/workers
  - cli/main.py
  - config/defaults.yaml
  - scripts/db
  - docs/architecture/data/MARKET_STRUCTURE_DATA_PLANE.md
---
# ADR 0053: Use A Tiered Market-Structure Archive And Replay Boundary

## Status

Proposed on 2026-08-02. No runtime, schema, object-store, or collector behavior
implements this decision yet. Phase 0 provider and capacity proof is required
before acceptance.

## Context

Quant-Trad's current canonical market store is append-only, typed, causal, and
dataset-bound. Its polling collectors already provide ownership fencing,
retries, pacing, provenance, known-at timing, gap evidence, and one shared fact
commit order. Paper streaming is bot-owned and candle-specific: it parses
heartbeat/ticker/candle messages but does not durably preserve exact raw frames.

Event trades and L2 are different from minute polls. Raw frames are
high-volume, irreplaceable evidence; L2 requires stateful reconstruction and
explicit invalid intervals; derived BBO, depth, flow, and normalized features
are reproducible; backtests require immutable, provider-free inputs. Keeping all
raw frames forever in PostgreSQL would make the transactional store the wrong
storage tier. Keeping only derived rows would make parser, reconstruction, and
feature errors unauditable.

The design must also distinguish object storage from block storage. Local disks
can buffer and recover in-flight bytes but are not a durable archive. Object
storage can retain immutable evidence but cannot own leases or hot causal
queries.

Canonical facts may be published once their exact source record is safely
fsynced, while object upload and manifest acknowledgement lag. Therefore a
manifest identifier cannot be the canonical fact's immutable raw identity.
Trade aggregation also needs an absence-proof contract separate from book
validity: a quiet bucket is not equivalent to a healthy, ordered, fully
archived and canonicalized stream interval with zero trades.

## Decision

Market-structure ingestion will use four explicit layers:

1. **Local durable spool on block storage.** A fenced WebSocket session assigns
   stable `spool_segment_id` and `raw_record_id` values plus receive ordinals,
   then fsyncs exact frames before canonical publication. The spool is bounded,
   recoverable staging, not long-term truth.
2. **Immutable raw archive in object storage.** Sealed Parquet/ZSTD objects
   preserve exact frame bytes and ordered content fingerprints. PostgreSQL
   manifests and append-only manifest-to-record mappings are inserted only
   after upload checksum verification. Canonical facts retain `raw_record_id`;
   they are never mutated later to attach a manifest identifier.
3. **Typed hot canonical/derived facts in PostgreSQL/Timescale.** Trades, L2
   snapshots/mutations, book validity, trade stream coverage, BBO, depth,
   aggregates, relationships, quality, and feature revisions use typed tables,
   append-only revisions, the shared fact clock, provenance, and known-at
   semantics. Mutable book state is a disposable projection.
4. **Immutable frozen datasets.** A dataset binds exact fact revisions,
   normalization specifications, quality, material/provenance fingerprints, and
   any pinned archive/checkpoint objects. Canonical backtests read only this
   layer.

Delivery between durable boundaries is at-least-once with deterministic
idempotency. The architecture does not claim distributed exactly-once delivery.
Raw object bytes are authoritative evidence of receipt; typed PostgreSQL rows
are authoritative query facts; parser/reconstruction/spec versions are
authoritative transformations; frozen dataset manifests are authoritative
execution inputs.

Raw and derived retention are independent and bounded. L2 raw evidence has a
shorter default retention than low-volume manifests and frozen datasets.
Dataset freeze copies or pins only the objects needed for its declared replay
contract. Ordinary compaction or retention cannot change a frozen dataset.

## Invariants

- Exact frames are locally durable before their canonical facts are published.
- Stable raw record and spool segment identities are assigned before parsing;
  immutable canonical facts reference `raw_record_id`, not a future manifest.
- A raw archive manifest never points to an unverified or partial object.
- A source record is archive-complete and dataset-eligible only after an
  acknowledged manifest-to-record mapping covers it. Later upload or compaction
  appends mappings and never mutates the canonical fact.
- Local block storage is never treated as archive authority.
- Object storage is never used for leases, mutable coordination, or hot query
  truth.
- PostgreSQL remains the only relational persistence boundary and uses
  `PG_DSN`; object-store configuration is not another DSN.
- Canonical query surfaces are typed. Raw provider frames may remain native,
  but one generic JSON table cannot replace trade, L2, validity, or feature
  contracts.
- Parser, reconstruction, normalization, and archive schema versions are
  fingerprint inputs.
- Book-derived facts are emitted only from an explicitly valid interval and
  name the source state hash.
- A complete trade aggregate, including an explicit zero-trade bucket, requires
  a typed trade stream coverage interval proving the exact product/channel
  scope, trusted ordering assurance, no intersecting gaps, archive-complete raw
  evidence through the closing position, and a canonicalization watermark past
  that position. Connection health or heartbeat evidence alone is insufficient.
- A disconnect, sequence gap, invalid state, or backlog termination remains
  visible quality evidence and cannot be represented as complete data.
- Compaction preserves ordered content fingerprints. Replacement/deletion is
  manifest-driven and cannot invalidate a dataset pin.
- Frozen backtests use no provider, raw live stream, mutable book projection, or
  operational feature table.
- Credentials stay at the provider boundary. Stream definitions declare auth
  mode but contain neither secrets nor credential references.

## Consequences

The platform gains deterministic forensic replay and can correct parser or
feature implementations without pretending the corrected result was known
earlier. High-volume raw retention does not force PostgreSQL to become an
unbounded blob store. Dataset consumers remain independent from providers and
operational retention.

The design adds operational complexity: local spool capacity, upload
acknowledgement, object manifests, compaction, pins, replay versions, and two
independent retention policies. A Phase 0 measurement is mandatory because L2
cost and throughput cannot be safely inferred from candle/OI rates.

Raw replay after default retention is possible only for ranges pinned/copied by
a dataset or explicit retention hold. Manifests remain, but they must report an
expired object as unreplayable rather than silently falling back to derived
facts.

## Rejected Alternatives

- Store every raw frame indefinitely in PostgreSQL/Timescale.
- Store only BBO/features and discard source frames or L2 mutations.
- Use local block storage as the long-term archive.
- Let each paper/live bot own its own market-structure connection and book.
- Publish canonical facts before the raw frame is durably spooled.
- Make canonical fact identity depend on a manifest that does not exist at
  publication time, or mutate facts later to attach that manifest.
- Infer zero trades from an open connection or heartbeat without typed trade
  coverage, archive-completeness, and canonicalization evidence.
- Claim exactly-once delivery instead of at-least-once plus deterministic
  idempotency.
- Materialize every proposed research normalization continuously.
- Let backtests read hot operational features or reconstruct from providers.
- Use direct CDE FIX/UDP as a hidden recovery or enrichment source.

## Evidence Required Before Acceptance

- BIP/BTC Advanced Trade `market_trades` and `level2` proof captures establish
  product/auth/schema/sequence/reconnect/unit behavior.
- A 24-hour capture measures rates, compression, index amplification, spool
  backlog, checkpoint size, and replay speed.
- duplicate, crash, partial upload, stale-fence, reconnect, checkpoint/full
  replay, gap propagation, and truncation-invariance tests pass.
- canonical publication before upload preserves stable raw identity; later
  upload and compaction append verified mappings without updating source facts.
- trade coverage tests distinguish proven zero activity from message loss,
  unhealthy connection, pending archive upload, and canonicalization lag.
- object content fingerprints survive idempotent upload and compaction.
- a frozen dataset executes provider-free and produces the same results after
  eligible hot/archive retention and compaction.
- unsupported public CDE history is either admitted through a stable verified
  unauthenticated file contract or remains outside the data plane.

## References

- [Market Structure Data Plane](../data/MARKET_STRUCTURE_DATA_PLANE.md)
- [Data Boundary](../data/DATA_BOUNDARY.md)
- [Persistence Boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [ADR 0050: Canonical Market Store](0050-use-one-canonical-append-only-market-data-store.md)
- [ADR 0051: Frozen Datasets](0051-require-frozen-datasets-for-canonical-backtests.md)
- [ADR 0052: Typed Collectors And Instrument Roles](0052-use-typed-fact-collectors-and-explicit-instrument-roles.md)
- [ADR 0044: Known-At Prefix Invariance](0044-enforce-known-at-prefix-invariance.md)
