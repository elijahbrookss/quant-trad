---
component: adr-tiered-market-structure-archive-replay
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - market-data
  - market-structure
  - object-storage
  - replay
  - feature-materialization
  - datasets
  - accepted
code_paths:
  - src/data_providers/streams
  - src/core/market_storage_lifecycle.py
  - src/market_data
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market
  - portal/backend/service/market/market_storage_lifecycle.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/market_lifecycle.py
  - portal/backend/service/storage/repos/fact_references.py
  - portal/backend/workers
  - cli/main.py
  - docker/docker-compose.yml
  - config/defaults.yaml
  - scripts/db
  - scripts/db/manual_enable_market_storage_lifecycle_v1.sql
  - docs/architecture/data/MARKET_STRUCTURE_DATA_PLANE.md
---
# ADR 0053: Use A Tiered Market-Structure Archive And Replay Boundary

## Status

Accepted for implementation on 2026-08-02 after public and existing-CDP
one-hour BIP/BTC proofs plus bounded ETP/ETH and SLP/SOL access/unit spot checks.
No production collector is authorized by this status. The operator deferred the
24-hour implemented-path capacity measurement and explicit budget approval
until after Phase 4; both remain mandatory before production enrollment.

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

## Implementation Status

Phase 1 implemented layers 1–4 for bounded Coinbase BIP/BTC trades on
2026-08-02. The implementation uses stable pre-parse raw identities, fsynced
definition-scoped spool segments, verified immutable Parquet/ZSTD objects,
transactional manifests and record mappings, typed trade/coverage/flow
revisions, and archive-pinned provider-free datasets.

The final live proof reconciled every replayed raw frame and unique provider
trade ID for both BIP and BTC, returned both definition-scoped spools to zero,
and froze complete one-second flow ranges with both source objects pinned. See
[Market Structure Phase 1 Trades](../data/MARKET_STRUCTURE_PHASE_1_TRADES.md).

This implementation chose the stricter permitted publication sequence:
canonical facts wait for verified object acknowledgement and committed raw
record mappings. They still reference only stable `raw_record_id` values; no
fact is mutated to attach a manifest.

Phase 2 implemented this same boundary for bounded BIP/BTC Level 2 on
2026-08-02. Typed snapshots, absolute mutations, validity revisions,
deterministic checkpoints, full/checkpoint replay, quality linkage, immutable
compaction lineage, and append-only explicit retention pins now exist. Replay
selects a compacted replacement only after its manifest, mappings, and lineage
commit atomically; source objects are not deleted by compaction. See
[Market Structure Phase 2 Level 2](../data/MARKET_STRUCTURE_PHASE_2_LEVEL2.md).

Phase 3 implemented typed one-second BBO/spread/depth/imbalance, one-second and
one-minute flow/CVD projections, futures/spot basis, OI/funding derivative
state, and direction-specific response facts. All are append-only, use the
shared commit clock, and name exact typed source material. Session replay now
replays the persisted transport-quality timeline at its original source
position, so a heartbeat/sequence invalidation and post-invalid feature
suppression reproduce exactly. Cross-stream materialization fixes an input-only
commit watermark; its own outputs cannot advance its source identity. See
[Market Structure Phase 3 State Features](../data/MARKET_STRUCTURE_PHASE_3_STATE_FEATURES.md).

Phase 4 implemented immutable normalization specifications, append-only causal
normalized revisions, one typed fact registry for planning/freezing/runtime,
transitive source/archive/spec dataset references, physical archive checksum
verification, and provider-free frozen delivery. The one-hour BIP proof froze
the exact source and aggressive-flow normalization twice to the same dataset
identity. See [Market Structure Phase 4 Normalization And Frozen Datasets](../data/MARKET_STRUCTURE_PHASE_4_NORMALIZATION_DATASETS.md).

The Phase 3 scrub also proved that the original filesystem archive path was
inside the replaceable backend container. Pre-correction objects were lost on
recreation and remain loudly unreplayable; their database manifests are not
treated as sufficient evidence or dataset-eligible bytes. The local stack now
mounts a dedicated named volume at `/app/logs/market-structure` in both backend
and collector services. A new BIP archive replayed with an identical fingerprint
after backend replacement. This is durable local object storage for the current
deployment boundary, not a claim of cloud object-store durability.

The generic supervisor and continuous Coinbase trade adapter now rotate and
finalize archive segments without closing acquisition, reconnect by explicit
epoch, and recover orphaned WAL segments under fresh fencing before a new
session. Recovery closes prior coverage at the last proven event and never
bridges downtime. The later single-node admission registered Level 2 through
the same supervisor/runtime boundary; its projection adapter owns checkpoint
restore, delta replay, exact-state reconciliation, and post-gap invalidation.

On 2026-08-05 the generic storage-lifecycle implementation added scheduled,
lease-independent raw archive compaction; checksum-verified, pin-safe object
expiration; Timescale chunk compression and controlled expiration; immutable
lifecycle evidence; and dry-run-first API/`qt` controls. Frozen dataset creation
and explicit archive pinning share the lifecycle fence, and replay reports an
expired object explicitly instead of treating a retained manifest as retained
bytes. Terminal reconnect epochs now retire their projection state after final
canonicalization, bounding in-process state by active/finalizing epochs.

Destructive lifecycle execution remains configuration-gated and defaults off.
Timescale compression activation is an explicit maintenance operation through
`scripts/db/manual_enable_market_storage_lifecycle_v1.sql`; the script refuses
to run while a stream lease is active. Quant-Trad intentionally installs no
native Timescale retention policy because that path cannot enforce frozen
dataset pins.

The current cold tier covers exact raw provider frames and book checkpoints.
It does not yet archive typed derived rows to Parquet. Derived-table hot expiry
therefore must remain disabled or use a retention window that matches the
deployment's rehydration requirements until a typed cold archive contract is
implemented.

No definition is production-admitted by this implementation status. Phase 4 is
complete; the 24-hour canonical implemented-path evidence and authoritative
physical/cloud resource budget remain production-readiness gates. That proof is
not a 24-hour process lifetime: admitted production has no duration cap.

## Consequences

### Canonical Tier Retention Addendum

The generalized canonical tier keeps immutable revision headers in PostgreSQL,
verified immutable Parquet payloads on the configured archive filesystem, and
explicit daily hot-payload progress. The current implementation and rollout
gates are defined in
[Generalized Fact Data Plane](../data/GENERALIZED_FACT_DATA_PLANE.md#canonical-retention-planning);
the historical family-table compression script above is not its cutover.

Raw retention windows do not override canonical source lifetime. Hot backlog
protects source objects until cold publication creates permanent dependency
holds. Expiration takes an object-specific manifest row lock through final
admission/unlink/completion; canonical writers take a conflicting shared row
lock and recheck expiry before committing new references. This closes the
check-versus-publication race without serializing all ingestion behind a global
archive fence. New reference writes use READ COMMITTED, and identical existing
canonical rows remain no-ops. Current stream publication remains archive-first;
this does not introduce a spool-first publication lane.

This safety choice can retain raw evidence beyond an ordinary age window, and
the permanent canonical index still grows on NVMe. Budget pressure must report
that cost rather than expire unarchived evidence or silently disable collection.
The canonical bounded executor is now wired through the existing lifecycle
service, outside its raw exclusive fence, with a separate default-off execution
gate and durable page/verification resume state. Complete dependency closure and
representative production validation remain activation requirements; the
existence of backlog protection or orchestration alone does not complete them.

### Operational Consequences

The platform gains deterministic forensic replay and can correct parser or
feature implementations without pretending the corrected result was known
earlier. High-volume raw retention does not force PostgreSQL to become an
unbounded blob store. Dataset consumers remain independent from providers and
operational retention.

The design adds operational complexity: local spool capacity, upload
acknowledgement, object manifests, compaction, pins, replay versions, worker
supervision, restart recovery, and two independent retention policies. The
Phase 0 one-hour measurements provide a
provisional implementation envelope because L2 cost and throughput cannot be
safely inferred from candle/OI rates. A 24-hour measurement on the implemented
path remains mandatory before production enrollment, and Docker Desktop/WSL
guest capacity cannot substitute for physical backing-volume evidence.

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

## Evidence Required Before Production Enrollment

- BIP/BTC Advanced Trade `market_trades` and `level2` proof captures establish
  product/auth/schema/sequence/reconnect/unit behavior. This evidence passed.
- bounded ETP/ETH and SLP/SOL spot checks establish access, futures units,
  reconnect snapshots, and deterministic replay. This evidence passed.
- after Phase 4, a 24-hour implemented-path capture measures rates,
  compression, index amplification, spool backlog, object upload, checkpoint
  size, and replay speed, followed by explicit operator budget approval.
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
