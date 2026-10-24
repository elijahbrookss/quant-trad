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
# ADR 0053: Use a Tiered Market-Structure Archive and Replay Boundary

Accepted for implementation August 2, 2026. This records the durable storage and
replay choice. Later canonical storage uses the generalized Fact representation;
the original family-table topology is historical. Current retention and operator
admission belong to [the Fact data plane](../data/GENERALIZED_FACT_DATA_PLANE.md)
and [storage management](../persistence/STORAGE_MANAGEMENT.md).

<a id="decision"></a>
## Keep receipt evidence separate from query facts

High-volume provider frames, typed query facts and frozen execution inputs have
different responsibilities. Storing every frame forever in PostgreSQL would
burden the transactional boundary; keeping only derived rows would make parser
and reconstruction errors unauditable.

Use a bounded, fsynced local spool for recoverable in-flight frames and immutable
checksum-verified archive objects for retained raw evidence. Assign stable raw
record and segment identities before parsing. Canonical Facts reference that
identity, not a future upload manifest. Append verified manifest mappings rather
than rewriting accepted Facts when upload or compaction completes.

Typed PostgreSQL Facts own causal queries and the shared commit order.
Reconstructed book state is a disposable projection. Frozen Datasets bind exact
revisions, schemas, quality, normalization and required archive/checkpoint objects.
Consumers replay admitted local evidence without provider access.

Delivery between durable boundaries is at-least-once with deterministic
idempotency. This design does not promise distributed exactly-once delivery.

<a id="invariants"></a>
## Publication and reconstruction need explicit evidence

Local durability is the minimum before canonical publication. The implemented
stream path uses the stricter archive-first sequence: verified object
acknowledgement and committed raw mappings precede Fact publication.

A healthy connection or heartbeat does not prove a complete quiet interval.
Zero-trade aggregates require typed coverage, trusted ordering, no intersecting
gaps, archive-complete closing evidence and sufficient canonicalization progress.
Book-derived Facts require explicitly valid state, exact source identity and
reconstruction provenance. Disconnects, sequence gaps and invalid intervals
remain quality evidence.

Replay must verify bytes, versions, checkpoints and ordered source material.
The early implementation exposed archive files inside a replaceable container;
lost objects remain unavailable rather than becoming valid because their
database manifests survived. A persistent local archive fixes that lifecycle
error but does not imply cloud or off-host durability.

## Retention preserves dependency closure

Raw and canonical retention are independent. A frozen reference or canonical
dependency can retain raw evidence beyond its ordinary age window.
Compaction publishes verified replacement lineage before readers may select it;
expiration must not invalidate retained Datasets or canonical sources.

Current hot-to-cold archival preserves every output revision and the complete
required causal input closure. Where historical formulas omitted exact input
identities or processing bounds, retain the validated conservative candidate
set rather than inventing a smaller original selection. Preserve historical
values, fingerprints, corrections, invalidations and quality flags without
claiming to recalculate or recertify the old conclusion.

Versioned verification receipts bind the proof they actually performed.
A stronger lineage rule requires explicit additive reverification; an old
receipt cannot silently become stronger deletion permission. Final destructive
admission still requires fresh bytes, placement, expiry, pins and source proof
under its owning fences. Unknown families remain unadmitted.

The consequence is additional metadata, archive reads and sometimes a larger
retained causal superset. Report that cost and refuse bounded work that cannot
complete; do not release dependencies to conceal pressure.

## Operational tradeoffs

This design adds spool budgeting, upload acknowledgement, manifests, pins,
replay versions, recovery and dependency-safe expiry. It keeps those concerns
with their owners instead of making consumers interpret provider transport.

Disposable tests establish their exercised reconstruction, correction, frozen
identity, corruption and interruption behavior. Production enrollment and
destructive retention require exact-source results, measured capacity and
explicit operator admission. Fixture success and nominal deadlines do not
establish physical workload throughput or a particular installation's readiness.

Related decisions: [canonical Facts](0063-use-schema-registered-canonical-facts.md),
[frozen backtests](0051-require-frozen-datasets-for-canonical-backtests.md),
[replay-certified execution tapes](0058-use-replay-certified-execution-book-tapes.md).
