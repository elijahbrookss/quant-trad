---
component: market-structure-phase2-level2
subsystem: data
layer: operations
doc_type: architecture
status: active
tags:
  - market-data
  - market-structure
  - level2
  - order-book
  - raw-archive
  - replay
  - compaction
  - retention
  - operations
code_paths:
  - src/market_data/archive.py
  - src/market_data/book_archive.py
  - src/market_data/order_book.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market/market_structure_service.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - tests/test_market_data/test_market_structure_archive.py
  - tests/test_market_data/test_order_book_phase2.py
  - tests/test_market_data/test_market_structure_repository_db.py
  - tests/test_portal/test_market_structure_routes.py
  - tests/test_cli/test_market_data_cli.py
---
# Market Structure Phase 2 Level 2

## Status And Boundary

Phase 2 is implemented and accepted for bounded BIP/BTC Level 2 capture,
deterministic reconstruction, archive replay, checkpoint replay, compaction,
and retention-pin validation. It is not production admission. All definitions
remain disabled and `production_admitted=false`; the implemented-path 24-hour
capacity run and explicit storage/cost approval remain a post-Phase-4 gate.

Acquisition uses Coinbase Advanced Trade through the existing provider and CDP
credential boundary. Stream definitions contain auth mode but no secrets or
credential references. Only the BIP/BTC pair completed this implemented-path
gate; ETP/ETH and SLP/SOL remain unenrolled.

## Implemented Path

```text
Advanced Trade level2 + heartbeats
  -> one product-scoped fenced connection/session
  -> stable raw_record_id + spool_segment_id
  -> fsynced bounded local WAL
  -> verified immutable Parquet/ZSTD raw object + mappings
  -> typed complete snapshot / atomic absolute-quantity mutation batches
  -> explicit valid / invalid / resynchronized book intervals
  -> deterministic Parquet/ZSTD checkpoints
  -> disposable current reconstruction projection
  -> provider-free full and checkpoint-plus-delta replay
```

Canonical book publication uses the stricter archive sequence already selected
by Phase 1: every source record must have an acknowledged manifest mapping.
Checkpoint metadata likewise requires acknowledged source manifests and a
verified immutable checkpoint object.

## Reconstruction And Validity Contract

- A complete Coinbase `snapshot` is the only event that opens validity.
- Updates are ordered absolute quantities. All mutations inside one provider
  event apply atomically; a zero quantity deletes a level.
- A zero delete for an unknown level is retained as typed, non-invalidating
  `unknown_zero_delete` evidence. It is not silently discarded.
- An update before a snapshot emits evidence and cannot produce state.
- Exact provider-position/material redelivery is a no-op even when local raw
  record and receipt ordinals differ. Divergent reuse of a provider position
  invalidates the interval.
- Connection-sequence gaps, disconnect evidence, invalid quantities, or a
  crossed/locked post-batch state close the interval and clear the mutable
  state. Updates remain suppressed until a fresh complete snapshot opens a new
  interval, including after a new connection epoch.
- Bounded clean shutdown appends a closed-valid interval revision. It does not
  imply continuity beyond the last accepted provider position.
- State hashes include reconstruction version, product definition and units,
  observed ordering assurance, exact source position, and sorted Decimal
  levels. A repeated valid replay reproduces the same facts and hashes.

Advanced Trade documents guaranteed Level 2 delivery, while the implementation
also observes its connection-wide sequence. The persisted ordering assurance is
`provider_delivery_guaranteed`; it does not claim native CDE retransmission,
order-level L3, or lossless continuity across connections.

## Typed Storage

The hot store adds append-only snapshot headers and levels, mutation batch
headers and ordered mutations, validity interval revisions, checkpoint
manifests, quality links, compaction lineage, and retention pin revisions. The
snapshot and mutation headers are Timescale hypertables using the shared
`market.fact_commit_seq`. The current reconstruction row is explicitly mutable
and disposable.

The installed TimescaleDB version rejects ordinary-table foreign keys that
reference hypertables. Snapshot-level and mutation-child ownership is therefore
enforced by typed composite parent identities and one atomic fenced repository
transaction, not a database FK. The real-Postgres test verifies parent/child
counts and retry invariance. This is an explicit storage capability boundary,
not an implied relationship.

The 44,555-level sanitized BTC snapshot exposed unacceptable per-row database
round trips during the scrub. Snapshot levels and mutation rows now enter
PostgreSQL through one typed `jsonb_to_recordset` statement per provider event;
logical row grain and immutability are unchanged.

## Checkpoints, Replay, Compaction, And Retention

Checkpoints are deterministic typed Parquet/ZSTD objects containing sorted bid
then ask levels, exact quantity strings, and explicit provider units. An initial
accepted snapshot always checkpoints; later checkpoints occur at five minutes
or 100,000 mutations. They accelerate replay and never replace raw authority.

Full session replay reads only active acknowledged objects, verifies object
SHA-256, reconstructs typed facts, reconciles their IDs with PostgreSQL, then
verifies checkpoint object bytes and applies only later deltas. No provider is
called.

Compaction requires an explicit set of at least two contiguous active manifests
from one definition, source session, and connection epoch. It verifies every
source checksum, writes a new immutable object, proves the exact ordered raw
identity/hash sequence, commits replacement mappings and append-only lineage in
one fenced transaction, and leaves source objects untouched. Replay excludes a
source only after its replacement lineage commits. An upload or consumer
failure therefore leaves the prior active set authoritative and retryable.

`market.archive_retention_pin_versions` records explicit pin and release
revisions for raw manifests or checkpoints. Frozen
`market.dataset_archive_refs` remain independent immutable pins. Retention
status combines both sources and refuses ordinary-retention eligibility while
either is active. Phase 2 does not expose object deletion; destructive expiry
remains a later explicit retention job and may never remove a pinned object.

## Operator Surface

```bash
qt data market-structure capture ms_coinbase_l2_bip_20dec30_cde --duration 60
qt data market-structure capture ms_coinbase_l2_btc_usd --duration 60
qt data market-structure replay-book <definition_id> <session_id>
qt data market-structure compact <definition_id> <session_id> \
  --manifest-id <first> --manifest-id <second>
qt data market-structure retention-pin raw_manifest <manifest_id> \
  --owner-kind operator --owner-id <id> --reason <reason>
qt data market-structure retention-pin raw_manifest <manifest_id> \
  --owner-kind operator --owner-id <id> --reason <reason> --release
qt data market-structure retention-status raw_manifest <manifest_id>
```

The same operations have typed API routes. Compaction and replay are
provider-free. Status exposes archive mapping lag, validity revisions, quality
counts, checkpoints, current disposable state, configured capacity, and the
unchanged production blockers.

## 2026-08-02 Live Implemented-Path Proof

The accepted runs were authenticated 10-second BIP and BTC captures after two
earlier scrub runs exposed and fixed a status-query bind cast and a missing
Phase 2 quality classification boundary. Those failed sessions remain durable
failure evidence; only the later completed sessions are acceptance evidence.

| Measure | BIP futures | BTC spot |
|---|---:|---:|
| Session | `mss_6f29a1638eb14d31bb89a73db79f3c26` | `mss_d4ded11ade1847a0bee71f3ab6fec0a3` |
| Raw frames / raw frame bytes | 58 / 181,496 | 223 / 5,304,274 |
| Raw archive bytes | 21,283 | 413,433 |
| Snapshot levels | 1,413 | 44,612 |
| Mutation batches / mutations | 44 / 290 | 209 / 3,181 |
| Checkpoint bytes | 13,084 | 564,931 |
| Invalidating quality | 0 | 0 |
| Non-invalidating unknown zero deletes | 0 | 44 |
| Raw mapping lag / spool backlog | 0 / 0 | 0 / 0 |
| Validity closed cleanly | yes | yes |
| Checkpoint-plus-delta equals full replay | yes | yes |

Exact accepted evidence:

| Product | Raw manifest / object SHA-256 | Checkpoint / object SHA-256 | Final state / replay fingerprint |
|---|---|---|---|
| BIP | `ram_0b79ffd7fb1f1e746ab598d15ed76e0cc5f046244f2f24983a538e43fa3355b6` / `dde4d36bc7026eb8c67abebed03e78df1d7bd4341fcca8a3b4c49728456e83c9` | `bcp_d3a2c3cc76654f5d6feba892b972778f8aa94df2e41c68ee9aac402c016d1ed6` / `9bf6a37bc8a60a0e37e3d2459e6d440fac342210ea9f80ee16711a73417ad217` | `1061de56b17603c7c6c517b29fd75cba1c0509fb24c91a122404760870a613b2` / `c4763fd2258c0b936c6f4c2593ac3bffd26fb8879355a92923880c5c72f2681a` |
| BTC | `ram_37b72c32dc05cc082dd912977f0fe549c8a94bf0ef7e8a292cc444400da3cdb7` / `435096105de54178c016c6c13b298dbbe8f26de161937bb073e9169dafc40d6f` | `bcp_0c61e6d064dabd6210d42d8b4eba85d41e01fbb1e00912630de24da973d959c8` / `ea3d46376c5a8f557f77b9d93b2ccda015a3340d5ae6f7206a8f35ed20ff45ee` | `6f540a53d368c4977803a7ab40bd2ebab4caa6df1d327ec421930e0e8c8bc55f` / `566c880e16797b280ceb2021767277463298798a9e9adc97ae4826432757708e` |

This bounded proof is sufficient to begin Phase 3. It is not a sustained
capacity result and does not replace the post-Phase-4 24-hour gate.

## Verification And Remaining Work

Phase 2 tests prove deterministic randomized replay, exact duplicate and
divergent duplicate behavior, gap invalidation, reconnect/resnapshot, invalid
state suppression, real sanitized BIP/BTC snapshots, deterministic checkpoint
bytes, full/checkpoint replay equality, crash-tail recovery, partial upload,
backpressure, compaction identity, compaction failure safety, archive-before-
canonical publication, bulk typed persistence, quality linkage, stale fencing,
dataset pins, explicit pin/release eligibility, and a terminal invalid book
being unable to report a clean bounded close.

Phase 3 features, Phase 4 normalization/dataset delivery, production retention
deletion, continuous workers, other pairs, and the 24-hour admission run remain
outside this phase.
