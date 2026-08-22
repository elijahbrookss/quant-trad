---
component: continuous-collector-runtime
subsystem: data
layer: service
doc_type: architecture
status: active
tags:
  - market-data
  - collectors
  - continuous
  - supervision
  - recovery
  - capacity
  - retention
  - timescaledb
code_paths:
  - src/core/settings.py
  - src/data_providers/structured_facts.py
  - src/market_data/instrument_enrollment.py
  - src/data_providers/providers/chainlink.py
  - src/data_providers/streams/runtime.py
  - src/core/market_storage_lifecycle.py
  - src/market_data/archive.py
  - portal/backend/service/market/collector_supervisor.py
  - portal/backend/service/market/collector_safety.py
  - portal/backend/service/market/collector_service.py
  - portal/backend/service/market/continuous_stream_collector.py
  - portal/backend/service/market/continuous_stream_runtime.py
  - portal/backend/service/market/market_storage_lifecycle.py
  - portal/backend/service/market/market_structure_service.py
  - portal/backend/service/storage/repos/market_lifecycle.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/workers/market_data_collector.py
  - portal/backend/workers/market_data_collector_health.py
  - portal/backend/workers/single_node_initializer.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - scripts/reporting/docker_capacity_sampler.sh
  - scripts/reporting/host_capacity_sampler.ps1
  - scripts/db/manual_enable_market_storage_lifecycle_v1.sql
  - config/defaults.yaml
  - docker/grafana/provisioning/dashboards/capacity-database-growth.json
  - docker/grafana/provisioning/alerting/collector-safety.yml
  - docker/docker-compose.yml
  - docker/docker-compose.server.yml
  - config/market_data/coinbase_perpetual_instruments.v1.json
  - scripts/automation/server_deploy.sh
---
# Continuous Collector Runtime

## Purpose

The continuous collector runtime owns provider streams that must remain active
indefinitely. It is a generic supervised worker boundary, not a trade-specific
daemon. Provider/domain implementations register adapters against explicit
stream definitions; unsupported or ambiguous definitions fail loudly without
stopping healthy collectors.

The scheduled open-interest, funding, and manifest-bound structured Fact
collectors use the same worker process and operator lifecycle, with the
scheduler's bounded attempt/lease contract rather than a socket session. The first
structured adapter polls a Chainlink MVR reserve bundle and emits canonical
`asset.reserve_state.v1`; it is not a separate Chainlink research subsystem.
The first continuous adapters implement Coinbase `market_trades` plus
`heartbeats` and Coinbase `level2` plus `heartbeats`. Future trade, book, news,
or alternate-provider collectors register a transport adapter and a projection
adapter rather than adding provider or channel switches to the supervisor,
runtime, or definition repository.

Scheduled structured definitions are installed disabled by default. Their
manifest pins schema, subject, dimensions, provider mapping, endpoint
environment reference, cadence, staleness, and adapter configuration. The
normal scheduler owns restart, fencing, retries, attempt logs, and gap evidence.
Repeated latest-state reads are idempotent; changed provider reports append new
canonical observations and never overwrite history.

The runtime has no default provider, analyzer, or projection. A transport
adapter owns connection, authentication, subscription, parser, and
observed-channel binding. A projection adapter creates its epoch analyzer and
owns canonical translation, domain state, recovery, and projection-specific
counters. Registration must resolve exactly one supervisor
adapter composing those two capabilities. L2 is therefore a normal continuous
collector whose book projection additionally owns reconstruction validity and
checkpoint evidence. Per-epoch projection state and analysis are opaque to the
runtime; the finalizer returns them only to the adapter that created them.

Authentication is definition material, not a property of the collector
lifecycle. Coinbase documents `market_trades`, `level2`, and `heartbeats` as
public channels, so the single-node manifests enroll them with public auth mode
and the transport sends no JWT. A separately reviewed enrollment may select
`auth_mode: authenticated`; only that branch resolves credentials and signs
each subscription. The locked Coinbase SDK supports the CDP-recommended
Ed25519 format as well as legacy ECDSA, and provider-specific onboarding proves
local JWT signing before saving a downloaded key file.

The full server composition runs TimescaleDB, the backend control plane, both
operator frontends, this collector worker, database administration, and the
Grafana/Loki/Alloy observability surface. It uses commit-tagged application
images, private database networking, loopback-only host publication, durable
NVMe PostgreSQL, an explicit host archive mount, health-gated startup, and a
five-minute collector drain window. IBKR Gateway remains profile-gated because
broker credentials and trading surfaces require separate admission. The
deployment helper promotes an exact reviewed Git commit and retains prior
commit-tagged images for compatible rollback; it does not make the agent or Git
checkout a workload supervisor.

## Runtime Contract

One enabled definition maps to one supervisor task and one fenced stream claim.
The generic policy in `src/data_providers/streams/runtime.py` bounds:

- segment age and byte size;
- in-flight finalization segments;
- lease and heartbeat cadence;
- reconnect backoff, stale-heartbeat detection, and continuous-disconnect
  budget.

Acquisition writes every provider frame to a fsynced local WAL before parsing.
It rotates sealed segments while the provider socket remains active and sends
them through a bounded asynchronous finalization queue. Archive upload, manifest
mapping, canonical trade publication, coverage revision, and completed-bucket
materialization occur off the acquisition loop. A full queue or spool limit is
a visible hard failure; memory or disk backlog cannot grow without bound.

Reconnect creates a new connection epoch on the same logical session. The
disconnect budget resets only after a provider message arrives, not after a
successful socket handshake. Sequence, subscription, heartbeat, snapshot, and
coverage evidence remain epoch-scoped.

Projection-quality rejection is distinct from transport failure. For example,
Coinbase can emit a mixed trade update whose maker sides include
`UNKNOWN_ORDER_SIDE`. The trade projection archives the exact frame, admits
the BUY/SELL siblings, folds the rejected trades into one typed quality event,
invalidates the affected flow-coverage interval, and acknowledges the segment.
It never guesses a side, drops the raw record, or terminates the generic stream
runtime on that known provider sentinel. Retained-spool recovery applies the
same rule idempotently, so one semantically unusable trade cannot become a
permanent restart loop.

After a terminal segment is archived, mapped, canonicalized, and its terminal
coverage revision is committed, the finalizer retires that connection epoch's
projection state. Memory is therefore bounded by active/finalizing epochs, not
the lifetime reconnect count.

## Stop, Restart, And Recovery

A normal stop closes the provider connection, seals the final segment, drains
all archive/database finalizers, records terminal evidence, and only then
releases the fencing lease.

The worker's generic supervisor drain budget defaults to 270 seconds through
`workers.collectors.shutdown_drain_timeout_seconds`, leaving a 30-second margin
inside the single-node container's five-minute stop grace period. While terminal
segments are still finalizing, the runtime continues renewing the stream lease;
a replacement owner cannot overlap unfinished canonicalization. Lease release
failure is a task failure with correlated context, not a suppressed best-effort
cleanup. Any supervisor, lifecycle, or worker-heartbeat shutdown failure also
makes the worker exit nonzero after bounded cleanup. These rules apply to every
registered continuous projection. L2 merely has enough terminal state to make
the boundary especially visible.

After an interruption, the next owner scans durable spool segments before
opening a new session. It:

1. validates and reclaims the original session under a fresh lease generation;
2. truncates only an incomplete final JSONL tail and records the byte count;
3. publishes the immutable archive and mappings idempotently;
4. invokes the registered projection adapter to rebuild canonical output;
5. closes or invalidates prior coverage at its last already-proven event;
6. records recovery lifecycle evidence and releases the recovery claim.

The next provider connection must establish a new coverage interval. Collector
downtime is therefore visible and never bridged by invented completeness.
Trade recovery republishes immutable trades. L2 recovery verifies a typed book
checkpoint, replays acknowledged raw deltas, reconciles the exact state and
validity interval, then records the restart discontinuity. If that proof is not
possible, the book remains invalid until a fresh provider snapshot establishes
a new valid interval.

The retained-spool boundary is authoritative during recovery. A process can be
interrupted after its canonical book transaction advances the disposable
reconstruction watermark but before the local spool acknowledgement is written.
L2 recovery therefore selects the latest verified checkpoint strictly before
the first retained record, replays only earlier acknowledged archives, and then
re-applies every retained segment idempotently. It does not try to reconcile a
pre-tail reducer against a watermark that can already include that same tail.
When the immutable book Fact already exists, recovery reuses its persisted
acceptance clock; an interrupted post-book feature stage can therefore be
repaired without manufacturing a correction to the canonical source Fact.
This rule belongs to the L2 projection adapter; lease, WAL, queue, lifecycle,
and restart orchestration remain provider- and projection-neutral.

Derived BBO and depth source validation uses the canonical L2 observation key
to reach the existing `(series_id, observation_key, revision)` index before it
checks provenance, validity, and state hashes. Source proof remains strict, but
continuous finalization no longer performs a JSON-expression scan for every
derived observation. Sustained admission still depends on measured
capture-to-canonicalization lag and bounded spool growth, not merely free disk.

## Storage Lifecycle

The collector worker also owns a provider-independent storage-lifecycle
supervisor. It plans bounded work on an hourly default cadence and never blocks
the acquisition loop. Planning and execution share one typed policy; the safe
default is `execution_enabled: false`, so deployment produces plans without
mutation until an operator reviews the output and explicitly enables execution.
Archive windows, hot-table windows, compaction thresholds, batch limits, and
the execution gate are configuration values with environment bindings shared
by the backend API and collector worker; moving to a different volume or cloud
deployment does not require a code change.

Every execution run takes one global PostgreSQL advisory fence. Dataset freeze
and explicit archive-pin transactions take the corresponding shared fence, so
an object or Timescale chunk cannot pass its final pin check while a new pin is
being committed. Operations are idempotent and append `planned`, `completed`,
`skipped`, or `failed` evidence to `market.storage_lifecycle_events`.

The lifecycle has four bounded action types:

- `archive_compact` combines a contiguous, same-session/same-epoch raw manifest
  set into verified Parquet/ZSTD without taking or interrupting the live stream
  lease. Source bytes remain until their grace period expires.
- `archive_expire` rechecks dataset and explicit pins, verifies source and any
  replacement checksums, fsyncs filesystem deletion, then records immutable
  completion evidence. A manifest remains visible as `expired` and replay fails
  with that explicit state.
- `chunk_compress` compresses only old, fully closed Timescale chunks. L2 parent
  and child chunks are treated as one layout group.
- `chunk_expire` drops a complete chunk group only after a last-moment frozen
  dataset overlap check. Candle, open-interest, and funding rows are
  compression-only by default; raw trades, L2, and reproducible feature tables
  have separate configured hot windows.

No Timescale `add_retention_policy` job is installed because it cannot enforce
Quant-Trad's dataset pins. Chunk removal stays in the application lifecycle.
The one-time
`scripts/db/manual_enable_market_storage_lifecycle_v1.sql` script converts the
two L2 child tables to hypertables and configures compression for all covered
tables. It takes the same lifecycle fence and fails if any stream lease is
active; it is an out-of-band activation step and is never run by application
startup.

Operator surfaces are:

- `qt data market-structure lifecycle-plan` for a non-mutating candidate and
  blocker report;
- `qt data market-structure lifecycle-run` for the same dry run;
- `qt data market-structure lifecycle-run --execute` only after the policy gate
  is enabled;
- `qt data market-structure lifecycle-events` for immutable deletion,
  compaction, compression, skip, and failure evidence.
- `qt data collectors create-structured` for a reviewed scheduled structured
  Fact definition; `--enabled` is required to grant collection authority.

## Operator Lifecycle

`qt data market-structure` retains code-reviewed setup, proof, and safety
operations:

- `continuous-validate` starts the implementation path for a bounded
  60-second to seven-day proof;
- `continuous-evidence` derives proof status from canonical session, archive,
  mapping, quality, and coverage rows;
- `enroll` applies a validated product/stream fleet manifest;
- `safety-halt`, `safety-acknowledge`, and `safety-status` operate persistent
  global, fleet, and stream latches.

Normal lifecycle control is provider-neutral and goes through
`qt data collectors start|stop|restart|pause|resume`. The same operations are
used by Frontend V2 and MCP, preserve request/actor/reason context, and append
immutable operation evidence. No surface edits the stream adapter or runtime
configuration while operating it.

The BIP, ETP, and SLP trade and L2 enrollments are continuous: their runtime has
no `stop_at`. A restart reconstructs desired tasks
from the database and cannot bypass an active safety latch. Reapplying an
enrollment changes reviewed configuration but does not overwrite an operator's
later stopped or paused lifecycle state.

## Resource Authority

Resource detection is scoped. Container CPU/memory and Docker engine storage
can be detected inside the runtime. On native Linux, the engine filesystem is
authoritative for its backing volume. On Docker Desktop/WSL, that same sample is
only virtual-guest capacity and must not be presented or admitted as physical
host headroom.

`docker_capacity_sampler.sh` emits `capacity_scope`, `capacity_authority`,
`physical_host_visible`, `runtime_kind`, and `resource_id` with each sample.
The Docker storage mount is configurable with `QT_DOCKER_STORAGE_ROOT`; no host
drive is encoded in the application.

On Windows Docker Desktop, the optional
`scripts/reporting/host_capacity_sampler.ps1` reads Docker's
`CustomWslDistroDir`, locates the Docker data VHDX, derives its backing volume,
and emits physical free/used/reserve bytes, allocated VHDX growth, and projected
days to reserve. It falls back only to bounded Docker/WSL metadata locations and
fails visibly when discovery is unavailable. Promtail ingests its bounded daily
NDJSON files through the existing Loki path. Its opt-in
`-InstallScheduledTask` mode installs a user-level, at-logon, restart-on-failure
task; application startup never mutates host scheduling implicitly.

Cloud deployments should provide the equivalent `physical_host_filesystem` or
`cloud_volume` authority from the mounted data volume/provider. Automatic
detection is valid only when the runtime can observe the real backing resource;
otherwise the state is `unavailable`, never an optimistic estimate.

## Collector Safety Authority

Each enrollment pins a `CollectorSafetyPolicy`. Qualification derives adapter
support, exact product registration, writable storage, actual filesystem free
bytes, current spool utilization, and applicable persistent latches. Operators
do not supply an optimistic storage budget to make a stream eligible.

Warning thresholds append immutable safety evidence and trigger Grafana without
stopping collection. Critical thresholds append evidence, latch the stream,
disable desired work, and let the normal collector shutdown drain archives and
canonicalization before the lease is released. Local spool exhaustion remains
an immediate fail-closed condition.

Safety state is a database projection derived from append-only warning, halt,
and acknowledgement events. The applicable `global:*`, fleet, and stream scopes
are checked on every supervisor pass, so process/container restart cannot clear
a halt. A distinct operator acknowledgement is required to release a scope.
