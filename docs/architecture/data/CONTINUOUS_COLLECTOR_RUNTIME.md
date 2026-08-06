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
  - src/data_providers/streams/runtime.py
  - src/core/market_storage_lifecycle.py
  - src/market_data/archive.py
  - portal/backend/service/market/collector_supervisor.py
  - portal/backend/service/market/continuous_stream_collector.py
  - portal/backend/service/market/market_storage_lifecycle.py
  - portal/backend/service/market/market_structure_service.py
  - portal/backend/service/storage/repos/market_lifecycle.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/workers/market_data_collector.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - scripts/reporting/docker_capacity_sampler.sh
  - scripts/reporting/host_capacity_sampler.ps1
  - scripts/db/manual_enable_market_storage_lifecycle_v1.sql
  - docker/grafana/provisioning/dashboards/capacity-database-growth.json
---
  - docker/docker-compose.yml
# Continuous Collector Runtime

## Purpose

The continuous collector runtime owns provider streams that must remain active
indefinitely. It is a generic supervised worker boundary, not a trade-specific
daemon. Provider/domain implementations register adapters against explicit
stream definitions; unsupported or ambiguous definitions fail loudly without
stopping healthy collectors.

The scheduled open-interest and funding collectors continue to use the same
worker process and existing lease/attempt contracts. The first continuous
adapter implements Coinbase `market_trades` plus `heartbeats`. Future trade,
book, news, or alternate-provider adapters must register explicitly rather than
adding provider switches to the supervisor.

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

After a terminal segment is archived, mapped, canonicalized, and its terminal
coverage revision is committed, the finalizer retires that connection epoch's
projection state. Memory is therefore bounded by active/finalizing epochs, not
the lifetime reconnect count.

## Stop, Restart, And Recovery

A normal stop closes the provider connection, seals the final segment, drains
all archive/database finalizers, records terminal evidence, and only then
releases the fencing lease.

After an interruption, the next owner scans durable spool segments before
opening a new session. It:

1. validates and reclaims the original session under a fresh lease generation;
2. truncates only an incomplete final JSONL tail and records the byte count;
3. publishes the immutable archive and mappings idempotently;
4. republishes canonical trades without claiming coverage across downtime;
5. closes any prior open-valid coverage at its last already-proven event;
6. records recovery lifecycle evidence and releases the recovery claim.

The next provider connection must establish a new coverage interval. Collector
downtime is therefore visible and never bridged by invented completeness.

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

## Operator Lifecycle

`qt data market-structure` exposes worker-owned controls:

- `continuous-validate` starts the production implementation path for a bounded
  60-second to seven-day proof;
- `continuous-evidence` derives proof status from canonical session, archive,
  mapping, quality, and coverage rows;
- `continuous-admit` records explicit operator and resource-budget approval;
- `continuous-start` starts admitted production with no duration cap;
- `continuous-stop` requests graceful drain and stop.

The 24-hour requirement is an admission proof, not a collector lifetime cap.
Callers cannot assert that proof with a Boolean: admission replaces submitted
claims with repository-derived evidence and rejects failures, archive mapping
lag, open/invalid terminal coverage, missing archive evidence, or less than 24
hours of implemented-path runtime.

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

## Production Admission

Admission requires both canonical validation evidence and an explicit resource
budget containing an authoritative resource identity, observation time,
physical visibility, observed free bytes, observed growth, allowed daily
growth, and minimum headroom. Virtual-guest storage is rejected. Observed
growth above budget or free space below headroom is rejected.

Admission never starts collection by itself. Starting and stopping remain
separate explicit controls, and revoking admission disables the definition.
