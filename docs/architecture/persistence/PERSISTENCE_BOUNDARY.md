---
component: persistence-boundary
subsystem: persistence
layer: boundary
doc_type: architecture
status: active
tags:
  - storage
  - persistence
  - runtime-events
  - ledger
  - leasing
  - postgres
code_paths:
  - portal/backend/db/models.py
  - portal/backend/db/session.py
  - portal/backend/service/provenance.py
  - portal/backend/service/storage
  - portal/backend/service/bots/storage_gateway.py
  - portal/backend/service/storage/repos/lifecycle.py
  - portal/backend/service/storage/repos/run_leases.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/bots/botlens_domain_events.py
  - portal/backend/service/bots/botlens_canonical_facts.py
  - portal/backend/service/bots/botlens_event_retention.py
  - src/engines/bot_runtime/runtime/components/canonical_facts.py
  - src/engines/bot_runtime/runtime/components/step_trace_buffer.py
  - src/engines/bot_runtime/runtime/components/step_trace_rollup.py
  - src/engines/bot_runtime/runtime/components/overlay_delta.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - docs/architecture/persistence/diagrams/runtime-event-ledger-flow.mmd
  - scripts/db/manual_migration_versioning_hard_cutover.sql
  - scripts/db/manual_migration_canonical_lifecycle_ledger_v1.sql
---
# Persistence Boundary

## Purpose

The persistence boundary stores durable runtime truth and read-model support data. It protects replay, BotLens rebuilds, reporting, comparison, and operator recovery.

Related diagram: [runtime-event-ledger-flow.mmd](diagrams/runtime-event-ledger-flow.mmd).

## Boundary Contract

`PG_DSN` is the only runtime persistence DSN. Each persistence responsibility
is owned by one named module under `portal/backend/service/storage/repos`.
Services import those owners directly unless orchestration needs an injectable
protocol, in which case the protocol and its repository-backed implementation
live at the consuming service boundary. The bot runtime uses
`bots/storage_gateway.py` for that purpose. The storage packages do not
re-export repository functions, and there is no aggregate storage facade.

Persistence owns durable storage. It does not own execution decisions or projection interpretation.

Physical table, index, constraint, and ORM class names are stable concern names,
not version carriers. Version and code-provenance fields live on the artifact
that owns them. Runtime run rows carry runtime contract/source/image/storage
provenance; report materialization rows carry report contract, dataset contract,
builder source revision, and report storage provenance.

## Schema Bootstrap Contract

Fresh database bootstrap is a first-class persistence responsibility. Backend
startup creates the current model-declared schemas, tables, and indexes from
`portal/backend/db/models.py` using the single `PG_DSN`; a new desktop or clean
Docker volume must not require operators to replay historical manual migration
files.

Bootstrap may create missing schemas, tables, and model-declared indexes. It
must not use blanket `IF NOT EXISTS` DDL as the schema contract. Existing tables
are inspected against the current column contract; missing columns fail loud
with the table and column names so the operator can rebuild the database or
intentionally run an out-of-band migration.

Required operational indexes are part of the current schema contract. If they
are still absent after bootstrap attempts to create model-declared indexes,
startup fails. Historical SQL files under `scripts/db/` are repair/reference
artifacts for old local databases, not normal fresh-start instructions.

Provider market-data storage follows the same rule for
`market_candles_raw`, `derivatives_market_state`, and
`portal_candle_closures`: inspect the configured table names, create missing
tables once, assert existing columns, create required indexes, and fail loud if
the contract still does not hold. Provider credential helpers do not create
their own table; `portal_provider_credential_refs` is owned by the portal ORM
metadata and credential helpers only validate that the bootstrapped table and
lookup index exist.

## Diagram Walkthrough

[runtime-event-ledger-flow.mmd](diagrams/runtime-event-ledger-flow.mmd) shows:

1. Execution runtime emits domain events and trade/lifecycle facts.
2. Storage repositories write durable rows with typed hot fields.
3. BotLens projectors rebuild bounded read models.
4. Reports rebuild datasets from run, trade, event, and step truth.
5. Forensics page the ledger on cold paths.

## What Persistence Must Rebuild

Persistence stores the rows needed to recover a run without asking projections
what they last displayed. `portal_bot_runs`, trade rows/events, run leases,
and runtime events including lifecycle events are durable runtime evidence.
Reports, BotLens rebuilds, and forensics should be able to start from those
rows and explain the run again.

`portal_bots` is different: it stores bot definitions, not runtime liveness. A
bot definition may contain name, strategy binding, run defaults, risk/wallet
config, execution settings, and environment config. It should not become the
place to recover run status, runner ownership, summaries, heartbeats, or report
artifacts.

BotLens projections, observability rows, and report artifact status are
convenience or diagnostic state. They can be rebuilt,
unavailable, stale, or degraded, but they must not contradict durable runtime
evidence. Viewer/debug writes also must not become material run identity just
because they share a `run_id`, symbol, or continuity payload shape.

## Table Contract Triage

Active schema surfaces are justified by role:

- Keep as durable truth: `portal_bot_runs`, `portal_bot_run_events`,
  `portal_bot_run_event_seq_allocators`, `portal_bot_trades`,
  `portal_bot_trade_events`, `portal_bot_run_leases`, strategy/bot/instrument
  config tables, and market data source tables.
- Keep as definition only: `portal_bots`. Runtime state belongs to
  `portal_bot_runs`, canonical lifecycle events, run leases, and report
  materialization tables. Fleet cards and API responses may project those rows
  together, but readers must not recover runtime truth from bot definition
  columns.
- Keep as bounded observability: `observability_events.botlens_backend_events`
  and `observability_metrics.botlens_backend_metric_rollups`.
- Keep as bounded profiler data: `portal_bot_run_step_rollups` stores typed
  bucketed phase-duration metrics with mergeable histogram counts for p95/p99
  estimates. Raw `portal_bot_run_steps` rows are not part of the schema
  contract, and the storage repository accepts only precomputed rollups. Queue
  depth, queue lag, worker health, payload-size, and sub-phase debug metrics
  belong to observability rollups, not per-step durable rows.
- Removed from active contract: `portal_bot_run_snapshots` and
  `portal_bot_run_view_state`. They were legacy projection/cache payload stores,
  not canonical truth.
- Keep as bounded job coordination: `portal_async_jobs.result` may hold a fresh
  worker result long enough for the waiting API request to return, but succeeded
  QuantLab jobs are not reusable result-cache truth. Finished result payloads are
  pruned to bounded summaries after the configured short retention window.
- Removed from active contract:
  `observability_metrics.botlens_backend_metric_samples_v1`; raw samples are not
  a durable database surface.
- Removed from active `portal_bots` contract: `status`, `last_run_at`,
  `last_stats`, `last_run_artifact`, `runner_id`, and `heartbeat_at`.

## Runtime Event Storage Budget

Runtime event storage persists material truth, not every emitted BotLens or
transport event. Event retention is tiered:

- Tier 1, canonical run truth: lifecycle terminal facts, signals, decisions,
  material trade lifecycle facts, wallet ledger facts, material hashes,
  report metadata, faults, and diagnostics that affect trust/readiness. These
  are persisted long term.
- Tier 2, research context: compact series/catalog context, continuity
  summaries, selected indicator/world-state context, and decision/trade
  evidence. Terminal `run_final` continuity summaries are material report
  evidence; BotLens selected-symbol/bootstrap continuity rows are diagnostic
  observability unless explicitly promoted through a canonical path. Raw
  per-bar candles and per-bar stats are summarized or referenced from
  source/catalog storage instead of retained as runtime-event rows.
- Tier 3, observability metrics: write latency, queue depth, runtime health,
  projector timing, and step metrics. These are aggregated in
  `observability_metrics.botlens_backend_metric_rollups`; raw metric samples
  are in-memory/live-only and are not part of the database schema contract.
- Tier 4, live UI/projection transport: viewer notifications, repeated health
  pings, unchanged overlay state, and live fanout deltas. These are transport
  messages and bounded projection inputs, not permanent ledger rows.

The runtime may still assign viewer-blind fact-stream sequence numbers to
Tier 2-4 projection/debug messages so BotLens can ingest and project them while
a run is active. Backend websocket transport has its own live delivery cursors.
Durable `run_seq` is assigned only to rows retained by the storage-budget
policy, so raw transport events do not create permanent ledger growth or
ordering gaps.

## Event Ledger Shape

Runtime events should carry typed hot fields for common query paths:

- `bot_id`, `run_id`, `seq`,
- `event_name`, `series_key`, `correlation_id`, `root_id`,
- `bar_time`, `instrument_id`, `symbol`, `timeframe`,
- `signal_id`, `decision_id`, `trade_id`,
- `reason_code`, `event_time`, `known_at`,
- `run_seq`, `run_seq_status`.

The full payload can remain richer, but readers should not parse giant blobs for ordinary routing and correlation.

`seq` is a producer/batch sequence and may repeat for multiple BotLens-domain
facts emitted in one runtime batch. Canonical replay order is `run_seq`: a
dense, monotonic, per-run event sequence assigned by the runtime-event
persistence boundary at canonical append time. `run_seq` starts at 1 for a run
and is stamped into durable event context with `run_seq_status=runtime_assigned`.
It is not assigned by frontend, projection, reporting, or export code.

Within one producer/batch `seq`, persistence preserves the producer's semantic
event order while assigning dense `run_seq` values. Event IDs are idempotency
keys and never determine replay order. Reusing an event ID is a no-op only when
the bot, run, event type, schema, criticality, event time, and semantic payload
match. Allocator-owned `run_seq` fields are excluded from that comparison;
divergent event material fails loudly.

Runtime event persistence allocates `run_seq` from
`portal_bot_run_event_seq_allocators` inside the same transaction that inserts
the committed event rows. Duplicate event ids are removed before allocation, so
idempotent replays and no-op conflicts do not consume sequence numbers. The
ledger stores `run_seq` both as a typed hot column and in payload context; the
allocator table is the hot-path source of truth, not a JSON `MAX(run_seq)` scan
over `portal_bot_run_events`.

Lifecycle checkpoint reads filter canonical lifecycle event names from
`portal_bot_run_events` and expose the runtime-event `run_seq` as lifecycle
`seq`. `portal_bot_runs.status` and its start/end timestamps are a rebuildable
current-run summary projection. No lifecycle-specific history or current-state
mirror is part of the schema contract.
The canonical lifecycle append and run-summary projection commit in one
database transaction; a projection failure rolls back both so terminal state
cannot leave a permanently stale summary. Non-lifecycle run upserts reject
`status`, `started_at`, and `ended_at`; startup, runtime control, reporting, and
artifact writers cannot bypass the lifecycle ledger. Lifecycle admission
rejects unknown phase/status/owner values, phase/status mismatches, backdated
checkpoints, and checkpoints after a terminal state. The explicit
`rebuild_bot_run_lifecycle_summary` operation restores projected status and
timestamps from the ordered ledger while preserving independently stored run
configuration, provenance, and report summary data.

Runtime fact transport and durable persistence must not compete to write the
same event id. Source-owned canonical facts, including wallet ledger facts, are
projection inputs when they arrive through the live BotLens transport; they are
not written again by ingest. The source runtime may buffer these facts for
bounded async batch persistence after assigning their producer sequence, but the
buffer is part of the required persistence contract: overflow, writer failure,
or terminal drain timeout is a run failure, not telemetry loss. Ingest may keep
a bounded per-process event-id idempotence cache to avoid repeated no-op DB
prechecks for stable health, overlay, diagnostic, or stats facts. The database
uniqueness constraint remains the final correctness guard after restarts or
retries.

Projection fanout uses a separate bounded dispatcher over the already committed
batch. That dispatcher is not a second persistence authority and must not assign
or rewrite durable ordering. It exists to keep websocket/projector fanout
pressure out of the runtime bar step while preserving visible degradation
semantics. Projection overflow or drain timeout may drop stale visual/debug
handoffs and mark BotLens projection degraded. It must not fail the run unless
canonical persistence also failed.

Source-owned runtime batches carry both live facts and durable facts. The
durable writer filters those batches through
`botlens_event_retention.py`: signals, decisions, material trades, wallet facts,
and compact catalog facts are retained; raw candle, health, overlay, stats, and
nonmaterial diagnostic messages are summarized, aggregated, or kept live-only.
Before retention, the runtime fact stream already compacts high-volume
projection/debug facts at the source: health facts exclude full snapshots,
series identity excludes full instrument/provider blobs, stats facts use the
compact reportable summary, and overlay deltas use bounded render payloads with
payload summaries. The storage layer should not depend on a second pass to make
unbounded live payloads safe.

Rows without runtime-assigned `run_seq` are inspection-only and not
certification grade. Backfills must mark `run_seq_status=backfilled`, and
reports must caveat or block golden-run certification when ordering is missing,
backfilled, or inconsistent.

Derived wallet ledger facts carry their persistence `run_seq`, source lineage,
and wallet causal order separately. Replay uses `wallet_commit_seq` plus
`wallet_event_order` to apply wallet state transitions in committed wallet
order, even if derived facts are persisted later. `source_run_seq` remains
diagnostic lineage; it is not a replay fallback for modern rows. A wallet ledger
fact without `wallet_commit_seq` is malformed and must block certification.

## Failure And Recovery

- Required persistence for audit trails fails loud.
- Missing required columns fail with actionable errors.
- Missing model-declared indexes are created during bootstrap.
- Missing required indexes after bootstrap fail with actionable errors.
- Duplicate event IDs represent idempotency/replay outcomes, not new truth.
- Sequence/cursor ordering is a replay contract.

## Invariants

- Durable truth is append-friendly and replayable.
- Runtime events preserve known-at context.
- Storage does not perform hidden execution reconstruction.
- Fresh schemas come from current clean definitions.
- Existing schema drift fails loud instead of being patched with hidden runtime backfills.

## Related Docs

- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Identity and correlation boundary](../identity/IDENTITY_AND_CORRELATION_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [ADR 0016: Treat runtime event ledger order as operational evidence](../decisions/0016-treat-runtime-event-ledger-order-as-operational-evidence.md)
- [ADR 0042: Runtime event ledger as lifecycle truth](../decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md)
- [ADR 0043: Canonical accounting reconciliation](../decisions/0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md)
