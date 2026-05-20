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
  - portal/backend/service/storage
  - portal/backend/service/storage/repos/run_leases.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/bots/botlens_domain_events.py
  - portal/backend/service/bots/botlens_canonical_facts.py
  - portal/backend/service/bots/botlens_event_retention.py
  - src/engines/bot_runtime/runtime/components/overlay_delta.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - docs/architecture/persistence/diagrams/runtime-event-ledger-flow.mmd
---
# Persistence Boundary

## Purpose

The persistence boundary stores durable runtime truth and read-model support data. It protects replay, BotLens rebuilds, reporting, comparison, and operator recovery.

Related diagram: [runtime-event-ledger-flow.mmd](diagrams/runtime-event-ledger-flow.mmd).

## Boundary Contract

`PG_DSN` is the only runtime persistence DSN. Runtime services should write through explicit repository or gateway boundaries, not hidden globals or alternate data stores.

Persistence owns durable storage. It does not own execution decisions or projection interpretation.

## Diagram Walkthrough

[runtime-event-ledger-flow.mmd](diagrams/runtime-event-ledger-flow.mmd) shows:

1. Execution runtime emits domain events and trade/lifecycle facts.
2. Storage repositories write durable rows with typed hot fields.
3. BotLens projectors rebuild bounded read models.
4. Reports rebuild datasets from run, trade, event, and step truth.
5. Forensics page the ledger on cold paths.

## Canonical Durable Truth

Canonical runtime persistence includes:

- `portal_bot_runs`,
- `portal_bot_trades`,
- `portal_bot_trade_events`,
- `portal_bot_run_leases`,
- `portal_bot_run_events`,
- lifecycle checkpoint rows.

Projection or convenience state includes:

- BotLens run/symbol projections,
- lifecycle helper views,
- observability metric/event rows,
- report artifact status.

Projection rows can be rebuilt or unavailable. They must not contradict canonical runtime truth.
Observability metric/event rows are diagnostic storage. They are not report or
golden certification evidence unless the same fact is written through an
explicit canonical runtime/reporting path. Viewer/debug writes must never
promote themselves into material run identity by sharing a run ID, symbol, or
continuity payload shape.

## Table Contract Triage

Active schema surfaces are justified by role:

- Keep as durable truth: `portal_bot_runs`, `portal_bot_run_events`,
  `portal_bot_run_event_seq_allocators`, `portal_bot_trades`,
  `portal_bot_trade_events`, `portal_bot_run_leases`, `portal_bot_run_lifecycle`,
  `portal_bot_run_lifecycle_events`, strategy/bot/instrument config tables, and
  market data source tables.
- Keep as bounded observability: `observability_events.botlens_backend_events_v1`
  and `observability_metrics.botlens_backend_metric_rollups_v1`.
- Keep as bounded profiler data: `portal_bot_run_step_rollups_v1` stores typed
  bucketed step metrics with mergeable histogram counts for p95/p99 estimates.
  Raw `portal_bot_run_steps` rows are not part of the schema contract. Step
  rollups intentionally use an allowlist of latency, queue pressure, payload
  size, and execution timing fields; arbitrary `_ms` or `_count` debug context
  must not become durable storage by suffix match.
- Removed from active contract: `portal_bot_run_snapshots` and
  `portal_bot_run_view_state`. They were legacy projection/cache payload stores,
  not canonical truth.
- Question/reshape: `portal_async_jobs.result` is active for QuantLab async
  responses and short-lived result reuse, but large overlay result blobs do not
  belong permanently in the job queue row. The job table should retain status,
  request fingerprint, failure context, and a bounded summary or artifact
  pointer.
- Removed from active contract:
  `observability_metrics.botlens_backend_metric_samples_v1`; raw samples are not
  a durable database surface.

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
  `observability_metrics.botlens_backend_metric_rollups_v1`; raw metric samples
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

Runtime event persistence allocates `run_seq` from
`portal_bot_run_event_seq_allocators` inside the same transaction that inserts
the committed event rows. Duplicate event ids are removed before allocation, so
idempotent replays and no-op conflicts do not consume sequence numbers. The
ledger stores `run_seq` both as a typed hot column and in payload context; the
allocator table is the hot-path source of truth, not a JSON `MAX(run_seq)` scan
over `portal_bot_run_events`.

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
- Missing useful indexes should warn with migration guidance.
- Duplicate event IDs represent idempotency/replay outcomes, not new truth.
- Sequence/cursor ordering is a replay contract.

## Invariants

- Durable truth is append-friendly and replayable.
- Runtime events preserve known-at context.
- Storage does not perform hidden execution reconstruction.
- Schema changes come from clean definitions or explicit migrations, not runtime backfills.

## Related Docs

- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Identity and correlation boundary](../identity/IDENTITY_AND_CORRELATION_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [ADR 0016: Treat runtime event ledger order as operational evidence](../decisions/0016-treat-runtime-event-ledger-order-as-operational-evidence.md)
