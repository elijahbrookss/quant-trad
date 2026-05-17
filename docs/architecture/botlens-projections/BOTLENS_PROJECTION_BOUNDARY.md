---
component: botlens-projection-boundary
subsystem: botlens-projections
layer: boundary
doc_type: architecture
status: active
tags:
  - botlens
  - projections
  - runtime
  - debugger
  - read-model
code_paths:
  - portal/backend/service/bots/botlens_contract.py
  - portal/backend/service/bots/botlens_bootstrap_service.py
  - portal/backend/service/bots/botlens_candle_continuity.py
  - portal/backend/service/bots/botlens_canonical_facts.py
  - portal/backend/service/bots/botlens_domain_events.py
  - portal/backend/service/bots/botlens_event_retention.py
  - portal/backend/service/bots/botlens_projector_registry.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_state.py
  - portal/backend/service/bots/botlens_transport.py
  - portal/backend/service/bots/botlens_run_stream.py
  - portal/backend/service/bots/botlens_symbol_service.py
  - portal/backend/service/bots/container_runtime_telemetry.py
  - src/engines/bot_runtime/runtime/components/chart_state.py
  - src/engines/bot_runtime/runtime/components/overlay_delta.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - portal/frontend/src/features/bots/botlens
  - portal/frontend/src/components/bots/BotLensChart.jsx
  - docs/architecture/botlens-projections/diagrams/botlens-projection-flow.mmd
---
# BotLens Projection Boundary

## Purpose

BotLens is the runtime debugger. It turns runtime/domain events into bounded run and symbol projections so a user can inspect what the runtime knew and did. BotLens does not execute trades and does not create execution truth.

Related diagram: [botlens-projection-flow.mmd](diagrams/botlens-projection-flow.mmd).

## Boundary Contract

BotLens owns:

- run projection snapshots,
- symbol projection snapshots,
- selected-symbol read models,
- trade overlays,
- decision/runtime event overlays,
- live deltas and stream continuity,
- cold-path forensics reads.

BotLens does not own:

- indicator mutation,
- strategy decisions,
- fill ordering,
- wallet settlement,
- report readiness.

## Diagram Walkthrough

[botlens-projection-flow.mmd](diagrams/botlens-projection-flow.mmd) shows two paths:

1. Hot path: runtime facts feed projectors, snapshots, stream deltas, and frontend state.
2. Cold path: paged event-ledger reads feed forensic inspection.

Projector failure is explicit unavailable state. An empty chart is not a valid substitute for a failed projection.

## Run And Symbol Projections

Run projection owns run-level state:

- lifecycle,
- health,
- selected symbol catalog,
- open-trade summaries,
- runtime progress and freshness.

Run-level open-trade summaries may be fed by both run lifecycle batches and
symbol notifications. Symbol notifications are a live projection path, not
canonical trade truth. Delayed symbol notifications must not re-open a trade
after the run-level lifecycle projection has already advanced beyond that
notification and removed the trade. Closed-trade truth in the durable runtime
ledger dominates stale live projection state.

Symbol projection owns symbol-level state:

- candles,
- overlays,
- decisions,
- trades and markers,
- runtime diagnostics,
- selected-symbol inspection payload.

The frontend should treat these as projections keyed by runtime IDs, not as local execution state.

## State And Truth

BotLens state is rebuildable from runtime/domain events and supporting durable facts. It can cache, compact, and window state for the UI. It cannot invent missing decisions or reinterpret fills.

BotLens consumes more live facts than the permanent runtime ledger keeps. Raw
per-bar candles, overlay deltas, repeated health state, and runtime step
telemetry are live/debug projection inputs. They may be streamed, compacted,
aggregated, or retained in bounded observability storage, but they are not
canonical run truth. Required replay and reporting surfaces rebuild from Tier 1
material truth plus compact Tier 2 context such as series metadata, candle
continuity summaries, selected decision evidence, and source candle/catalog
references. Cold chart-history reads use source candle storage/catalog
references when raw `CANDLE_OBSERVED` runtime rows are absent.

Live transport is a projection path. When the runtime owns canonical fact
persistence, including wallet ledger facts, the ingest side uses those events
for projection and skips duplicate durable writes. Source-owned canonical facts
may arrive over live transport before their async source-side DB batch has
completed, but the run is not finalized until that buffer drains; projection
does not become canonical truth by seeing the live message first.
Transport-only derived facts may be persisted by ingest, but repeated stable
event ids should be filtered before DB access and still remain protected by
storage uniqueness.

The runtime-to-portal fact stream is viewer-blind. Runtime must not inspect
BotLens panes, websocket subscribers, selected symbols, chart state, or any
other UI demand signal. It emits the same canonical facts and bounded
projection/debug facts for the same run inputs regardless of whether anyone is
watching. Viewer demand can never change strategy timing, execution timing,
wallet timing, canonical fact emission, or run finalization.

BotLens read endpoints may compute selected-symbol continuity diagnostics for
the response they return, but ordinary reads do not durably persist observer
continuity facts. Durable observer continuity writes require the explicit debug
configuration `QT_BOT_RUNTIME_BOTLENS_PERSIST_OBSERVER_CONTINUITY=true` and
must be labeled diagnostic/non-material. These rows remain operational evidence
only; report/golden material identity accepts terminal `run_final` continuity
evidence from the canonical run path, not BotLens viewer/bootstrap facts.

The fact stream is also source-compacted before it reaches backend projectors.
`runtime_state_observed` carries only compact health/runtime fields, not the
full runtime snapshot or aggregate stats blob. `series_state_observed` carries
routing identity only. `series_stats_updated` carries the compact reportable
summary, not risk-engine debug curves. `overlay_ops_emitted` carries bounded
render overlays with payload summaries and overlay clocks, not unbounded
indicator history. This compaction is invariant across viewer state and is the
normal runtime contract, not a live-UI optimization.

The live websocket stream is a bounded viewport transport, not a full
replicated runtime database. Bootstrap snapshots send the latest configured
candle window and bounded debug context. Live updates are delta-only fact
batches derived from backend projections. When logs, decisions, overlay
geometry, or historical candles exceed the runtime fact-stream budget, runtime
may truncate debug-only fact-stream entries and expose the truncation in step
context; material trade, wallet, series, and candle facts remain the priority.
Older history belongs to cold chart-history and forensic reads.

Live transport is demand-aware only after runtime ingestion. Backend projectors
continue to ingest canonical facts and maintain rebuildable run/symbol
projections, but fanout only builds websocket payloads for active run viewers
and selected-symbol viewers. If there is no matching viewer, disposable live
deltas are dropped with `live_transport_dropped_stale_count` and the next
viewer hydrates from the cold snapshot/ledger path instead of replaying every
skipped UI payload.

Overlay live deltas use their own viewport clock:
`overlay_commit_seq`, `base_overlay_commit_seq`, and
`overlay_commit_seq_status=overlay_scoped`. This clock orders changed overlay
transport operations only. It is separate from the selected-symbol websocket
`base_seq` replay cursor and separate from the durable run `run_seq` spine.
Overlay entries may carry source `indicator_commit_seq` for provenance, but
that provenance must not force unchanged overlay geometry to emit a new delta.
Selected-symbol snapshots must include the current `overlay_commit_seq` and
status beside the bounded overlay payload so a reconnect or symbol handoff seeds
the frontend overlay cursor from the exact snapshot state. The next overlay
delta must advance that cursor and declare the matching `base_overlay_commit_seq`;
otherwise the frontend treats it as stale projection transport, advances the
stream cursor, and does not mutate overlay state.

Runtime bootstrap is not allowed to build the full chart payload as an
intermediate shape. It must assemble the selected series directly from runtime
state, using the chart state builder for the configured candle window, selected
series overlays, selected series stats, wallet/debug facts, and a bounded
closed-trade tail plus any open trades. Full multi-series chart composition and
historical aggregate trade lists belong to explicit chart/debug reads, not the
runtime-to-BotLens hot bootstrap path.

Chart/debug payloads use the same bounded visual-trade contract. Runtime
projection asks the risk engine for a configured trade window instead of asking
for every serialized trade and slicing afterward. The engine window keeps open
trades plus the recent closed-trade tail, so omitted history is never serialized
for hot visual payloads. Canonical trade truth remains in runtime events, trade
rows, and cold forensic/reporting paths.

Live trade facts use a revision cursor contract. The runtime cache stores the
last observed trade revision per series, and asks the risk engine for material
trade changes after that revision. The risk engine owns the mutation timeline:
it records which trade IDs changed when `trade_revision` advances, then
serializes only those changed trades for the fact batch. If the cursor is too
old for the retained change log, runtime logs a warning and emits the available
current trade batch as a projection resync boundary rather than silently
assuming the cursor delta is complete.

Symbol-to-run notifications are live projection transport. A pending
notification for the same run and symbol may be replaced by a newer one before
the run projector consumes it. Replacement keeps the latest symbol/runtime
summary and folds trade upsert/removal deltas to their net state so live
projection does not accumulate repeated stale summaries. The durable runtime
ledger remains the recovery source when notification pressure causes coalescing
or overflow.

The durable ingest path now applies the same retention budget as source-side
persistence. Source-persisted material facts are not written again by ingest;
transport-owned facts are persisted only when they are compact research context
or material diagnostics. Live-only facts continue through the projectors and
fanout channels without becoming permanent runtime-event rows.

The canonical replay source for run projection reconciliation is the durable
runtime event ledger. A completed run projection that still has open trades must
replay from that ledger before publishing terminal projection truth. If replay
proves the trades are closed, the projection may be marked reconciled; if replay
is unavailable or still disagrees with the ledger, projection health remains
degraded and golden-run certification must stay blocked.

Projection replay uses the typed ledger `run_seq` spine to enumerate durable
facts, then uses scoped causal clocks for domain ordering. Position/trade
lifecycle facts with `position_commit_seq` are ordered by
`trade_id, position_commit_seq` when projecting trade state; this lets a durable
close tombstone dominate a stale open notification even if append/run sequence
arrival crosses batches. Rows without runtime-assigned `run_seq` are not
certification-grade replay truth and may only be used by explicit forensic
tools, but `run_seq` itself must not override a valid scoped position clock.

## Failure And Recovery

- Missing projector state returns unavailable/projection-error state.
- Stale selected-symbol snapshots are rejected or refreshed.
- Stream continuity uses sequence/cursor fields.
- Rebuild failures surface bounded operational faults.
- Forensics can page the ledger when live projection is insufficient.
- Run-notification queue overflow is a projection-health event. The queue may
  coalesce or drop older notifications to keep the latest notification moving,
  but overflow requires canonical ledger replay before projection state can be
  trusted for a golden run.

## Invariants

- BotLens is a debugger, not a demo path.
- Projection and transport payloads stay bounded.
- Heavy event history belongs on cold paths.
- Runtime truth remains in execution events and trade rows.
- Closed-trade truth in the durable ledger must dominate stale projection
  notifications.

## Related Docs

- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Identity and correlation boundary](../identity/IDENTITY_AND_CORRELATION_BOUNDARY.md)
- [Observability boundary](../observability/OBSERVABILITY_BOUNDARY.md)
