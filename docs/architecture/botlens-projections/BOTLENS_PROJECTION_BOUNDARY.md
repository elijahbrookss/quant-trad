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
  - portal/backend/service/bots/botlens_chart_service.py
  - portal/backend/service/bots/botlens_chart_contracts.py
  - portal/backend/service/bots/botlens_overlay_history.py
  - portal/backend/service/bots/botlens_forensics_service.py
  - portal/backend/service/storage/repos/candles.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/bots/botlens_candle_continuity.py
  - portal/backend/service/bots/botlens_canonical_facts.py
  - portal/backend/service/bots/botlens_domain_events.py
  - portal/backend/service/bots/botlens_event_retention.py
  - portal/backend/service/bots/botlens_intake_router.py
  - portal/backend/service/bots/botlens_projector_registry.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_state.py
  - portal/backend/service/bots/botlens_transport.py
  - portal/backend/service/bots/botlens_run_stream.py
  - portal/backend/service/bots/botlens_symbol_service.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/bots/container_runtime_telemetry.py
  - portal/backend/service/bots/paper_market_stream.py
  - src/core/settings.py
  - src/engines/bot_runtime/live_market.py
  - src/engines/bot_runtime/runtime/components/canonical_facts.py
  - src/engines/bot_runtime/runtime/components/chart_state.py
  - src/engines/bot_runtime/runtime/components/overlay_delta.py
  - src/engines/bot_runtime/runtime/mixins/setup_prepare.py
  - src/engines/bot_runtime/runtime/mixins/execution_loop.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - src/engines/bot_runtime/runtime/mixins/runtime_projection.py
  - src/engines/bot_runtime/strategy/series_builder_parts/models.py
  - portal/frontend/src/features/bots/botlens
  - portal/frontend/src/components/bots/BotLensChart.jsx
  - portal/frontend/src/components/bots/botlensProjection.js
  - portal/frontend/src/features/bots/botlens/buildBotLensRuntimeViewModel.js
  - portal/frontend/src/features/bots/botlens/components/ChartPanel.jsx
  - portal/frontend/src/features/bots/botlens/state/botlensRuntimeSelectors.js
  - portal/frontend/src/features/bots/botlens/state/botlensRuntimeState.js
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
- trade markers and trade visual projections built from trade facts,
- decision/runtime event overlays,
- bounded visual overlay projections,
- live deltas and stream continuity,
- cold-path forensics reads.

BotLens does not own:

- indicator mutation,
- strategy decisions,
- fill ordering,
- wallet settlement,
- report readiness.

BotLens may display a provider-derived provisional candle for a live paper run,
but that candle is projection/debug state only. It is not a runtime candle, not
strategy input, not report input, and not execution-eligible market truth.

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
- display-only provisional candle,
- bounded overlays,
- overlay projection metadata,
- decisions,
- trades and markers,
- runtime diagnostics,
- selected-symbol inspection payload.

The frontend should treat these as projections keyed by runtime IDs, not as local execution state.

Symbol fact intake drains in bounded batches. A symbol projector may apply many
ready fact envelopes in one drain turn, preserving event order while emitting a
single run-summary notification and a single fanout package for the drain. This
keeps burst pressure from multiplying scheduler turns and downstream messages
without changing canonical runtime truth.

Run health warnings are run-level state. Run health carries one warning summary
with per-symbol buckets and grouped warning conditions. Symbol views may render
the matching bucket as a badge or compact runtime summary, but they must not
copy the entire run warning list into every symbol runtime panel.

## Rebuilding From Runtime Facts

BotLens state is rebuildable from runtime/domain events and supporting durable
facts. It can cache, compact, and window that state for the UI, but it cannot
invent missing decisions or reinterpret fills.

BotLens sees more live/debug material than the permanent runtime ledger keeps:
raw per-bar candles, overlay deltas, repeated health state, provisional candles,
runtime step telemetry, and disposable websocket deltas. Those facts may be
streamed, compacted, aggregated, or retained in bounded observability storage.
They are still projection inputs, not canonical run truth.

Replay and reports rebuild from material runtime truth plus compact context:
series metadata, candle-continuity summaries, selected decision evidence, and
source candle/catalog references. When raw CANDLE_OBSERVED runtime rows are not
retained, a dataset-bound backtest chart reads only the matching frozen series
at its dataset commit boundary. A non-dataset paper/live chart may read the
current canonical hot store. The response labels which source supplied bars; a
dataset-bound run never silently falls through to later canonical revisions.

## Durable Eligibility And Cold Replay

Run inventories do not reconstruct terminal projections. They batch one compact
durable-ledger summary per requested run and report replay eligibility from that
evidence or from an existing hot projection. Selecting a terminal run rebuilds
only run/catalog scope under the bounded bootstrap timeout; it does not embed a
full selected-symbol history. The selected-symbol projector is a secondary read,
while a dataset-bound chart and forensic cursor pages may begin as soon as run
scope identifies the frozen dataset and canonical series.

Selected-symbol snapshot transport is an explicit latest-tail window: 32 signals,
32 decisions, 64 trade states, 32 diagnostics, and 160 overlays. Every concern
reports included and available counts, `ordering=latest_tail`, and whether it
was truncated. The full projector remains authoritative for live state; the
durable cursor path is authoritative for complete historical inspection.

Chart-history pages pair each bounded candle range with trade states rebuilt from
the retained `TRADE_OPENED`, `TRADE_UPDATED`, and `TRADE_CLOSED` facts. The
response reports ordering assurance, terminal-run completeness for the returned
candles, the event and trade counts, and a stable evidence fingerprint. Page
merging uses the same close-dominance and `position_commit_seq` semantics as the
live projector, so loading an older entry page cannot reopen a closed trade. The
frontend keeps this bounded chart-marker history separate from the compact
recent-trades table.

Cold decision inspection pages typed domain truth by run, selected series, and
the stable after_seq/after_row_id cursor. Each page is bounded to 200 events in
the operator UI (the backend contract caps requests at 1,000). Snapshot and cold
rows are deduplicated by domain event identity; missing, failed, or exhausted
pages remain explicit. Cold reads never become execution authority.

Event identity must distinguish distinct observations while remaining stable for
retries of the same observation. SERIES_METADATA_REPORTED therefore includes
its known-at observation time in event identity. Reusing an identity for changed
material remains a fail-loud divergent collision; the projector does not choose
one row silently. Series metadata is a revision observation, not a per-bar
heartbeat: runtime emits it on first series discovery and when routing identity
changes. Stable bars reuse the cached identity and do not consume durable run
sequence allocations.

## Live Handoff Without Execution Authority

Live transport starts after runtime has produced or accepted a fact. The
runtime-to-portal fact stream is viewer-blind: runtime must not inspect BotLens
panes, websocket subscribers, selected symbols, chart state, or any other UI
demand signal. The same run inputs must emit the same canonical facts and
bounded projection/debug facts whether anyone is watching or not.

When runtime owns canonical persistence, including wallet ledger facts, ingest
uses those events for projection and skips duplicate durable writes.
Source-owned canonical facts may arrive over live transport before their async
DB batch has completed, but the run is not finalized until that source-side
buffer drains. Seeing a live message first does not make projection state
canonical.

Producer-side fanout is a bounded projection handoff after sequence assignment.
Execution enqueues the committed live payload and keeps walking forward; the
dispatcher owns websocket/subscriber pressure and drains during terminal runtime
flush. Subscriber failures, queue overflow, or drain timeout degrade BotLens
projection and require resync. Canonical persistence remains fail-loud.

Runtime lifecycle, bootstrap, and shutdown messages use the telemetry control
lane. The control lane is still projection input and remains prioritized.
Accepted general-lane messages drain within the same bounded shutdown deadline
instead of being discarded immediately. Material facts—including
`overlay_ops_emitted`, decisions, trades, and wallet ledger facts—are never
coalesced. Repeated non-material runtime/status facts may still coalesce while
queued. If shutdown cannot drain either lane, runtime marks every undelivered
entry failed and emits lane-specific WARN diagnostics. A failed control flush
may still use the bounded direct websocket fallback.

## Bounded Hot Views

The live websocket stream is a bounded viewport transport, not a replicated
runtime database. Bootstrap snapshots send the latest configured candle window
and bounded debug context. Live updates are delta-only fact batches derived from
backend projections. Older history belongs to cold chart-history and forensic
reads.

The fact stream is compacted before it reaches backend projectors.
`runtime_state_observed` carries compact health/runtime fields, not the full
runtime snapshot. `series_state_observed` carries routing identity only.
`series_stats_updated` carries the compact reportable summary.
`overlay_ops_emitted` carries bounded render overlays with overlay clocks, not
unbounded indicator history. Rolling polyline changes use a typed tail patch:
the patch names the line index, expected point count, dropped prefix, and
appended points, and carries SHA-256 fingerprints for both the previous and
resulting polyline collection. A mismatch fails replay rather than silently
accepting divergent geometry. The first overlay state remains a bounded full
checkpoint; recurring changes avoid retransmitting the entire rolling window.
Wallet ledger and diagnostic facts keep full
canonical payloads on the producer-side canonical append path while live
transport drops repeated wallet snapshots and raw diagnostic context that the
hot view does not need.

Paper market streams may emit `provisional_candle_updated` facts. They update
the selected-symbol chart by replacing the latest visual candle for the
in-progress bar, then clear when the closed candle arrives. They remain Tier 4
live transport only: useful for a responsive chart, irrelevant to indicator,
strategy, wallet, order, trade, report, or replay semantics.

Runtime bootstrap must assemble the selected series directly from runtime
state. It uses the chart state builder for the configured candle window,
projected overlays already in the overlay projection cache, selected series
stats, wallet/debug facts, and a bounded closed-trade tail plus open trades.
Full multi-series chart composition and historical aggregate trade lists belong
to explicit chart/debug reads.

Chart/debug payloads use the same bounded visual-trade contract. Runtime asks
the risk engine for a configured trade window instead of serializing every
trade and slicing afterward. Live trade deltas use `trade_revision`: if the
cursor is too old for the retained change log, runtime warns and emits the
available current trade batch as a projection resync boundary.

## Bounded Visual Overlay Projection

Visual overlays are projection/read-model artifacts. They are not stored on
`StrategySeries`, do not participate in strategy decisions, and are not built by
ordinary runtime push updates. Overlay deltas remain bounded viewport evidence,
but new runs retain those deltas as Tier 2 research context. Chart-history reads
sort retained events by the scoped overlay clock rather than database arrival
order, replay that clock from the beginning of the selected series,
causally stop before the returned page end, and clip the resulting geometry to
the returned candle window. Terminal immutable timelines are held in an
eight-entry process-local LRU so left-pan pages do not requery the same ledger;
each page still performs its own causal time cut and stable fingerprint.

Historical overlay completeness is conditional, never inferred. The page must
have runtime-assigned run-order evidence and contiguous overlay clocks, a
projection window that covers
the returned candles, cadence coverage through the returned last candle, no
payload truncation, and—on the latest terminal page—a terminal checkpoint. A
gap, invalid delta, cadence hole, missing checkpoint, or truncated payload
returns bounded/incomplete evidence with reason codes. Runs created before
overlay retention return `overlay_timeline_not_retained`; the frontend labels
them unavailable instead of substituting a live tail. Frozen candles, typed
decisions, and durable trades remain independently inspectable.

Runtime configures each indicator's render-only overlay-history bound from
`bot_runtime.botlens.overlay_window_bars` through the indicator engine's single
overlay-history dispatcher. This is not a research range, evaluation range,
warmup window, runtime recovery window, or transport replay window. Indicator
typed outputs still follow the normal
`initialize -> apply_bar -> snapshot` runtime timeline. Overlay
geometry is requested only through the overlay projection step, which snapshots
the current indicator visual state without mutating the runtime series model.

The ordinary runtime push update emits compact BotLens facts for candles,
series state, health, decisions, trades, wallet, logs, and stats. After a bar is
finalized, a separate `overlay_projection` step may build visible overlay
geometry, diff it against the overlay projection cache, and emit
`overlay_ops_emitted` only when there are changed overlay operations. A terminal
bar forces one final delta even when geometry is unchanged; that no-op clock
advance is the explicit terminal checkpoint, not invented geometry.

Overlay projection cadence is bar based, not wall-clock based. The cadence is
controlled by `bot_runtime.botlens.overlay_emit_every_bars`, with terminal bars
forcing a final projection. Projection deltas carry a `projection` object with
`mode`, `window_bars`, `emit_every_bars`, `bar_index`, `reason`, and `terminal`
so the backend and frontend can distinguish live bounded state from a complete
ledger-backed page. Transport compaction records whether geometry was truncated
and the source counts; truncated pages cannot be labeled complete.

Trade visuals follow the trade-fact path. Runtime no longer registers or emits
a runtime-owned trade overlay type. The frontend may draw trade markers,
regions, segments, and price lines from projected trade facts, but those are
not indicator overlays and not execution truth.

## Projection Clocks And Resync

Overlay live deltas use their own viewport clock:
`overlay_commit_seq`, `base_overlay_commit_seq`, and
`overlay_commit_seq_status=overlay_scoped`. This clock orders changed overlay
transport operations only. It is separate from the selected-symbol websocket
`base_seq` replay cursor and from durable `run_seq`.

Selected-symbol snapshots carry the current overlay cursor beside the bounded
overlay payload. The next overlay delta must advance that cursor and declare
the matching `base_overlay_commit_seq`; otherwise the frontend treats the delta
as stale projection transport, advances the stream cursor, and does not mutate
overlay state.

Symbol-to-run notifications are live projection transport. A newer pending
notification may replace an older one for the same run and symbol, keeping the
latest symbol/runtime summary and folding trade upsert/removal deltas to their
net state. If notification pressure causes coalescing or overflow, the durable
runtime ledger remains the recovery source.

The durable ingest path follows the same retention budget as source-side
persistence. Source-persisted material facts are not written again by ingest.
Transport-owned facts are persisted only when they are compact research context
or material diagnostics. Bounded `OVERLAY_STATE_CHANGED` deltas are compact
research context: useful for deterministic rendering but never execution,
strategy, report, or research-feature authority. Provisional candles and other
live-only facts continue through projectors and fanout without becoming
permanent runtime-event rows.

A completed run projection that still has open trades must replay from the
durable runtime event ledger before publishing terminal projection truth. Replay
enumerates durable facts through typed `run_seq`, then uses scoped causal clocks
for domain ordering. Position/trade lifecycle facts use
`trade_id, position_commit_seq`, so a durable close tombstone dominates a stale
open notification even when batch arrival crosses append order. Rows without
runtime-assigned `run_seq` are forensic evidence, not certification-grade replay
truth.

BotLens read endpoints may compute selected-symbol continuity diagnostics for a
response, but ordinary reads do not persist observer continuity facts. Durable
observer continuity writes require
`QT_BOT_RUNTIME_BOTLENS_PERSIST_OBSERVER_CONTINUITY=true` and must be labeled
diagnostic/non-material. Report and golden material identity accept terminal
`run_final` continuity evidence from the canonical run path, not BotLens viewer
or bootstrap facts.

## Failure And Recovery

- Missing projector state returns unavailable/projection-error state.
- Stale selected-symbol snapshots are rejected or refreshed.
- Stream continuity uses sequence/cursor fields.
- Rebuild failures surface bounded operational faults.
- Visual overlay projection failure records `overlay_projection` diagnostics
  and degrades BotLens overlay freshness without rewriting execution truth.
- Forensics can page the ledger when live projection is insufficient.
- Symbol fact queue pressure is handled by bounded batch drains first; if
  projection still falls behind, projection state is degraded/resync-required
  rather than promoted into runtime truth.
- Run-notification queue overflow is a projection-health event. The queue may
  coalesce or drop older notifications to keep the latest notification moving,
  but overflow requires canonical ledger replay before projection state can be
  trusted for a golden run.

## Invariants

- BotLens is a debugger, not a demo path.
- Projection and transport payloads stay bounded.
- Heavy event history belongs on cold paths.
- Complete trade markers require ledger-backed range evidence; selected-symbol tails are never presented as complete history.
- Historical indicator overlays are page-complete only when the retained bounded
  timeline proves clocks, cadence, window coverage, terminal state, and no
  truncation; old or gapped runs remain explicitly unavailable/incomplete.
- Runtime truth remains in execution events and trade rows.
- Closed-trade truth in the durable ledger must dominate stale projection
  notifications.

## Related Docs

- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Identity and correlation boundary](../identity/IDENTITY_AND_CORRELATION_BOUNDARY.md)
- [Observability boundary](../observability/OBSERVABILITY_BOUNDARY.md)
