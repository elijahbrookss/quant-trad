---
component: adr-botlens-bounded-hot-durable-inspection
subsystem: botlens-projections
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - botlens
  - frontend
  - websocket
  - pagination
  - memory
  - replay
  - accepted
code_paths:
  - portal/backend/controller/reports.py
  - portal/backend/service/bots/botlens_run_stream.py
  - portal/backend/service/reports/contract.py
  - portal/frontend/src/adapters/bot.adapter.js
  - portal/frontend/src/adapters/report.adapter.js
  - portal/frontend/src/components/bots/BotLensChart.jsx
  - portal/frontend/src/components/bots/chartCameraPolicy.js
  - portal/frontend/src/components/bots/hooks/useMarkerManager.js
  - portal/frontend/src/components/bots/hooks/useTradeMarkers.js
  - portal/frontend/src/components/bots/botlensProjection.js
  - portal/frontend/src/features/bots/botlens
  - src/core/settings.py
  - config/defaults.yaml
---
# ADR 0055: Separate Bounded BotLens Hot State From Durable Inspection

## Status

Accepted on 2026-08-04 after the full-year BIP replay exposed incomplete
inspection counts and unbounded browser-memory risk.

## Context

BotLens has two different jobs. For a running bot it must paint the latest
sequenced state with low latency. For a terminal run it must let an operator
inspect the complete decision, trade, diagnostic, candle, and overlay evidence.
Treating one growing browser array as both contracts made neither trustworthy:
bounded live snapshots looked like complete history, while repeatedly merging
historical pages retained enough candles, markers, and overlay geometry to
crash the frontend on a year-long run.

WebSocket delivery created a second coupling. One browser that stopped reading
could delay fanout to healthy viewers, and one message dispatch per frame could
force a React render for every packet. A live stream is a viewport transport,
not a durable run database, so it needs explicit pressure and resync behavior.

## Decision

BotLens separates three read contracts:

1. **Hot projection.** The selected-symbol bootstrap and live deltas own only
   current operator state. The browser retains at most 320 hot candles and
   applies ordered append/replacement in constant time. Incoming messages are
   reduced once per animation frame in original order. A pending client batch
   is capped at 256 messages and 2 MiB; overflow marks the view stale and
   requests a fresh bootstrap.
2. **Bounded chart window.** Frozen chart history loads 240 bars at a time when
   the viewport approaches the left edge. The browser retains at most 3,840
   candles and 16 overlay pages, and retains only trade states overlapping that
   candle window. Prepending restores the exact prior visible time range and
   never follows latest. Ordinary append follows only while the user remains at
   the live edge. Selecting a decision or trade replaces unrelated chart
   history with a bounded window around its evidence time.
3. **Complete durable indexes.** Terminal decision, completed-trade, and
   diagnostic tabs read independent 100-record pages from typed report
   contracts. Total counts come from those durable contracts, never from hot
   snapshot lengths. The UI retains only the current page, windows mounted
   rows, filters the loaded page, and exposes previous/next navigation. Active
   positions stay in a distinct snapshot section. The diagnostics contract
   remains backward compatible when limit is absent and returns explicit total,
   offset, and limit when paging is requested.
4. **Staged inspection readiness.** Durable terminal inspection loads the chart
   first, then decisions, completed trades, and diagnostics in that order. Each
   section owns independent readiness/error state. Structured evidence lenses
   expose persisted causal and provenance fields; absent inputs, fills,
   slippage, position transitions, and partial exits are labeled unavailable
   rather than inferred.
5. **Causal marker projection.** Entry and exit timestamps map only to an exact
   candle start or the containing normal candle interval. Missing-bar gaps and
   events outside the loaded window produce no marker. Original evidence time
   remains part of marker identity, and the composed marker layer deduplicates
   before rendering. Completed trades never retain active stop/target rays.

Run-stream fanout is concurrent across viewers. Each send has a configurable
deadline, 1,500 ms by default. A slow or failed viewer is evicted without
serially delaying healthy viewers. Its recovery boundary is a new bootstrap,
not replay from another browser's memory.

## Invariants

- Bounded hot or chart state is never labeled complete run history.
- Terminal decision/trade/diagnostic totals come from durable typed datasets.
- The browser does not accumulate every page merely because the operator can
  navigate to it again.
- WebSocket batching preserves message order and therefore all run, concern,
  position, and overlay clocks.
- Buffer overflow, stale cursors, and send timeouts fail visible and trigger
  resynchronization; packets are not silently discarded while the view remains
  labeled live.
- A slow viewer cannot delay a healthy viewer by the slow viewer's timeout.
- Chart-window replacement or eviction changes only presentation memory. It
  cannot change persisted evidence, report fingerprints, or backtest results.
- Automatic left-edge loading stops at the frozen dataset boundary and never
  falls through to mutable provider data.
- A decision or trade focus request is scoped to the exact run and series and
  cannot reuse a response from a prior selection.
- Every chart response is admitted only for the latest request identity; newer
  work aborts superseded requests and identical work is deduplicated.
- Historical prepend preserves the exact visible time range without triggering
  follow-latest behavior.
- Marker projection never snaps across a missing bar or beyond the loaded
  candle range.
- Completed trade evidence and active position state remain visually and
  semantically separate.

## Consequences

Year-long and multi-tab inspection has a fixed browser-memory envelope while
the operator still sees complete counts and can navigate every durable record.
Live rendering performs at most one reducer dispatch per animation frame, and
server fanout isolates slow viewers. Chart navigation feels continuous because
history arrives from viewport movement; there is no second manual loading
control competing with pan/zoom.

The bounded chart is not a miniature full-run database. Crossing an evicted
range causes another deterministic read, and a trade jump discards unrelated
chart geometry. Terminal evidence tabs use page navigation rather than an
infinite scroll. Those are intentional costs of preserving responsiveness and
honest completeness semantics.

## Rejected Alternatives

- Keep every candle, overlay, trade marker, decision, and diagnostic in one
  browser session.
- Treat the selected-symbol snapshot's counts as complete historical totals.
- Paginate candles with a separate manual **Load earlier** button while also
  loading on viewport movement.
- Dispatch every WebSocket packet directly into React state.
- Broadcast to WebSocket viewers serially and let one slow socket delay all
  later viewers.
- Drop excess client packets but continue displaying the stream as healthy.

## Enforcement And Evidence

- Frontend projection/state tests prove hot-candle, chart-candle, overlay-page,
  trade-window, stale-request, and trade-marker bounds plus ordered batch
  reduction.
- Viewport and marker tests prove exact prepend range preservation,
  containing-candle projection, missing-gap rejection, stable event identity,
  and marker deduplication.
- Controller policy tests prove chart-first durable stage readiness and the
  decisions-to-trades-to-diagnostics order.
- View-model tests prove durable totals replace bounded snapshot counts and
  diagnostic grouping ignores volatile timing fields.
- Backend stream tests prove slow-viewer eviction and healthy-viewer progress.
- Report route tests prove optional bounded diagnostic pages.
- Full-year browser/API validation must confirm exact date range, complete
  decision/trade totals, automatic chart history, trade focus, and stable memory
  across multiple simultaneous lenses.

## References

- [Operator Console v2](../frontend/OPERATOR_CONSOLE_V2.md)
- [BotLens Projection Boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Reporting Boundary](../reporting/REPORTING_BOUNDARY.md)
- [ADR 0054: Keep Blocking API Work Off The Event Loop](0054-keep-blocking-api-work-off-the-event-loop.md)
