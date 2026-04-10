---
component: botlens-debugger-architecture
subsystem: portal-runtime
layer: reference
doc_type: architecture
status: active
tags:
  - runtime
  - botlens
  - debugger
  - read-model
  - playback
code_paths:
  - portal/backend/service/bots/botlens_contract.py
  - portal/backend/service/bots/botlens_state.py
  - portal/backend/service/bots/botlens_session_service.py
  - portal/backend/service/bots/botlens_symbol_service.py
  - portal/backend/service/bots/botlens_run_stream.py
  - portal/backend/service/bots/botlens_lifecycle_bridge.py
  - portal/backend/service/bots/telemetry_stream.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/frontend/src/components/bots/BotLensLiveModal.jsx
  - portal/frontend/src/components/bots/BotLensChart.jsx
  - portal/frontend/src/components/bots/botlensProjection.js
  - portal/frontend/src/components/bots/chartDataUtils.js
  - src/engines/indicator_engine/runtime_engine.py
  - src/engines/bot_runtime/runtime/mixins/execution_loop.py
  - src/engines/bot_runtime/runtime/mixins/runtime_events.py
  - src/engines/bot_runtime/runtime/mixins/runtime_projection.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
---
# BotLens Runtime Inspection Architecture

## Purpose

BotLens is the run-scoped runtime inspection surface for a bot run.

It exists to expose one coherent run story:

- run lifecycle and health,
- symbol-level runtime state,
- open trades across the run,
- symbol detail for chart/debug surfaces,
- and the durable runtime event ledger behind those views.

BotLens is not a second execution engine and not a demo transport.

## Core Position

BotLens must stay aligned with the platform contracts:

- runtime facts are emitted on the canonical `initialize -> apply_bar -> snapshot` timeline,
- the backend owns read-model projection and transport semantics,
- the frontend owns presentation and bounded client caching only,
- and no BotLens surface may invent alternate execution meaning.

If runtime and BotLens disagree, runtime semantics are source of truth.

## Architecture Shape

BotLens now has four explicit state layers.

### 1. Run summary state

Run summary is always-hot state for the active run.

It contains:

- run metadata,
- lifecycle,
- health,
- grouped runtime warnings,
- symbol summaries for every active symbol,
- and the run-level open trades index.

Run summary is persisted in `portal_bot_run_view_state` with `series_key=__run__`.

### 2. Run-level open trades index

Open trades are modeled once per run, not inside the selected chart symbol.

This index powers:

- the live trades panel,
- run-wide trade visibility,
- and trade-driven symbol pivots.

Open trades are derived from the same runtime trade facts as symbol detail.

### 3. Symbol detail state

Each symbol has its own detail snapshot.

It contains only what the symbol detail surface needs:

- recent candles,
- current overlays,
- recent trades for that symbol,
- logs,
- decisions,
- stats,
- runtime/detail continuity status.

Symbol detail is persisted per canonical `instrument_id|timeframe` key.

### 4. Symbol history pages

Older history is fetched independently per symbol from durable runtime events.

History paging does not:

- reconnect the run websocket,
- mutate unrelated symbols,
- or require full run bootstrap.

## Transport Model

BotLens live delivery is now one websocket per run.

The live session carries:

- run summary deltas,
- open trades deltas,
- symbol detail deltas for selected or cached hot symbols.

The websocket does not change when the user switches symbols.

Symbol switching now works as:

1. update `selectedSymbolKey` locally in the client store,
2. keep the run websocket alive,
3. tell the websocket which symbol is selected/hot,
4. fetch symbol detail independently if the cache does not already have it.

This eliminates the old per-series reconnect/bootstrap churn.

## Backend Ownership

Current ownership split:

- worker runtime emits canonical BotLens bridge fact batches per symbol,
- worker runtime also aggregates bounded indicator-guard warnings into the runtime snapshot before BotLens transport,
- `container_runtime.py` owns worker supervision and bridge metadata,
- `telemetry_stream.py` owns canonical BotLens read-model projection,
- `botlens_run_stream.py` owns run-scoped websocket fanout and bounded replay,
- `botlens_session_service.py` owns run bootstrap reads,
- `botlens_symbol_service.py` owns symbol detail and symbol history reads.

The runtime still emits symbol-scoped domain facts.
The runtime does not emit UI-shaped multi-symbol projection blobs.

## Contracts

BotLens uses six explicit contracts:

1. Run session bootstrap:
   - run metadata
   - lifecycle
   - health
   - grouped runtime warnings
   - symbol summaries
   - open trades
   - deterministic selected symbol
   - selected symbol detail snapshot
2. Run summary delta:
   - health updates
   - grouped runtime warning updates
   - lifecycle updates
   - symbol summary upserts/removals
3. Open trades delta:
   - trade upserts
   - trade removals
4. Symbol detail snapshot:
   - one symbol only
5. Symbol detail delta:
   - candle/runtime/stats/overlay/trade/log/decision changes for one symbol only
6. Symbol history page:
   - one symbol only
   - paginated candles

These contracts are narrow and typed so the frontend does not normalize a giant cross-symbol projection blob on every update.

## Default Symbol Selection

Default symbol selection is deterministic.

Order:

1. symbol with an open trade,
2. otherwise most recently active symbol,
3. otherwise stable symbol/timeframe/key ordering.

No hidden first-row fallback is allowed.

## Memory And Lifecycle

BotLens now uses explicit bounded ownership:

- bounded run replay ring,
- bounded grouped runtime warning list,
- bounded per-symbol candle windows,
- bounded recent trade/log/decision tails,
- bounded frontend detail cache,
- run cache eviction after inactivity,
- faster eviction for terminal runs.

BotLens does not retain:

- unbounded full-world snapshots per symbol,
- per-series websocket fanout trees,
- or forever-growing in-memory dicts without a cleanup path.

## Persistence Semantics

Durable storage remains split between:

- append-only raw BotLens runtime/lifecycle facts in `portal_bot_run_events`,
- latest run summary snapshot in `portal_bot_run_view_state` with `series_key=__run__`,
- latest symbol detail snapshots in `portal_bot_run_view_state` keyed by canonical symbol key.

Indicator guard warnings flow through the same run summary path instead of a side channel:

- the runtime snapshot carries grouped warning rows,
- the BotLens summary health payload persists that bounded list plus `warning_count`,
- and the frontend renders it as a compact collapsed-by-default warning indicator with an expandable panel.

Important rule:

- durable runtime events remain the authoritative replay source,
- latest BotLens view rows are caches for bootstrap/read performance,
- and live execution never reads BotLens projections back into the runtime timeline.

## Frontend Store

The frontend uses one normalized run store:

- `runMeta`
- `lifecycle`
- `health`
- `symbolIndex`
- `openTradesIndex`
- `detailCache`
- `detailCacheOrder`
- `selectedSymbolKey`

Read rules:

- symbol selector reads `symbolIndex` only,
- live trades panel reads `openTradesIndex` only,
- chart/detail reads `detailCache[selectedSymbolKey]` only,
- symbol switching mutates `selectedSymbolKey` locally,
- cache misses fetch one symbol detail snapshot independently.

## Why This Matters

This design keeps BotLens trustworthy under multi-symbol live runs.

It prevents:

- one symbol interfering with another,
- symbol switch reconnect churn,
- full-screen blanking on selection changes,
- and hidden fallback behavior in the inspection surface.

BotLens remains explainability infrastructure, not transport glue.
