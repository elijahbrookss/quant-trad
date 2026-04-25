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
  - portal/backend/service/observability.py
  - portal/backend/service/bots/botlens_contract.py
  - portal/backend/service/bots/botlens_canonical_facts.py
  - portal/backend/service/bots/botlens_domain_events.py
  - portal/backend/service/bots/botlens_runtime_state.py
  - portal/backend/service/bots/botlens_intake_router.py
  - portal/backend/service/bots/botlens_mailbox.py
  - portal/backend/service/bots/botlens_projector_registry.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/botlens_state.py
  - portal/backend/service/bots/botlens_transport.py
  - portal/backend/service/bots/botlens_event_replay.py
  - portal/backend/service/bots/botlens_chart_service.py
  - portal/backend/service/bots/botlens_forensics_service.py
  - portal/backend/service/bots/botlens_retrieval_queries.py
  - portal/backend/service/bots/botlens_bootstrap_service.py
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_symbol_service.py
  - portal/backend/service/bots/botlens_run_stream.py
  - portal/backend/service/bots/botlens_lifecycle_bridge.py
  - portal/backend/service/bots/telemetry_stream.py
  - portal/backend/service/observability_exporter.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/storage/repos/observability.py
  - portal/frontend/src/components/bots/BotLensChart.jsx
  - portal/frontend/src/components/bots/useBotStream.js
  - portal/frontend/src/components/bots/botlensProjection.js
  - portal/frontend/src/components/bots/chartDataUtils.js
  - portal/frontend/src/features/bots/diagnostics/BotDiagnosticsView.jsx
  - portal/frontend/src/features/bots/diagnostics/buildBotDiagnosticsViewModel.js
  - portal/frontend/src/features/bots/page/BotsPageView.jsx
  - portal/frontend/src/features/bots/page/useBotsPageController.js
  - portal/frontend/src/features/bots/page/components/BotsFleetPanel.jsx
  - portal/frontend/src/features/bots/page/components/BotsRuntimeWorkspace.jsx
  - portal/frontend/src/features/bots/botlens/BotLensRuntimeContainer.jsx
  - portal/frontend/src/features/bots/botlens/BotLensRuntimeView.jsx
  - portal/frontend/src/features/bots/botlens/buildBotLensRuntimeViewModel.js
  - portal/frontend/src/features/bots/botlens/hooks/useBotLensController.js
  - portal/frontend/src/features/bots/botlens/hooks/useBotLensLiveTransport.js
  - portal/frontend/src/features/bots/botlens/state/botlensRuntimeSelectors.js
  - src/engines/indicator_engine/runtime_engine.py
  - src/engines/bot_runtime/runtime/mixins/execution_loop.py
  - src/engines/bot_runtime/runtime/mixins/runtime_events.py
  - src/engines/bot_runtime/runtime/mixins/runtime_projection.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - src/engines/bot_runtime/runtime/components/canonical_facts.py
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

### 1. Durable domain truth

Durable BotLens truth is `botlens_domain.*` only.

The runtime appends committed compact domain rows before transport for the
producer-owned BotLens facts. The intake router converts remaining bridge facts
and lifecycle payloads into immutable domain events and persists those rows once.

Projection fanout now follows ownership:

- symbol fact/bootstrap batches go only to symbol projectors,
- lifecycle batches go only to the run projector,
- and symbol projectors emit compact run notifications after symbol projection
  and symbol-state persistence complete.

Projectors do not project raw bridge facts, websocket payloads, bootstrap DTOs,
or HTTP detail shapes.

For active runs, the projector registry also tails newly committed durable
series domain rows after durable `RUN_READY` or terminal lifecycle truth is
visible. This is a recovery feed for runtime-fact bridge backpressure: if the
bridge misses bulky live fact batches, symbol projectors still advance from the
committed ledger rows without waiting for a new bootstrap or frontend poll.

### 2. Run projection state

Run projection is always-hot state for the active run.

It is composed from bounded concern-owned slices:

- lifecycle,
- runtime health,
- recent faults,
- run symbol catalog,
- and the run-level open trades index.

Runtime health is the current BotLens truth surface for:
- `runtime_state`,
- `progress_state`,
- `last_useful_progress_at`,
- degraded start/clear facts,
- churn detection,
- summarized pressure snapshots,
- recent runtime transition history,
- terminal actor/reason classification,
- canonical active warning conditions with stable `warning_id`, `count`,
  `first_seen_at`, and `last_seen_at`,
- and persisted runtime-state transition rejections surfaced through the lifecycle diagnostics trail.

Runtime health transport is coalesced at the runtime edge:
- repeated warning-count and `last_seen_at` churn do not emit a fresh
  `runtime_state_observed` fact on every candle,
- semantic condition-set changes still emit immediately,
- and a bounded heartbeat refresh preserves operator visibility without tying
  run health cadence to candle cadence.

Run projection stays in memory for active runs.

The legacy DB-backed live view cache has been removed from the architecture.
Run health, websocket continuity, active bootstrap, and inactive reconstruction
all resolve from in-memory projector state plus the append-only event ledger.

Run projection does not consume raw symbol fact traffic directly.

It consumes:

- immutable lifecycle batches from the dedicated run lifecycle mailbox, which
  must fail loud on overflow rather than shed terminal truth, and
- compact symbol-summary notifications from symbol projectors through the
  run-notification mailbox.

### 3. Symbol projection state

Each symbol has its own projection state.

It is composed from bounded concern-owned slices:

- series identity,
- recent candles,
- current overlays,
- recent signals,
- recent decisions,
- recent trades,
- recent diagnostics,
- and current series stats.

Run health is not symbol-owned state. Transport composes it into symbol
snapshot/detail payloads when needed.

Bootstrap and recovery guardrails:
- startup bootstrap is valid only while run runtime state is still in startup,
- that admission rule is shared across container runtime, intake routing, run projection, and symbol projection so ordering between those layers does not change legality,
- projector scope reset is startup-only,
- post-live transport gaps move runtime state to degraded instead of back to startup,
- and post-live bootstrap attempts are rejected at container, ingest, and projector boundaries.

Symbol projection also stays in memory for active runs.

### 4. Historical retrieval

Historical retrieval is separate from projection and separate from live transport.

BotLens now exposes two historical categories:

- chart retrieval:
  - range-based
  - symbol-scoped
  - owned by `botlens_chart_service.py`
- forensic retrieval:
  - cold-path truth inspection and causal-chain reads
  - owned by `botlens_forensics_service.py`

These reads do not:

- reconnect the run websocket,
- mutate unrelated symbols,
- reuse projector state,
- reuse websocket/bootstrap/detail DTOs,
- or consume durable `runtime.*` rows as a fallback truth path.

## Transport Model

BotLens live delivery is one websocket per run.

The live websocket is transport-only:

- it emits only transport-owned live DTOs,
- it emits only current concern deltas,
- it does not emit selected-symbol snapshot payloads,
- it does not replay chart/detail/bootstrap payloads,
- and it does not carry retrieval/history slices.

Run-scope live contracts are:

- `botlens_run_lifecycle_delta`
- `botlens_run_health_delta`
- `botlens_run_fault_delta`
- `botlens_run_symbol_catalog_delta`
- `botlens_run_open_trades_delta`

Symbol-scope live contracts are:

- `botlens_symbol_candle_delta`
- `botlens_symbol_overlay_delta`
- `botlens_symbol_signal_delta`
- `botlens_symbol_decision_delta`
- `botlens_symbol_diagnostic_delta`
- `botlens_symbol_trade_delta`
- `botlens_symbol_stats_delta`

Every live message carries explicit continuity metadata:

- `stream_session_id`
- `stream_seq`
- `scope`
- `concern`
- `scope_seq`
- explicit `run_id`
- explicit `symbol_key` for symbol-scoped deltas

`stream_seq` is the monotonic transport ordering for one live stream session.
`scope_seq` is the projector timeline sequence for the run or symbol concern.

Symbol switching stays on one websocket, and the standard selected-symbol snapshot is projector-backed:

1. keep the run websocket open,
2. read the selected-symbol snapshot from canonical run/symbol projector state when the client needs a base state,
3. send `set_selected_symbol` on the existing websocket with the selected-symbol snapshot `base_seq` as `resume_from_seq` plus `stream_session_id`,
4. the server updates the viewer subscription and replays symbol deltas with `stream_seq > resume_from_seq` from the bounded replay ring,
5. the client commits the selected-symbol snapshot only if it is still current, rejects stale bootstraps, and applies only initialized-symbol deltas.

Live transport does not compensate for missing base state by replaying or inventing a selected-symbol base on the websocket.
It only replays post-cursor deltas for the newly selected symbol. If the cursor
cannot be proven because the replay ring expired or the stream session rolled,
the server emits `botlens_live_reset_required` and the client reboots the run
snapshot contract.
Overlays no longer ride the per-bar websocket lane. The selected-symbol snapshot
is read from active projector state and then maintained by websocket deltas,
including overlay deltas for the subscribed symbol.
Polling is not a live architecture dependency.
Reconnect uses `resume_from_seq` against a bounded in-memory replay window; if
that window has expired the server emits `botlens_live_reset_required` and the
client must perform a fresh bootstrap.

## Backend Ownership

Current ownership split:

- worker runtime emits BotLens bridge fact batches per symbol for projection transport only,
- worker runtime appends committed compact BotLens domain rows before transport
  for producer-owned candle, overlay, stats, signal, decision, trade, diagnostic,
  and runtime-health facts,
- worker runtime also aggregates bounded indicator-guard warnings into the runtime snapshot before BotLens transport,
- `container_runtime.py` owns worker supervision and bridge metadata,
- `telemetry_stream.py` is the public coordinator for ingest and viewer session calls,
- `botlens_intake_router.py` owns ingest validation, BotLens domain-event construction, and single-write domain persistence,
- `botlens_mailbox.py` owns bounded queue/slot semantics for symbol facts, run lifecycle, and run notifications,
- `botlens_symbol_projector.py` owns symbol-level concern projection from immutable BotLens domain batches,
- `botlens_symbol_projector.py` also emits compact symbol-summary notifications for run projection after symbol state advances in memory,
- `botlens_run_projector.py` owns run-level concern projection from immutable lifecycle batches plus symbol-summary notifications,
- `botlens_projector_registry.py` owns projector lifecycle, durable ledger tailing for active runs, and run fanout delivery loops,
- `botlens_run_stream.py` owns run-scoped websocket fanout and transport sequencing,
- `botlens_state.py` owns concern-owned projector state, reset rules, bounded windows, and persistence serialization,
- `botlens_transport.py` owns outward bootstrap, HTTP detail, symbol catalog, and live websocket delta contracts,
- `observability.py` owns the backend observability substrate used across those seams,
- `botlens_bootstrap_service.py` owns run bootstrap reads,
- `botlens_symbol_service.py` owns standard selected-symbol snapshot reads, explicit symbol detail reads, and symbol catalog reads,
- `botlens_chart_service.py` owns chart-history retrieval,
- `botlens_forensics_service.py` owns event-truth and causal-chain retrieval,
- `botlens_retrieval_queries.py` owns internal domain-row traversal for retrieval services.

The runtime still emits symbol-scoped domain facts.
The runtime does not emit UI-shaped multi-symbol projection blobs.

## Contracts

BotLens uses explicit boundary-owned contracts:

1. Run bootstrap:
   - run metadata
   - lifecycle
   - health
   - grouped runtime warnings
   - run navigation catalog
   - open trades
   - deterministic selected symbol
2. Selected-symbol snapshot:
   - one symbol only
   - standard BotLens symbol-view state from projector snapshots
   - candles, overlays, signals, decisions, recent trades, logs, runtime health, and stats
   - explicit selected-symbol scope and bootstrap sequencing metadata
3. Symbol detail snapshot:
   - one symbol only
   - explicit debugger/detail path
   - may reconstruct bounded historical state when that path is requested on purpose
4. Run live deltas:
   - `botlens_run_lifecycle_delta`
   - `botlens_run_health_delta`
   - `botlens_run_fault_delta`
   - `botlens_run_symbol_catalog_delta`
   - `botlens_run_open_trades_delta`
5. Symbol live deltas:
- `botlens_symbol_candle_delta`
- `botlens_symbol_overlay_delta`
- `botlens_symbol_signal_delta`
   - `botlens_symbol_decision_delta`
   - `botlens_symbol_diagnostic_delta`
   - `botlens_symbol_trade_delta`
   - `botlens_symbol_stats_delta`
6. Chart history page:
   - one symbol only
   - range-based
   - chart-owned candles
7. Forensic event page:
   - run-scoped
   - cursor-based truth inspection
   - forensic-owned truth wrappers
8. Signal forensics:
   - one signal only
   - causal-chain truth documents

Contract ownership is boundary-specific:

- persisted symbol and run projection state is projector/storage-owned and is not returned directly to the frontend,
- run bootstrap, selected-symbol snapshot, HTTP symbol detail, and live websocket deltas are separate transport-owned DTOs even when fields overlap,
- chart/forensic retrieval DTOs are separate retrieval-owned contracts even when fields overlap with transport or each other,
- projector concern deltas are internal projector output and are mapped by `botlens_transport.py` into outbound websocket contracts,
- run projection slices are mapped into bootstrap and websocket payloads instead of being serialized as pass-through transport shapes.

These contracts are narrow and typed so the frontend does not normalize a giant cross-symbol projection blob on every update, and live transport no longer depends on a composite summary payload or per-bar symbol visual payload.

Durable domain rows are also closed on write:

- `runtime_events.py` validates every `botlens_domain.*` payload against the BotLens domain event contract before persistence,
- malformed diagnostic/fault blobs are rejected instead of being stored and ignored later,
- and legacy flat view models such as `DecisionLedgerEvent` are not part of the durable BotLens truth path.

## Default Symbol Selection

Default symbol selection is deterministic.

Order:

1. symbol with an open trade,
2. otherwise most recently active symbol,
3. otherwise stable symbol/timeframe/key ordering.

No hidden first-row fallback is allowed.

## Memory And Lifecycle

BotLens now uses explicit bounded ownership:

- bounded grouped runtime warning list,
- bounded recent fault list,
- bounded per-symbol candle windows,
- bounded overlay set,
- bounded recent trade/log/decision tails,
- bounded recent signal tails,
- bounded frontend selected-symbol snapshot cache,
- run cache eviction after inactivity,
- faster eviction for terminal runs.

BotLens does not retain:

- unbounded full-world snapshots per symbol,
- per-series websocket fanout trees,
- or forever-growing in-memory dicts without a cleanup path.

## Persistence Semantics

Durable storage remains split between:

- append-only BotLens domain events in `portal_bot_run_events`.

Hot-read storage posture:

- typed ledger columns are the only canonical filtered read surface for `event_name`, `series_key`, `correlation_id`, `root_id`, `bar_time`, `instrument_id`, `symbol`, `timeframe`, `signal_id`, `decision_id`, `trade_id`, and `reason_code`,
- selected-symbol replay uses the typed `(bot_id, run_id, series_key, seq, id)` access path,
- chart history uses the typed candle-window access path scoped to `event_name = 'CANDLE_OBSERVED'`,
- and the legacy payload `series_key` expression index is retired rather than kept as a long-term bridge.

BotLens domain validation rules are part of the read contract:

- `CANDLE_OBSERVED`, `SIGNAL_EMITTED`, `DECISION_EMITTED`, `TRADE_OPENED`, `TRADE_UPDATED`, and `TRADE_CLOSED` are series-scoped facts and BotLens reads reject any persisted row for those event types that does not carry a canonical `series_key`,
- `DECISION_EMITTED.context.decision_state` is a closed enum with only `accepted` and `rejected` as valid persisted values,
- rejected decisions are invalid unless both `reason_code` and `message` are present during construction, persistence decode, projection, and detail reads,
- `signal_id` and `decision_id` are distinct identities in the BotLens contract and read paths must not alias one to the other when reconstructing signal detail or related-event links,
- malformed candle rows fail at the BotLens domain boundary with stable field-level validation errors for missing/non-finite OHLC values instead of leaking float-conversion errors,
- and `HEALTH_STATUS_REPORTED.context.trigger_event` is the only canonical persisted name for the inner runtime/lifecycle cause; BotLens read paths reject the legacy inner `event` alias.

Indicator guard warnings flow through the same run summary path instead of a side channel:

- the runtime snapshot carries grouped warning rows,
- the BotLens summary health payload persists that bounded list plus `warning_count`, `warning_types`, and `highest_warning_severity`,
- and the frontend renders that warning state in an explicit current-state panel instead of hiding it behind retrieval or chart-only affordances.

Durable heavy-family payload boundaries are now explicit:

- `OVERLAY_STATE_CHANGED` persists bounded renderable overlay truth:
  - `overlay_id`, `type`, `strategy_id`, `source`, `pane_key`, `pane_views`, `detail_level=bounded_render`, `overlay_revision`, `payload`, and `payload_summary` with counts/point totals,
  - render geometry arrays such as `polylines[].points[]` are capped by the BotLens overlay point budget before they enter the hot runtime-event row,
  - active selected-symbol projection and historical replay both reconstruct overlays from persisted domain rows when bridge delivery lags or overflows.
- `HEALTH_STATUS_REPORTED` persists compact runtime health truth:
  - `status`, worker counts, `runtime_state`, `progress_state`, `last_useful_progress_at`, bounded warnings, `warning_types`, `highest_warning_severity`, and compact degraded/churn/pressure/terminal summaries,
  - repeated warning `context` bodies and expanded pressure/terminal detail do not persist hot.
- `SERIES_STATS_REPORTED` persists only compact top-level RCA metrics such as trade counts, win/loss rates, PnL totals, drawdown, and quote currency.
- `CANDLE_OBSERVED` persists irreducible OHLCV candle truth only; repeated wrapper detail such as `end`, `atr`, and `range` is not part of the durable contract.

Important rule:

- durable BotLens domain events and runtime events remain the authoritative replay sources for their own domains,
- overlays remain render contracts,
- canonical live symbol deltas carry only overlay delta ops for the selected symbol, while durable rows retain bounded geometry needed for recovery,
- selected-symbol live state reads overlays from the in-memory symbol projector snapshot through the selected-symbol snapshot contract and then advances from websocket deltas,
- active live projection may tail bounded committed ledger rows when bridge fact
  delivery lags or overflows, with event-id dedupe preventing double application
  when bridge and ledger delivery both arrive,
- and live execution never reads BotLens projections back into the runtime timeline.
- forensic pagination must use the stable persisted-row cursor `(seq, row_id)` because multiple domain rows may legitimately share the same run `seq`,
- chart retrieval remains range-based and does not page through projector memory or websocket snapshot payloads,
- and retrieval filters apply to the filtered result stream before page slicing, so cursor advancement and the next page boundary are defined by returned rows rather than skipped rows and cannot produce empty pages while later filtered matches still exist.

Backend observability is now a separate contract:

- queue pressure, payload size, delivery latency, and persistence timing are emitted through the shared backend observability substrate,
- the shared sink is drained into durable Postgres observability tables by a bounded exporter worker,
- structured operational events are emitted alongside metrics for transitions, anomalies, and recoveries,
- hot-path typed-delta INFO logs are removed,
- and `portal_bot_run_events` no longer persists transport fact batches, bridge metadata, or `typed_delta_metrics` summaries as BotLens truth.

See `BOTLENS_BACKEND_OBSERVABILITY_CONTRACT.md`, `BOTLENS_OBSERVABILITY_PERSISTENCE_ARCHITECTURE.md`, and `BOTLENS_OBSERVABILITY_MIGRATION_CHECKLIST.md` for the backend-only observability seam model, durable storage/query surfaces, and the manual schema cleanup plan.

## Frontend Store

The frontend uses one normalized run store:

- `runMeta`
- `lifecycle`
- `health`
- `symbolIndex`
- `openTradesIndex`
- `symbolStates`
- `symbolStateOrder`
- `selectedSymbolKey`

Read rules:

- symbol selector reads `symbolIndex` only,
- live trades panel reads `openTradesIndex` only,
- chart/readout reads `symbolStates[selectedSymbolKey]` only,
- symbol switching mutates `selectedSymbolKey` locally,
- selected-symbol snapshot cache misses fetch one symbol snapshot independently,
- selected-symbol rendering is composed from snapshot and live-slice state:
  - metadata
  - candles
  - overlays
  - recent trades
  - decisions
  - signals
  - logs
  - runtime
  - stats
- live selected-symbol evolution applies typed reducers by concern instead of mutating one composite detail delta object,
- and live selected-symbol hydration starts only after an explicit selected-symbol snapshot read instead of a server-side replay handoff.

Frontend composition stays feature-owned:

- `src/features/bots/page/*` owns the Bots page shell, fleet selection surface, and runtime workspace selection,
- `src/features/bots/botlens/*` owns runtime orchestration, view-model shaping, and runtime panel layout,
- shared `src/components/bots/*` components remain presentational primitives such as the chart, decision table, overlay toggles, and trade chips,
- current-state panels render only from run-store selectors and selected-symbol snapshot/live slices,
- retrieval panels render only from retrieval cache selectors,
- and the frontend keeps one inline runtime workspace instead of a modal-local parallel BotLens path.

### Bots Fleet Ownership

The Bots fleet surface now has one automatic runtime owner:

- the `/api/bots/stream` SSE feed is the only automatic fleet-state source during normal runtime,
- the first fleet render waits for the stream snapshot instead of bootstrapping from an automatic HTTP list call,
- start/stop/create/delete success paths do not auto-refetch the fleet over HTTP,
- explicit user refresh is the only HTTP fleet resync path,
- and manual HTTP refresh replaces fleet state intentionally while the SSE stream stays connected.

## Why This Matters

This design keeps BotLens trustworthy under multi-symbol live runs.

It prevents:

- one symbol interfering with another,
- symbol switch reconnect churn,
- full-screen blanking on selection changes,
- and hidden fallback behavior in the inspection surface.

BotLens remains explainability infrastructure, not transport glue.
