---
component: signal-pipeline-architecture
subsystem: signals
layer: service
doc_type: architecture
status: active
tags:
  - signals
  - pipeline
code_paths:
  - src/engines/indicator_engine
  - src/strategies
  - src/overlays
  - portal/backend/service/indicators/indicator_service
  - portal/backend/service/strategies
---
# Signal Pipeline Architecture

## Documentation Header

- `Component`: Signal generation and consumption across QuantLab, Strategy Preview, and Bot Runtime
- `Owner/Domain`: Indicators + Strategies + Bot Runtime
- `Doc Version`: 1.0
- `Related Contracts`: [[00_system_contract]], [[01_runtime_contract]], [[INDICATOR_AUTHORING_CONTRACT]], [[RUNTIME_EVENT_MODEL_V1]], [[BOT_RUNTIME_ENGINE_ARCHITECTURE]]

## 1) Problem and scope

This document explains how strategy-driving signals are produced and consumed in three shipped paths:
- QuantLab indicator signal API,
- strategy preview signal API,
- bot runtime per-bar execution.

In scope:
- signal pipeline shape and boundaries,
- ordering/causality keys used for correctness,
- where the pipelines are shared and where they diverge,
- how signal emission affects decision and wallet runtime events.

### Non-goals

- indicator authoring internals (covered by indicator authoring contract),
- UI rendering behavior (chart marker drawing and front-end display),
- exchange/order execution adapter internals beyond signal-driven event triggers.

Upstream assumptions:
- indicator instances, strategy rules, and instrument records exist,
- candle data can be loaded for requested windows,
- runtime indicator manifests are registered.

## 2) Architecture at a glance

Boundary:
- inside: signal production/evaluation and runtime event emission decisions
- outside: candle provider internals, front-end display, exchange fills

```mermaid
flowchart TD
    A[QuantLab POST /indicators/{id}/signals] --> B[enqueue_signal_job]
    B --> C[indicator_worker _process_signals]
    C --> D[generate_signals_for_instance]
    D --> E[IndicatorSignalExecutor execute]
    E --> F[signal output previews + runtime_path=engine_snapshot_v1]

    G[Strategy Preview POST /strategies/{id}/preview] --> H[evaluate_strategy_preview]
    H --> I[IndicatorExecutionEngine.step]
    I --> J[typed outputs + overlays]
    J --> K[evaluate_typed_rules]
    K --> L[trigger_rows + canonical overlays]

    M[Bot Runtime per bar] --> N[IndicatorExecutionEngine.step]
    N --> O[typed output map + overlay frame]
    O --> P[evaluate_typed_rules]
    P --> Q[pending StrategySignal queue]
    Q --> R[SIGNAL_EMITTED -> DECISION -> ENTRY/EXIT runtime events]
```

## Mentor Notes (Non-Normative)

- QuantLab still has its own indicator signal executor for research requests.
- Strategy preview and bot runtime now share the same typed-output indicator engine and typed rule evaluator.
- A trigger in preview is a row derived from typed `signal` outputs; a signal in bot runtime is a `StrategySignal(epoch, direction)` used for execution.
- Wallet movement events are downstream of decision/execution events, not emitted directly by indicator signals.
- If this conflicts with Strict contract, Strict contract wins.

## 3) Inputs, outputs, and side effects

Inputs:
- QuantLab: `POST /api/indicators/{inst_id}/signals` with `start/end/interval/config` and required `instrument_id` (plus optional display-oriented `symbol/datasource/exchange` context).
- Strategy preview: `POST /api/strategies/{strategy_id}/preview` with `start/end/interval/instrument_ids/config`.
- Bot runtime: each candle in the runtime loop after `start()`.

Dependencies:
- runtime indicator manifest (`outputs`, `dependencies`),
- strategy rules,
- async job repository for QuantLab,
- runtime event and wallet gateway contracts for bot execution.

Outputs:
- QuantLab: indicator payload with flattened signal preview rows derived from canonical `signal` outputs, standard chart `overlays`, `runtime_path`, and `runtime_invariants`.
- Strategy preview: per-instrument `trigger_rows` and canonical `overlays`.
- Bot runtime: queued `StrategySignal` objects and runtime events (`SIGNAL_EMITTED`, `DECISION_*`, `ENTRY_FILLED`, `EXIT_FILLED`, wallet projections).

Side effects:
- QuantLab writes async job rows and worker status transitions.
- QuantLab signal display uses standard overlay-contract artifacts; no client-only signal render path exists.
- Strategy preview replays the typed indicator engine over the requested candle window and emits no separate overlay projection path.
- Bot runtime persists runtime events and updates wallet projections from emitted events.

## 4) Core components and data flow

QuantLab indicator signals:
- Controller enqueues `JOB_TYPE_SIGNALS` with partition key `datasource|exchange|symbol|instrument_id|interval|inst_id`.
- Before enqueue, the controller computes an exact request fingerprint from indicator revision plus market window/config and reuses either:
  - an in-flight matching async job, or
  - a recent succeeded result within the QuantLab result-cache TTL.
- Shared indicator workers claim signal and stats jobs from the same async queue pool by `created_at` (with partition slot), run `generate_signals_for_instance` for signal jobs, and enforce `runtime_path == engine_snapshot_v1`.
- `IndicatorSignalExecutor` loads candles through the canonical candle service using the same instrument-aware `DataContext` semantics as runtime graph construction and strategy preview.
- QuantLab signal adaptation does not publish a separate top-level signal contract module. It flattens occurrences from canonical indicator `signal` outputs into research preview rows, derives `event_time`, `timeframe_seconds`, and canonical `series_key` from walk-forward execution context, requires `instrument_id`, and preserves signal-output fields like `event_key`, optional `pattern_id`, and `known_at`.
- QuantLab signal responses emit standard overlay entries in the same response path (`type=indicator_signal`, `source=signal`) so chart rendering stays on the shared overlay contract rather than a separate client projection.
- Response returns once async job reaches `succeeded`; failed/timeout/not-found map to HTTP errors.

Strategy preview signals:
- `run_strategy_preview` delegates to `evaluate_strategy_preview`.
- For each instrument, preview builds runtime indicators, executes candles sequentially through `IndicatorExecutionEngine`, and evaluates rules with `evaluate_typed_rules`.
- Preview returns two downstream-facing products from the same engine timeline:
  - `trigger_rows` for rule inspection with `strategy_rule_id`,
  - `overlays` emitted directly from indicator `overlay_snapshot()` payloads, including the canonical `strategy_signal` overlay for preview markers.
- Preview does not fetch indicator overlays through a separate overlay service path.

Bot runtime signals:
- For each bar and each attached indicator: `IndicatorExecutionEngine.step(...)` executes `apply_bar -> snapshot -> overlay_snapshot`.
- The runtime consumes the flattened typed output map only.
- `evaluate_typed_rules` builds `StrategySignal` only when:
  - a `signal_match` node matches the current-bar signal output,
  - the enclosing rule resolves true,
  - the resulting action maps to the current bar epoch.
- Consumed direction triggers `SIGNAL_EMITTED`; decision/execution events follow from risk engine outcomes.

Ordering keys used:
- QuantLab job processing order key: `AsyncJobRecord.created_at` within partition slot.
- Strategy preview signal ordering key: terminal signal epoch selected by rule evaluator.
- Strategy preview overlay ordering key: final frame emitted by deterministic walk-forward execution over the requested bars.
- Bot runtime evaluation/consumption order key: candle `epoch` (`int(candle.time.timestamp())`) with `last_evaluated_epoch` and `last_consumed_epoch`.
- Bot runtime event stream order key: per-run monotonic `seq`.

## 5) State model

Authoritative state:
- QuantLab: async job row status plus indicator instance metadata.
- Strategy preview: stored strategy/instrument/rule/filter records.
- Bot runtime: in-memory runtime timeline (`SeriesExecutionState`, `RunContext.runtime_events`) and persisted runtime events.

Derived state:
- QuantLab and preview signal payloads from indicator snapshots.
- Preview trigger rows and overlays from the same typed indicator walk-forward execution.
- Bot runtime pending signal queue, decision trace, wallet projections.
- QuantLab request/result reuse is derived from async job rows keyed by exact request fingerprint; it is not a separate candle cache and does not affect bot runtime fetch semantics.

Persistence boundaries:
- Persisted: async jobs, strategy/rule/filter records, bot runtime events, run artifact payloads.
- In-memory only: indicator engine mutable states, per-bar snapshots during current evaluation, pending signal deque.

## 6) Why this architecture

- Strategy preview and bot runtime share one typed-output indicator execution path to reduce strategy/runtime semantic drift.
- Bot runtime computes incrementally per bar so execution decisions respect walk-forward timing.
- Runtime events are append-only and correlated (`root_id`, `parent_id`, `correlation_id`) so downstream wallet and trade views can be reconstructed deterministically.

## 7) Tradeoffs

- QuantLab still uses its own research executor, while strategy preview and bot runtime share the typed-output path.
- QuantLab async jobs improve worker isolation, but add queue latency and at-least-once processing behavior.
- Strategy preview now mirrors bot runtime rule semantics instead of applying a separate filter gate layer.
- Bot runtime signal emission depends on declared typed outputs and typed rule nodes, which is strict by design and fails fast on malformed output references or type mismatches.

## 8) Risks accepted

- Current bot runtime rule source is `series.meta["rules"]`; if missing, runtime emits no strategy signals.
- `Strategy.to_dict()` currently omits `rules`, while series metadata is built from `strategy.to_dict()`. This creates a shipped risk of empty rule evaluation in bot runtime.
- QuantLab still has a distinct research endpoint, but it now derives from the same indicator output contract instead of a separate `src/signals` package.

## 9) Strict contract

Non-negotiable invariants:
- QuantLab and strategy preview indicator signals must pass `runtime_path == engine_snapshot_v1`; mismatch fails loud.
- Indicator runtime strategy-driving signals must come from typed `signal` outputs published by the indicator execution engine.
- Bot runtime can emit execution-driving signals only for current bar epoch (`signal_epoch == current_epoch`).
- `SIGNAL_EMITTED` is the causal parent for `DECISION_*`; `DECISION_ACCEPTED` is parent for `ENTRY_FILLED`; entry/decision chain parents `EXIT_FILLED`.

Failure behavior:
- QuantLab: async job failures surface as HTTP error responses; no silent fallback.
- Strategy preview: missing instruments/invalid interval/invalid strategy shape fail loud; per-indicator exceptions are surfaced in payload error fields.
- Bot runtime: runtime exceptions emit `RUNTIME_ERROR` and can halt run (or degrade symbol when configured).

Retry and idempotency semantics:
- QuantLab signal jobs are at-least-once (enqueue with `max_attempts=2`, retry state, stale-running reclaim). Exactly-once is not guaranteed.
- QuantLab dedupes exact concurrent/recent requests at the async-job boundary by request fingerprint, but duplicate execution is still possible after TTL expiry or retry races.
- Strategy preview request execution has no internal retry contract.
- Bot runtime signal consumption is idempotent within a run by `last_consumed_epoch`; event persistence is append-only with monotonic `seq`, but cross-process exactly-once is not guaranteed.

Degrade state machine:
- `RUNNING`: per-bar signal evaluation and event emission active.
- `DEGRADED`: symbol-level degraded execution path (when degradation is enabled) with runtime warning/event.
- `HALTED`: runtime enters error/stopped terminal state; no further per-bar signal evaluation for halted work.

In-flight behavior:
- Entering `DEGRADED` stops normal processing for the failed series while other active series may continue.
- Entering `HALTED` stops normal signal/event progression for the run.

Sim vs live differences:
- QuantLab and strategy preview: no mode distinction in signal contract.
- Bot runtime: backtest/paper/live differ in candle feed and pacing, but signal extraction contract and runtime event taxonomy are unchanged.

Canonical error codes/reasons:
- Indicator/preview/QuantLab path emits runtime mismatch reasons such as `runtime_path_mismatch` and scope/contract validation failures.
- Bot runtime uses `ReasonCode` values including `SIGNAL_STRATEGY_SIGNAL`, `DECISION_REJECTED_*`, `EXEC_ENTRY_FILLED`, `EXEC_EXIT_*`, `RUNTIME_EXCEPTION`, `RUNTIME_PARENT_MISSING`, `SYMBOL_DEGRADED`.

Validation hooks:
- Tests: `tests/test_portal/test_indicator_runtime_contract.py`, `tests/test_portal/test_indicator_worker_runtime_contract.py`, `tests/test_bot_runtime_snapshot_signal_runtime.py`.
- Logs: `indicator_signal_runtime_*`, `strategy_signal_preview_*`, `bot_runtime_*` and runtime event logs.
- Storage: async job status rows and persisted bot runtime event rows with `seq`.

## 10) Versioning and compatibility

- Indicator signal runtime path version is `engine_snapshot_v1`.
- Indicator snapshot contract currently uses schema version `1` (`INDICATOR_SNAPSHOT_SCHEMA_VERSION`).
- Runtime events currently use schema version `1` (`SCHEMA_VERSION` in runtime event model).
- Compatibility rule: additive fields are safe by default; breaking changes require explicit version changes and consumer updates.
