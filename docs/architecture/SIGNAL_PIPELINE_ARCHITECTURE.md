# Signal Pipeline Architecture

## Documentation Header

- `Component`: Signal generation and consumption across QuantLab, Strategy Preview, and Bot Runtime
- `Owner/Domain`: Indicators + Strategies + Bot Runtime
- `Doc Version`: 1.0
- `Related Contracts`: `docs/agents/00_system_contract.md`, `docs/agents/01_runtime_contract.md`, `docs/architecture/INDICATOR_AUTHORING_CONTRACT.md`, `docs/architecture/SNAPSHOT_SEMANTICS_CONTRACT.md`, `docs/architecture/RUNTIME_EVENT_MODEL_V1.md`

## 1) Problem and scope

This document explains how signals are produced and consumed in three shipped paths:
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
- runtime plugins are registered.

## 2) Architecture at a glance

Boundary:
- inside: signal production/evaluation and runtime event emission decisions
- outside: candle provider internals, front-end display, exchange fills

```mermaid
flowchart TD
    A[QuantLab POST /indicators/{id}/signals] --> B[enqueue_signal_job]
    B --> C[quantlab_worker _process_signals]
    C --> D[generate_signals_for_instance]
    D --> E[IndicatorSignalExecutor execute]
    E --> F[signals + runtime_path=engine_snapshot_v1]

    G[Strategy Preview POST /strategies/{id}/signals] --> H[StrategyEvaluationOrchestrator]
    H --> I[generate_indicator_payloads]
    I --> D
    H --> J[rule.evaluate + apply_filter_gates]
    J --> K[buy/sell + chart_markers]

    L[Bot Runtime per bar] --> M[engine.apply_bar -> engine.snapshot]
    M --> N[emit_manifest_signals]
    N --> O[evaluate_rules_from_state_snapshots]
    O --> P[pending StrategySignal queue]
    P --> Q[SIGNAL_EMITTED -> DECISION -> ENTRY/EXIT runtime events]
```

## Mentor Notes (Non-Normative)

- QuantLab and strategy preview share the same indicator signal executor.
- Bot runtime does not call that executor; it evaluates indicator engines inline per bar.
- A signal in preview is a payload object; a signal in bot runtime is a `StrategySignal(epoch, direction)` used for execution.
- Wallet movement events are downstream of decision/execution events, not emitted directly by indicator signals.
- If this conflicts with Strict contract, Strict contract wins.

## 3) Inputs, outputs, and side effects

Inputs:
- QuantLab: `POST /api/indicators/{inst_id}/signals` with `start/end/interval/symbol/datasource/exchange/config`.
- Strategy preview: `POST /api/strategies/{strategy_id}/signals` with `start/end/interval/instrument_ids/config`.
- Bot runtime: each candle in the runtime loop after `start()`.

Dependencies:
- indicator plugin manifest (`engine_factory`, `signal_emitter`),
- strategy rules and filters,
- async job repository for QuantLab,
- runtime event and wallet gateway contracts for bot execution.

Outputs:
- QuantLab: indicator payload with `signals`, `runtime_path`, `runtime_invariants`.
- Strategy preview: per-instrument `indicator_results`, `rule_results`, `buy_signals`, `sell_signals`, `chart_markers`.
- Bot runtime: queued `StrategySignal` objects and runtime events (`SIGNAL_EMITTED`, `DECISION_*`, `ENTRY_FILLED`, `EXIT_FILLED`, wallet projections).

Side effects:
- QuantLab writes async job rows and worker status transitions.
- Strategy preview reads stats snapshots for filter gating and logs per-rule traces.
- Bot runtime persists runtime events and updates wallet projections from emitted events.

## 4) Core components and data flow

QuantLab indicator signals:
- Controller enqueues `JOB_TYPE_SIGNALS` with partition key `datasource|exchange|symbol|interval|inst_id`.
- Worker claims by `created_at` (with partition slot), runs `generate_signals_for_instance`, enforces `runtime_path == engine_snapshot_v1`.
- Response returns once async job reaches `succeeded`; failed/timeout/not-found map to HTTP errors.

Strategy preview signals:
- `generate_strategy_signals` delegates to `StrategyEvaluationOrchestrator`.
- For each instrument, `generate_indicator_payloads` calls the same `generate_signals_for_instance`.
- Preview then evaluates strategy rules and applies filter gates (`global_filters` and `rule_filters`).
- Only rule results with `matched=true` and `final_decision.allowed=true` become buy/sell chart markers.

Bot runtime signals:
- For each bar and each attached indicator: `engine.apply_bar -> engine.snapshot`.
- Snapshot payload is enriched with `emit_manifest_signals`.
- `evaluate_rules_from_state_snapshots` builds `StrategySignal` only when:
  - rule outcome is matched,
  - outcome contains a mapping in `outcome["signal"]`,
  - extracted signal epoch equals current bar epoch.
- Consumed direction triggers `SIGNAL_EMITTED`; decision/execution events follow from risk engine outcomes.

Ordering keys used:
- QuantLab job processing order key: `AsyncJobRecord.created_at` within partition slot.
- Strategy preview signal ordering key: terminal signal epoch selected by rule evaluator.
- Bot runtime evaluation/consumption order key: candle `epoch` (`int(candle.time.timestamp())`) with `last_evaluated_epoch` and `last_consumed_epoch`.
- Bot runtime event stream order key: per-run monotonic `seq`.

## 5) State model

Authoritative state:
- QuantLab: async job row status plus indicator instance metadata.
- Strategy preview: stored strategy/instrument/rule/filter records.
- Bot runtime: in-memory runtime timeline (`SeriesExecutionState`, `RunContext.runtime_events`) and persisted runtime events.

Derived state:
- QuantLab and preview signal payloads from indicator snapshots.
- Preview chart markers and filtered rule decisions.
- Bot runtime pending signal queue, decision trace, wallet projections.

Persistence boundaries:
- Persisted: async jobs, strategy/rule/filter records, bot runtime events, run artifact payloads.
- In-memory only: indicator engine mutable states, per-bar snapshots during current evaluation, pending signal deque.

## 6) Why this architecture

- QuantLab and strategy preview share one indicator signal execution path to reduce indicator semantic drift.
- Bot runtime computes incrementally per bar so execution decisions respect walk-forward timing.
- Runtime events are append-only and correlated (`root_id`, `parent_id`, `correlation_id`) so downstream wallet and trade views can be reconstructed deterministically.

## 7) Tradeoffs

- Shared indicator executor for preview/QuantLab improves consistency, but bot runtime still has a separate per-bar rule-evaluation path.
- QuantLab async jobs improve worker isolation, but add queue latency and at-least-once processing behavior.
- Strategy preview applies filter gates; bot runtime snapshot rule path does not apply that gate layer.
- Bot runtime signal emission depends on rule payload shape (`matched` plus terminal `signal` mapping), which is strict and brittle to malformed rule evaluators.

## 8) Risks accepted

- Current bot runtime rule source is `series.meta["rules"]`; if missing, runtime emits no strategy signals.
- `Strategy.to_dict()` currently omits `rules`, while series metadata is built from `strategy.to_dict()`. This creates a shipped risk of empty rule evaluation in bot runtime.
- QuantLab async retries can re-run a job; no payload-level dedupe key is enforced in the signal executor.
- Strategy preview may return partial indicator payload errors (`{"error": ...}`) while still evaluating other indicators.

## 9) Strict contract

Non-negotiable invariants:
- QuantLab and strategy preview indicator signals must pass `runtime_path == engine_snapshot_v1`; mismatch fails loud.
- Indicator runtime signal creation must use engine snapshots and manifest dispatch (`initialize -> apply_bar -> snapshot -> emit_manifest_signals`).
- Bot runtime can emit execution-driving signals only for current bar epoch (`signal_epoch == current_epoch`).
- `SIGNAL_EMITTED` is the causal parent for `DECISION_*`; `DECISION_ACCEPTED` is parent for `ENTRY_FILLED`; entry/decision chain parents `EXIT_FILLED`.

Failure behavior:
- QuantLab: async job failures surface as HTTP error responses; no silent fallback.
- Strategy preview: missing instruments/invalid interval/invalid strategy shape fail loud; per-indicator exceptions are surfaced in payload error fields.
- Bot runtime: runtime exceptions emit `RUNTIME_ERROR` and can halt run (or degrade symbol when configured).

Retry and idempotency semantics:
- QuantLab signal jobs are at-least-once (enqueue with `max_attempts=2`, retry state, stale-running reclaim). Exactly-once is not guaranteed.
- Strategy preview request execution has no internal retry contract in the orchestrator path.
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
- Tests: `tests/test_portal/test_indicator_runtime_contract.py`, `tests/test_portal/test_quantlab_worker_runtime_contract.py`, `tests/test_bot_runtime_snapshot_signal_runtime.py`.
- Logs: `indicator_signal_runtime_*`, `strategy_signal_preview_*`, `bot_runtime_*` and runtime event logs.
- Storage: async job status rows and persisted bot runtime event rows with `seq`.

## 10) Versioning and compatibility

- Indicator signal runtime path version is `engine_snapshot_v1`.
- Indicator snapshot contract currently uses schema version `1` (`INDICATOR_SNAPSHOT_SCHEMA_VERSION`).
- Runtime events currently use schema version `1` (`SCHEMA_VERSION` in runtime event model).
- Compatibility rule: additive fields are safe by default; breaking changes require explicit version changes and consumer updates.
