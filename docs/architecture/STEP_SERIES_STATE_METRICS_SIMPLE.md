# Step Series State Metrics (Plain-English)

## Why this doc exists

This explains what `step_series_state` is, why "Selected Series Bar Latency" can rise even when container snapshot cycle is flat, and what is safe to optimize without changing trading semantics.

## One sentence

`step_series_state` is the total per-bar backend execution step for one `(strategy, symbol, timeframe)` series.

It is not a frontend metric.
It is not the container snapshot cycle metric.

## What your dashboard panel is measuring

In Grafana, "Selected Series Bar Latency Avg / p95 (ms)" is:

- `avg(duration_ms)` / `p95(duration_ms)`
- from `portal_bot_run_steps`
- where `step_name = 'step_series_state'`

So if this rises, bar execution step time is rising.

## Candle-by-candle flow inside `step_series_state`

For each bar, runtime does:

1. Resolve current candle and epoch.
2. Evaluate signals/indicators/overlays (`step_signal_eval`).
3. Run decision + risk flow (`step_decision_flow`).
4. Prime execution / intrabar path (`step_execution_prime`).
5. Apply settlement.
6. Process trade events and runtime logs.
7. Finalize bar:
   - update runtime state,
   - publish stream delta,
   - record profiling traces.
8. Record the final `step_series_state` row (total duration + sub-metrics).

## Key sub-metrics in context

- `overlays_update_ms`: overlay/state update work from signal path.
- `execution_decision_flow_ms`: decision/risk path.
- `execution_prime_ms`: pre-execution/intrabar prime work.
- `execution_settlement_ms`: settlement application.
- `execution_trade_event_processing_ms`: event/log/trade bookkeeping.
- `stream_emit_ms`: runtime delta broadcast enqueue/broadcast time.
- `persistence_ms`: step-trace enqueue time (async trace writer path).
- `step_trace_queue_depth`: profiler queue backlog.
- `step_trace_persist_lag_ms`: how far profiler DB persistence trails enqueue.

## Why latency can rise while container cycle is flat

Container cycle tracks BotLens telemetry orchestration.

`step_series_state` tracks bar execution path.

So you can have:
- flat container telemetry metrics,
- rising per-bar latency,

when bar pipeline work (or bar-path persistence/tracing overhead) increases.

## What changed in this refactor

Step trace DB writes were moved off the hot bar path:

- before: each step trace wrote to DB synchronously.
- now: each trace is enqueued quickly, and a background worker persists in batches.

This preserves execution semantics and removes profiler DB commit latency from per-bar critical path.

Overlay runtime work was also split into explicit timing components:

- `series_overlay_entries_ms`: total time to assemble per-series overlay entries.
- `series_overlay_indicator_entries_ms`: indicator overlay-entry merge time.
- `series_overlay_regime_build_ms`: regime overlay build/reuse time.
- `series_overlay_indicator_entries_count`: indicator entry count in current assembled overlay set.
- `series_overlay_regime_entries_count`: regime entry count in current assembled overlay set.
- `series_overlay_total_entries_count`: total entries in assembled overlay set.
- `series_overlay_regime_mode_rebuild`: `1` when rebuilding regime overlays from visible candles each bar; `0` when reusing static prebuilt regime overlays.

Runtime behavior switch:

- `runtime_regime_overlay_rebuild=false` (default): build regime overlays once at prepare-time, then reuse per bar.
- `runtime_regime_overlay_rebuild=true`: rebuild regime overlays every bar using visible candle window.

Default mode keeps semantics for UI visibility while removing O(window-size) regime overlay rebuild from the hot bar loop.

## Semantics guidance (what is safe vs unsafe)

Safe to make async/batched:
- profiler step traces (`portal_bot_run_steps`)
- UI-oriented stream payload shaping
- latest-view checkpoints used for bootstrap

Do not async/defer if it changes trading truth:
- order/fill lifecycle events that define execution outcomes
- wallet/position state transitions used for risk decisions
- anything consumed by decision logic in the same bar timeline

## Recommendation

Use this policy:

1. Keep decision/execution state transitions synchronous and deterministic.
2. Keep observability and UI materialization async/batched.
3. Track lag explicitly (`queue_depth`, `persist_lag_ms`) so deferred writes remain observable.

That gives performance without compromising "realness."
