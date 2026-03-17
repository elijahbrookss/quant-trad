---
component: regime-classification-system
subsystem: signals
layer: domain
doc_type: architecture
status: active
tags:
  - regime
  - classification
code_paths:
  - src/engines/bot_runtime/strategy/regime_overlay.py
  - src/engines/bot_runtime/strategy/series_builder_parts/overlays_regime.py
  - portal/backend/service/bots/runtime_derived_state.py
---
# Regime Classification System

## Purpose

This document explains how regime classification works end-to-end in Quant-Trad:

- how each candle gets a regime,
- where that regime is stored,
- how strategy/runtime consume it,
- why runtime overlay rebuild is not required for correctness in backtest mode.

Audience: high/mid-level engineers who need implementation-level clarity.

## TL;DR

- Regime classification has two valid ownership paths now:
  - QuantLab / strategy-preview path: DB-backed async `candle_stats` and `regime_stats`.
  - Bot runtime path: runtime-local derivation from the in-memory candle timeline.
- New candles trigger async stats jobs.
- Jobs compute `candle_stats` first, then `regime_stats` (versioned).
- Strategy-preview/runtime filter code can still use persisted `regime_stats`.
- Bot runtime overlays and entry-context metrics use runtime-local derived state, not DB rows.
- Overlay geometry can be prebuilt and still be accurate because visibility is time-gated by `current_epoch` and `known_at`.

## Core Components

### 1) Ingest and Job Trigger

- Candles are persisted into `candles_raw`.
- Ingest enqueues a `stats_compute` job for the affected time range.
- Key code:
  - [persistence.py](/home/elijah/dev/quant-trad/portal/backend/service/providers/persistence.py:551)
  - [stats_queue.py](/home/elijah/dev/quant-trad/portal/backend/service/market/stats_queue.py:61)
  - [run_backend.py](/home/elijah/dev/quant-trad/portal/backend/run_backend.py:86)

### 2) Candle Stats Compute

- Worker computes deterministic candle features (`atr_zscore`, `tr_pct`, `directional_efficiency`, `overlap_pct`, volume stats, etc.).
- Uses lookback window to avoid edge artifacts (`LOOKBACK_BARS=200`).
- Upserts into `candle_stats` (versioned by `stats_version`).
- Key code:
  - [candle_stats_service.py](/home/elijah/dev/quant-trad/portal/backend/service/market/candle_stats_service.py:33)

### 3) Regime Classification Compute

- `RegimeStatsService` reads candles + candle_stats for the range.
- `RegimeEngineV1` produces raw axis states:
  - `structure`, `volatility`, `expansion`, `liquidity`.
- `RegimeStabilizer` applies:
  - confidence gates,
  - confirmation bar counts,
  - hysteresis,
  - optional smoothing per feature axis.
- Final stabilized regime is upserted into `regime_stats` (versioned by `regime_version`).
- Key code:
  - [regime_stats_service.py](/home/elijah/dev/quant-trad/portal/backend/service/market/regime_stats_service.py:34)
  - [regime_engine.py](/home/elijah/dev/quant-trad/portal/backend/service/market/regime_engine.py:22)
  - [regime_stabilizer.py](/home/elijah/dev/quant-trad/portal/backend/service/market/regime_stabilizer.py:37)
  - [regime_config.py](/home/elijah/dev/quant-trad/portal/backend/service/market/regime_config.py:8)

### 4) Regime Blocks and Confirmation Timing

- Regime points are grouped into blocks (`build_regime_blocks`).
- `min_block_bars` merges short flips/noise into neighboring blocks.
- Each block has `known_at`:
  - `known_at = start_time + timeframe_seconds * (min_block_bars - 1)`.
- This encodes when that block can be considered confirmed.
- Key code:
  - [regime_blocks.py](/home/elijah/dev/quant-trad/portal/backend/service/market/regime_blocks.py:47)
  - [regime_blocks.py](/home/elijah/dev/quant-trad/portal/backend/service/market/regime_blocks.py:187)

## Data Model

Relevant persistent tables:

- `candles_raw`
- `candle_stats` (`stats_version`, JSON `stats`)
- `regime_stats` (`regime_version`, JSON `regime`)
- `regime_blocks` (block summary JSON)

Schema is managed in:
- [persistence.py](/home/elijah/dev/quant-trad/portal/backend/service/providers/persistence.py:38)

Version contract:
- [stats_contract.py](/home/elijah/dev/quant-trad/portal/backend/service/market/stats_contract.py:8)

## "Does each new candle get a regime?"

Yes, with async semantics:

1. New candle persists.
2. Stats job enqueued.
3. Worker computes candle stats + regime stats for the requested range.
4. `regime_stats` row exists for that candle time.

Important nuance:

- Because this is async, newest regime rows may lag candle ingest briefly under load.
- In live mode, consumers can observe that delay.
- This is eventual consistency, not semantic inaccuracy.

## Runtime Consumption Paths

### A) Strategy/Decision Path (when filters use regime)

- Strategy filter runtime builds a `StatsSnapshot`.
- For each signal, it resolves candle time and reads `regime_stats` by time/version.
- Filter DSL can gate decisions on regime fields.
- Key code:
  - [filter_runtime.py](/home/elijah/dev/quant-trad/portal/backend/service/strategies/strategy_service/filter_runtime.py:17)
  - [filters.py](/home/elijah/dev/quant-trad/portal/backend/service/strategies/strategy_service/filters.py:72)

### B) BotLens Overlay Path (view layer)

- Runtime derives stats/regime from the worker's in-memory candle timeline, then builds overlay payloads from that runtime-local state.
- Overlays include boxes/segments/markers and block metadata without depending on async `regime_stats` table freshness.
- Key code:
  - [runtime_derived_state.py](/home/elijah/dev/quant-trad/portal/backend/service/bots/runtime_derived_state.py)
  - [overlays_regime.py](/home/elijah/dev/quant-trad/src/engines/bot_runtime/strategy/series_builder_parts/overlays_regime.py)
  - [regime_overlay.py](/home/elijah/dev/quant-trad/src/engines/bot_runtime/strategy/regime_overlay.py)

## Why Prebuilt Overlays Can Still Be Correct

In backtest/walk-forward mode, we can prebuild regime overlays once and reuse them because rendering still enforces time visibility.

Visibility enforcement happens at render shaping:

- overlays are trimmed to `current_epoch`,
- entries with `known_at > current_epoch` are hidden,
- future x2 endpoints are clipped to current time where needed.

Key code:
- [chart_state.py](/home/elijah/dev/quant-trad/src/engines/bot_runtime/runtime/components/chart_state.py:52)
- [chart_state.py](/home/elijah/dev/quant-trad/src/engines/bot_runtime/runtime/components/chart_state.py:202)
- [chart_state.py](/home/elijah/dev/quant-trad/src/engines/bot_runtime/runtime/components/chart_state.py:313)

So:

- prebuilding geometry != showing future information.
- correctness comes from `known_at/current_epoch` gating, not from recomputing geometry every bar.

## When Rebuild Is Actually Needed

Use per-bar/periodic rebuild only when underlying regime rows can change during the run timeline:

- live/sim modes with appended candles,
- when the runtime-local candle window changes and you need immediate overlay refresh.

In those cases, rebuild should be event-driven by new regime data availability, not unconditional every bar.

## Runtime Modes and the Current Refactor

Current runtime switch:

- `runtime_regime_overlay_rebuild=false` (default): prebuild once at prepare-time.
- `runtime_regime_overlay_rebuild=true`: rebuild from visible candles in bar loop.

This switch affects overlay compute cost, not classifier semantics.

Key code:
- [setup_prepare.py](/home/elijah/dev/quant-trad/src/engines/bot_runtime/runtime/mixins/setup_prepare.py:234)
- [setup_prepare.py](/home/elijah/dev/quant-trad/src/engines/bot_runtime/runtime/mixins/setup_prepare.py:875)

## Known Tradeoffs

- Async stats pipeline can create short freshness lag for newest candle regime in live contexts.
- Runtime-local derivation duplicates some research-time computation, but preserves the single runtime timeline and avoids async lag leaking into execution semantics.
- Recompute range + upsert is robust/idempotent but can be heavy under very high ingest throughput.
- Larger confirmation/min-block settings improve stability but delay visible state transitions.

## Practical Guidance

- For backtest/walk-forward playback correctness and speed:
  - keep `runtime_regime_overlay_rebuild=false`.
- For live "as-soon-as-available" regime visuals:
  - keep async stats workers healthy,
  - prefer targeted overlay refresh on new regime rows,
  - avoid unconditional full-window rebuild every bar.

## Debug Checklist

If regime looks stale or wrong:

1. Confirm stats jobs are running and succeeding.
   - `stats_worker_job_succeeded` in backend logs.
2. Verify `regime_stats` row exists for `(instrument_id, timeframe, candle_time, regime_version)`.
3. Check strategy filter snapshot time alignment (signal epoch floored to timeframe).
4. Inspect `known_at` on regime blocks if transitions seem delayed.
5. Confirm overlay visibility trimming vs current epoch.
6. Check runtime mode:
   - prebuilt static vs rebuild.
