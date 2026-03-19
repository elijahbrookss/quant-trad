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
  - src/indicators/regime
  - src/engines/indicator_engine/runtime_engine.py
  - src/strategies/evaluator.py
  - portal/backend/service/market/regime_stats_service.py
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

- Regime classification has two valid product surfaces now:
  - QuantLab / reporting path: DB-backed `candle_stats`, `regime_stats`, and `regime_blocks`.
  - Bot runtime path: `RegimeIndicator` publishes typed context outputs and optional overlays on the canonical runtime timeline.
- New candles trigger async stats jobs.
- Jobs compute `candle_stats` first, then `regime_stats` (versioned).
- Strategies consume `regime.market_regime` as a typed context output rather than a separate filter runtime.
- Bot runtime overlays come from indicator-owned overlay snapshots, not a separate runtime-derived cache.
- Overlay visibility is still time-gated by `current_epoch` and `known_at`.

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
  - [engine.py](/home/elijah/dev/quant-trad/src/indicators/regime/engine.py:24)
  - [stabilizer.py](/home/elijah/dev/quant-trad/src/indicators/regime/stabilizer.py:37)
  - [config.py](/home/elijah/dev/quant-trad/src/indicators/regime/config.py:7)

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

### A) Strategy/Decision Path

- The bot runtime executes `RegimeIndicator` in dependency order after its declared dependencies are ready.
- Strategies read the published typed output `regime.market_regime`.
- `context_match` rules compare `state_key` and optional fields directly from that output.
- Key code:
  - [runtime.py](/home/elijah/dev/quant-trad/src/indicators/regime/runtime.py)
  - [runtime_engine.py](/home/elijah/dev/quant-trad/src/engines/indicator_engine/runtime_engine.py)
  - [evaluator.py](/home/elijah/dev/quant-trad/src/strategies/evaluator.py)

### B) BotLens Overlay Path (view layer)

- `RegimeIndicator` may emit normalized canonical overlay payloads from `overlay_snapshot()`.
- BotLens consumes those canonical overlays and applies visibility, trimming, delta, and transport only.
- Key code:
  - [runtime.py](/home/elijah/dev/quant-trad/src/indicators/regime/runtime.py)
  - [state_streaming.py](/home/elijah/dev/quant-trad/src/engines/bot_runtime/runtime/mixins/state_streaming.py)
  - [chart_state.py](/home/elijah/dev/quant-trad/src/engines/bot_runtime/runtime/components/chart_state.py)

## Runtime Overlay Semantics

Regime overlays are no longer a separate rebuild contract in the core runtime.

- overlay meaning originates from the indicator,
- overlay payloads are already normalized when emitted,
- runtime/BotLens only decides visibility at the current epoch.

This preserves the single execution timeline:

- `apply_bar(...)`
- `snapshot()`
- `overlay_snapshot()`

## Known Tradeoffs

- Async stats pipeline can create short freshness lag for newest candle regime rows in research/reporting contexts.
- Bot runtime avoids that lag by computing regime on the same typed-output timeline as strategy decisions.
- Recompute range + upsert is robust/idempotent but can be heavy under very high ingest throughput.
- Larger confirmation/min-block settings improve stability but delay visible state transitions.

## Practical Guidance

- For runtime correctness:
  - treat `regime.market_regime` as the strategy truth surface.
- For reporting/export:
  - read `regime_stats` / `regime_blocks` from persisted tables.
- For visuals:
  - keep regime overlays indicator-owned and downstream-only after emission.

## Debug Checklist

If regime looks stale or wrong:

1. Confirm stats jobs are running and succeeding.
   - `stats_worker_job_succeeded` in backend logs.
2. Verify `regime_stats` row exists for `(instrument_id, timeframe, candle_time, regime_version)`.
3. Check typed runtime output for `regime.market_regime` on the affected bar.
4. Inspect `known_at` on regime blocks if transitions seem delayed.
5. Confirm overlay visibility trimming vs current epoch.
6. Confirm the indicator emitted a ready regime overlay on that bar if the issue is visual only.
