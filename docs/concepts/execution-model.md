# Execution Model

The execution model explains how Bot runtime resolves exits, fills, and playback-visible outcomes. It is runtime behavior, not UI animation behavior.

## What It Is

Execution mode chooses the price path used to resolve exits:

- `FAST`: evaluate exits from the strategy timeframe OHLC only.
- `FULL`: evaluate exits from ordered 1-minute intrabar candles when the strategy timeframe is coarser than 1 minute.

## FAST Mode

FAST mode is intentionally conservative. If a strategy-timeframe bar touches both take profit and stop loss, the stop wins for both long and short trades.

This pessimistic same-bar policy prevents the runtime from assuming a favorable price path that cannot be proven from the coarse candle.

## FULL Mode

FULL mode uses ordered 1-minute intrabar candles to determine whether take profit or stop loss happened first.

If the required 1-minute sequence is missing, incomplete, or ambiguous inside one 1-minute candle, runtime falls back to the FAST pessimistic same-bar policy and emits an `execution_intrabar_fallback_pessimistic` diagnostic with symbol, timeframe, bar time, and reason.

## Playback Separation

Playback mode controls inspection pacing and visualization only. It must not change the execution path, fill ordering, fees, wallet effects, or report metrics.

BotLens can show the selected execution mode and fallback diagnostics, but it does not reinterpret execution after the fact.

## How It Fits

Execution mode flows through bot configuration, run metadata, report artifacts, report payloads, and BotLens diagnostics. Reports and comparisons should identify the mode used before interpreting results.

## Next

- Source of truth: [execution and playback contract](../contracts/platform/02_execution_playback_contract.md).
- Runtime internals: [runtime engine](../engineering/runtime-engine.md).
- Inspection model: [BotLens](botlens.md).
