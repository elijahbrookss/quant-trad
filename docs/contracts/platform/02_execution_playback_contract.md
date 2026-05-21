# Execution & Playback Contract

## Bot Execution Contract

Bot runtime owns:
- order decisions in time
- fills and execution effects
- risk and protection behavior
- execution metrics

## Playback Contract

Playback is an audit/debug surface for execution semantics.
It should make visible:
- what was known
- what decision was made
- what execution outcome occurred

## Alignment Rule

Playback views should be derivable from runtime state transitions.
When visualization and runtime disagree, runtime semantics are source of truth.

## Execution Mode Policy

Execution mode is a runtime semantics choice and must not be inferred from UI playback or animation speed.

Supported execution modes:
- `FAST`: evaluate exits from the strategy timeframe OHLC only.
- `FULL`: evaluate exits from ordered 1-minute intrabar candles when the strategy timeframe is coarser than 1 minute.

FAST same-bar rule:
- if a long bar hits both take profit and stop, the stop wins,
- if a short bar hits both take profit and stop, the stop wins.

FULL intrabar rule:
- ordered 1-minute candles determine whether take profit or stop occurred first,
- if 1-minute data is missing or incomplete, runtime falls back to the FAST pessimistic same-bar policy and logs a warning,
- if one 1-minute candle hits both take profit and stop, runtime falls back to the FAST pessimistic same-bar policy and logs a warning.

Playback mode controls pacing/debug visualization only. It must not change which price path is used to resolve execution.

## Contract Exposure

Bot configuration must expose `execution_mode` as `fast` or `full`.

The selected execution mode must flow through:
- persisted bot config,
- run metadata,
- report artifacts and report payloads,
- BotLens run context and diagnostics.

When FULL execution falls back to pessimistic same-bar resolution, runtime must emit `execution_intrabar_fallback_pessimistic` with `symbol`, `timeframe`, `bar_time`, and one normalized reason:
- `missing_1m_data`
- `incomplete_1m_sequence`
- `ambiguous_1m_candle`
