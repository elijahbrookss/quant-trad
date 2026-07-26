# Execution & Playback Contract

## Bot Execution Contract

Bot runtime owns:
- order decisions in time
- fills and execution effects
- risk and protection behavior
- execution metrics

## Position Exit Contract

Position exits are runtime execution semantics.

Supported runtime exit policies include:
- target exits: resting take-profit limit fills using maker fee semantics,
- stop exits: stop-market fills using taker fee semantics,
- fixed-horizon exits: close remaining open legs after a configured number of
  completed position bars using market/taker semantics,
- terminal backtest closes: final market/taker close of otherwise open legs.

A strategy decision from candle `t` is known only when candle `t` closes. An
immediate market entry may fill at that close, but the new position must not
use candle `t`'s earlier high/low for a target or stop exit. Normal exit
eligibility begins with the next candle. A terminal backtest close may use the
final candle close without replaying that candle's prior range.

Stop movement must be monotonic:
- stop-to-breakeven and stop-adjustment rules may tighten the stop only,
- trailing stops may activate only from known-at bar evidence and may tighten
  only in the favorable direction,
- no stop policy may loosen protection to keep a trade alive.

Maker/taker classification belongs to execution outcomes. Reports and playback
must consume the runtime-emitted `fee_type`, `fee_rate`, `order_type`, and
`reason_code` fields instead of inferring liquidity from event names.

Slippage remains an explicit modeling gap until paper/live fill evidence is
available for calibration. Runtime must not hide unvalidated slippage
assumptions inside fee or exit summaries.

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
A missing value defaults to `fast`. Every other value is rejected:
`instant`, `walk-forward`, and `walkforward` are playback values, not execution
aliases. Playback configuration must never supply the execution default.

The selected execution mode must flow through:
- persisted bot config,
- run metadata,
- report artifacts and report payloads,
- BotLens run context and diagnostics.

When FULL execution falls back to pessimistic same-bar resolution, runtime must emit `execution_intrabar_fallback_pessimistic` with `symbol`, `timeframe`, `bar_time`, and one normalized reason:
- `missing_1m_data`
- `incomplete_1m_sequence`
- `ambiguous_1m_candle`
