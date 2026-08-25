# Execution & Playback Contract

## Bot Execution Contract

Bot runtime owns:
- order decisions in time
- fills and execution effects
- risk and protection behavior
- execution metrics

## Runtime Mode And External-Submission Authority

Backtest and paper execution use deterministic simulated fills. Paper may also
run `observe-only`; observe-only intake must not create order, fill, trade, fee,
slippage, wallet, or position mutation semantics.

`RuntimeMode.LIVE` is a reserved composition seam. A `live` runtime or lifecycle
label means only that the named runtime seam or lifecycle state is active. It
does not select an external venue adapter or grant order-submission authority.

External order submission, amendment, and cancellation remain closed in every
runtime mode. Provider credentials grant data access only. Configuration,
deployment state, CLI/MCP confirmation, or a mode label cannot open this
boundary. Any future external submission requires a separately accepted
decision, contract amendment, admission controls, and reviewed enforcement.

## Position Exit Contract

Position exits are runtime execution semantics.

Supported runtime exit policies include:
- target exits: resting take-profit limit fills using maker fee semantics,
- stop exits: stop-market fills using taker fee semantics,
- fixed-horizon exits: close remaining open legs after a configured number of
  completed position bars using market/taker semantics,
- terminal backtest closes: final market/taker close of otherwise open legs.

A strategy decision from candle `t` is known only when candle `t` closes. An
immediate market entry may fill at that close. An accepted resting limit-maker
submission originating from candle `t` must not fill from that candle's
already-known high/low range; its range-fill eligibility begins on a later
candle. A position newly opened at candle `t`'s close likewise must not use that
candle's earlier high/low for a target or stop exit. Normal exit eligibility
begins with the next candle. A terminal backtest close may use the final candle
close without replaying that candle's prior range.

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

## Execution-Quality Class Contract

QT uses one execution-quality ladder: X0 through X5. No other X-class value is
valid unless this contract is amended through reviewed normative authority.

A pinned `ExecutionModelArtifact` declares only the maximum class its model may
support. It does not grant the attained class. Reports derive the attained class
from the weakest required context, fee, slippage, rounding, liquidity, fill,
queue, lineage, and limitation evidence actually used by every economically
material execution path. A mixed run receives the minimum attained class unless
a report explicitly partitions claims by class.

| Class | Minimum attained meaning |
| --- | --- |
| `X0` | Legacy, exploratory, missing, or contradictory execution evidence. A causal execution trace may remain inspectable, but no economic, liquidity, venue-performance, or fill-realism claim is supported. |
| `X1` | Valid pinned costed-bar assumptions, configured fee evidence that is not an undisclosed default zero, per-fill fee and adverse market/stop slippage evidence when fills exist, and deterministic cost-stress evidence. Passive behavior and full-fill limitations remain disclosed. |
| `X2` | X1 plus the conservative bar model: adverse market/stop handling, strict passive penetration or an explicit conservative no-fill rule, deterministic same-bar ambiguity handling, and disclosed full-fill limitations. |
| `X3` | X2 plus causal replay-certified L1/top-of-book evidence at simulated arrival, valid bid/ask and spread evidence for every applicable fill, and fills bounded by visible best-level quantity. X3 does not claim depth walking or passive queue realism. |
| `X4` | X3 plus causal replay-certified L2 or L3 input, deterministic aggressive price-level walking, exact per-level fill evidence, partial/residual disposition, and aggregate quantity bounded by eligible visible depth. |
| `X5` | X4 plus an actually exercised named deterministic latency scenario and bounded passive-queue policy, causal book/trade progress evidence, conservative and scenario fill bounds, and explicit aggregated-book and queue-uncertainty limitations. Merely configuring X5-capable artifacts is insufficient. |

Missing or contradictory resolved-context material prevents a class above X0.
Missing higher-class evidence downgrades the result to the strongest lower class
whose complete requirements remain satisfied. In particular:

- missing or invalid X5-only evidence downgrades at most to X4;
- missing X4 depth or per-level evidence downgrades at most to X3;
- missing causal replay, arrival, or spread evidence downgrades at most to X2
  when the complete economic floor remains valid; and
- missing or contradictory economic or context evidence forces X0.

Every report and comparison that exposes execution quality must carry the pinned
model ceiling, attained class, exact context/model references, blocking or
downgrade reasons, and material limitations.

No X class implies venue-realized fill probability, exact queue position,
calibrated live behavior, external-order authority, scientific validity,
reproducibility certification, promotion eligibility, or guarantee activation.

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

FAST and FULL obey the same known-at and signal-bar causality rules, but they use
different price-path resolution and may legitimately produce different fills
and outcomes.

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
