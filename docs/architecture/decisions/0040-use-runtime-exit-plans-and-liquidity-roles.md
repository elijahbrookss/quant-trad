---
component: adr-runtime-exit-plans-liquidity-roles
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - runtime
  - execution
  - exits
  - fees
  - maker-taker
  - slippage
code_paths:
  - src/atm/schema.py
  - src/atm/template.py
  - src/engines/bot_runtime/core/execution_policy.py
  - src/engines/bot_runtime/core/domain/engine.py
  - src/engines/bot_runtime/core/domain/position.py
  - src/engines/bot_runtime/core/execution_runtime.py
  - src/engines/bot_runtime/runtime/mixins/runtime_events.py
  - src/engines/bot_runtime/runtime/mixins/execution_loop.py
  - docs/architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md
---
# ADR 0040: Use Runtime Exit Plans and Liquidity Roles

## Status

Accepted on 2026-06-16.

## Context

Research checks can show useful forward-return evidence, but paper/live bot
runs must execute an explicit position lifecycle. A fixed 24-bar forward return
is not equivalent to a bot unless the runtime can express and execute a
fixed-horizon exit. The same applies to trailing stops, stop-to-breakeven
behavior, and fee modeling: they are runtime execution semantics, not generic
strategy-output math.

The previous runtime had market entries, limit-maker entries, target exits,
stops, breakeven movement, and trailing state, but several semantics were
implicit:

- fixed-horizon exits had no first-class runtime policy,
- target exits were charged like taker exits even though they represent
  resting limit orders,
- stop adjustments and breakeven normalization had legacy/nested shape drift,
- trailing config had legacy fields that were not normalized through the main
  ATM template path,
- post-only entry behavior did not reject immediately marketable maker orders.

## Decision

ATM templates expose position lifecycle intent through normalized runtime
policy fields:

- `exit_plan.fixed_horizon` declares a deterministic hold duration in completed
  bars and closes remaining open legs at bar close with a market/taker exit.
- `breakeven` and `stop_adjustments` declare one-time stop movement. Runtime
  consumes both legacy flattened and normalized nested forms.
- `trailing` declares activation and distance inputs. The active stop only
  tightens in the favorable direction.

Bot runtime maps exit events to explicit order and liquidity-role semantics:

- target exits are resting limit exits and use maker fees,
- stop exits are stop-market exits and use taker fees,
- fixed-horizon exits are market closes and use taker fees,
- terminal backtest closes remain market/taker closes.

Limit-maker entries are post-only. If a submitted limit-maker entry would cross
the current reference price immediately, runtime rejects it as
`POST_ONLY_WOULD_CROSS`. Previously resting pending maker entries can still
fill as maker orders when later bars trade through their limit price.

The execution profile remains the fee authority. Templates declare order and
exit intent; they do not patch missing instrument fee fields.

## Slippage Gap

Runtime still has only deterministic slippage hooks. There is not enough
live/paper fill evidence yet to calibrate symbol-, venue-, order-type-, and
regime-aware slippage. Until that evidence exists, slippage must remain an
explicitly disclosed modeling gap rather than hidden "smart" behavior.

Future slippage work should hang off the same execution-policy boundary:
market, stop-market, resting maker, post-only rejection, timeout fallback, and
liquidity role must be known before a slippage model adjusts fill price.

## Consequences

- Research fixed-horizon evidence can be promoted into bot backtests without
  silently changing the exit thesis.
- Maker/taker fee impact is attached to the fill semantics instead of inferred
  from summaries.
- Trailing and breakeven behavior are easier to configure and audit because
  runtime consumes normalized policy shapes.
- Entry timing beyond immediate signal-bar handling remains an explicit future
  runtime feature. A next-bar entry queue should be added as a separate
  lifecycle policy, not faked by changing price anchors.

## References

- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Execution & Playback Contract](../../contracts/platform/02_execution_playback_contract.md)
- [Runtime Contract](../../contracts/platform/01_runtime_contract.md)
