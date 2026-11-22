---
component: candle-vwap-contract
subsystem: indicator-runtime
layer: indicator
doc_type: architecture
status: active
tags:
  - indicators
  - candles
  - known-at
code_paths:
  - src/indicators/candle_vwap
  - tests/test_indicators/test_candle_vwap_runtime.py
  - tests/test_portal/test_candle_vwap_evidence.py
---
# Candle VWAP v1

`candle_vwap` is a reusable typed market-context Indicator. It estimates a daily
UTC session's volume-weighted price from native candles on the canonical
`initialize -> apply_bar -> snapshot` timeline. It has no dependency on trades,
L2, execution fills, or a trading Strategy. Its distinct type does not reinterpret
the retired, unsupported `vwap` / VWAP Bands catalog or historical instances.

## Calculation and source

For each observed candle, `p = (high + low + close) / 3` (HLC3). Within a session,
`vwap = sum(p * volume) / sum(volume)`. Price has the source instrument's quote
units. `close_distance = close - vwap`; `close_distance_bps = 10000 *
close_distance / vwap`. Accumulators use finite floating-point arithmetic and
constant space. Invalid OHLCV, overflow, and invalid source clocks fail loudly.

This is a **candle-based VWAP approximation**, not exact transaction VWAP.
Coinbase OHLCV supplies candle volume, not exact quote turnover. HLC3 times volume
does not recover transaction turnover. Volume means the provider's reported
candle quantity; on Coinbase spot it is base-asset volume. Other instruments
retain their provider-defined volume units. Do not mix instruments, sources,
volume units, or timeframes within one execution. There is no inferred quote
volume, transaction count, or replacement for absent volume.

The only parameter is `source_timeframe`, default `15m`, bounded to `1m`, `5m`,
`15m`, `30m`, or `1h`. It must equal the runtime's primary candle timeframe.
Preparation must supply and freeze provider-native candles at that timeframe,
with volume declared as a required market-input field. The Indicator does not
resample, acquire, synthesize, or independently certify provider provenance.
Frozen Dataset source identity and normal admission own that evidence. The
runtime verifies aligned interval boundaries and successive source clocks.

## Session, availability and readiness

Sessions are fixed UTC calendar days, `[00:00, next 00:00)`, with no DST or
exchange-calendar interpretation. Session ownership uses the candle open time.
The bar opening at 23:45 on a 15-minute source belongs to the previous session
even though its result becomes available at midnight. The next bar resets all
accumulators and crossing memory before incorporating its own price and volume.

Every declared output exists on every bar. Outputs become ready after an
unbroken session has been observed from its midnight anchor and cumulative
volume is positive. The first positive-volume anchor bar can therefore be ready
at its close; there is no arbitrary lookback. A run starting mid-session emits
`warming_session` with `ready=false` and null VWAP/distance metrics until the
next observed midnight anchor. Load native candles from midnight when earlier
session context is needed. There is no hidden fetch or partial-session estimate.

`bar_time` remains the source candle open, matching the engine contract.
Availability is the source `known_at`, with interval-end fallback for historical
inputs that omit it, following QT's shared candle convention. Source availability
cannot precede close or move backwards. Delayed availability remains delayed;
public context and every emitted event preserve it. Checks apply their existing
known-at cutoff and outcome-price rules. No close-fill or execution claim follows.

Zero-volume candles contribute no price weight. Before any positive volume, all
outputs are unready (`awaiting_volume`). After positive volume, VWAP remains
ready and unchanged on a zero-volume candle; context identifies that bar and
distance still describes its reported close. No crossing is emitted on or across
a zero-volume candle. Missing, negative, boolean or nonfinite volume is invalid,
not equivalent to zero.

The versioned gap contract is `candle_vwap.reject.v1`. Recorded gaps and observed
nonconsecutive, duplicate or reordered source intervals fail. v1 does not support
`reset_rewarm` or `continue_degraded`: neither can repair a missing session
history. A new execution may start later and wait for a complete new UTC anchor.

## Public outputs and Check use

| Output | Type | Meaning |
| --- | --- | --- |
| `vwap_context` | context | State `warming_session`, `awaiting_volume`, `above`, `at`, or `below`; fields identify session bounds/timezone, HLC3 basis, native timeframe, volume semantics, completeness from anchor, observed/positive-volume counts, zero-volume status, close/known-at clocks and gap contract. |
| `vwap_metrics` | metric | `vwap`, `close_distance`, `close_distance_bps`, and `cumulative_volume`. VWAP and distances are null while unready. |
| `vwap_cross` | signal | Current-bar `close_cross_above_vwap` / `close_cross_below_vwap` events with causal identity, availability and public VWAP reference metadata. |

A cross compares consecutive ready, positive-volume bars in the same session:
previous distance `<= 0` and current distance `> 0` emits above; previous distance
`>= 0` and current distance `< 0` emits below. This compares each close with its
own contemporaneous session VWAP. State and crossing comparisons treat relative
price differences within `1e-12` as equality, exposed as
`price_comparison_rel_tol`; metrics retain their unrounded numerical distances.
This fixed v1 numerical tolerance prevents accumulated floating-point roundoff
from turning a constant-price session into crosses. Equality itself emits nothing. The first ready
bar, session reset and zero-volume boundary do not create crosses. Event
directions `long` / `short` are the existing signal-contract labels for above /
below, not recommendations or evidence of profitability.

The normal runtime graph and `indicator_output_evidence.v1` collector expose
these typed outputs; no evaluator reads private state. Existing
`event_fact_analysis` definition 4 can consume `vwap_cross`, with detector keys
`close_cross_above_vwap` mapped to `long` and `close_cross_below_vwap` mapped to
`short`, no Fact inputs, and `gap_policy: reject`. For descriptive forward paths,
the existing definition 6 can use the same detector. Choose an evaluator that
supports the specific research question; typed context does not automatically
add arbitrary context predicates to every Check. Durable runs still require
their existing frozen preparation, binding and replay contracts. No retired
generic Check family is restored.

The implementation declares no overlays or UI changes. Charts may later project
this runtime's outputs through the existing projection contract. Context is
usable independently of crossing events or trading rules.

## Qualification

Focused synthetic tests cover nonuniform volume/HLC3 arithmetic, every admitted
timeframe, UTC resets, partial-session readiness, zero/missing/invalid volume,
gap rejection, source availability, prefix invariance and snapshot isolation.
Integration tests use the actual requirement planner, runtime graph and output
collector, verify page-boundary invariance, and evaluate collected signals using
the existing registered Check evaluator. This establishes implementation
behavior, not an empirical market finding or exact transaction VWAP accuracy.

See the [Indicator runtime boundary](INDICATOR_RUNTIME_BOUNDARY.md) and
[authoring guide](../../guides/creating-an-indicator.md).
