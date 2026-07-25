---
component: candle-stats-signal-contract
subsystem: indicator-runtime
layer: indicator
doc_type: architecture
status: active
tags:
  - indicators
  - candle-stats
  - signals
  - strategy
  - research
code_paths:
  - src/indicators/candle_stats/manifest.py
  - src/indicators/candle_stats/runtime.py
  - tests/test_indicators/test_candle_stats_runtime.py
  - tests/test_strategies/test_strategy_compiler_params.py
---
# Candle Stats Signal Contract

## Purpose

`candle_stats` exposes raw candle-derived metrics and one strategy-visible ATR
expansion signal. The signal exists so ATR expansion can be tested as its own
strategy family instead of only acting as a filter on another indicator family.

## Signal Semantics

The public signal output is `atr_expansion`.

It currently emits one event key:

| Event Key | Direction | Meaning |
| --- | --- | --- |
| `atr_expansion_long` | long | ATR expansion crossed above the configured z-score threshold on this bar. |

This is a threshold-cross event, not a sticky high-ATR state. A bar only emits
`atr_expansion_long` when the previous ATR z-score was not above the threshold
and the current ATR z-score is above it. Bars that remain above the threshold do
not keep emitting the same event.

The default threshold is `atr_expansion_signal_threshold=2.0`, matching the
first promoted research candidate from the ATR expansion study. Stricter
thresholds should be tested by cloning the indicator with a different parameter
instead of adding hard-coded event names.

## Public Metadata

The event metadata may carry explanatory candle-stat facts such as:

- `threshold`
- `atr_zscore`
- `previous_atr_zscore`
- `atr_short`
- `atr_long`
- `atr_ratio`
- `directional_efficiency`
- `slope`
- `volume_zscore`
- `range_position`
- `trigger_price`

These fields are report and research evidence. Strategy rules should consume
the signal event itself first; additional filtering belongs in strategy guards
or strategy variants.

## Scope

The signal is long-only until a short-side study earns a separate short event.
It does not implement delayed entry, retest lifecycle, or confirmation logic.
Those are separate contracts if research later shows they are useful.

## Invariants

- The signal is produced from the same `initialize -> apply_bar -> snapshot`
  indicator timeline as the metric output.
- The signal must be declared in the indicator output catalog.
- Missing declared outputs fail at the engine boundary.
- Research checks and strategies consume the typed output, not private candle
  stats internals.
