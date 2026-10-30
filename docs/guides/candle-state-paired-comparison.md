# Candle-State Paired Comparison

`raw_forward_outcome@21` is a versioned candle-only Research Check for asking
whether one or more causal public Indicator metrics add forecast information
beyond a configured baseline at ordinary native candle closes.

The procedure is generic. Instrument identity, timeframe, date ranges, elapsed
outcome horizon, baseline windows, public metric parameters, historical-sign
transform, folds, buffers, support screens, effect criteria, and bootstrap
settings are request material. They are not source constants.

## Population and outcomes

- The detector is fixed to `candle_close` / `ordinary_native_close`. It does not
  select Indicator events or threshold crossings.
- Every observed close inside the declared range is an opportunity. Results
  report nominal UTC-grid slots, observed closes, readiness, feature, exact
  future-path, and paired-row counts with exclusion reasons.
- The primary label is up versus non-up at an exact configured elapsed endpoint.
  Exact flat futures remain class zero and are reported separately.
- The complete native path must exist. The method never substitutes the next
  surviving bars across a gap.
- Continuation and endpoint/path magnitude are diagnostics. They do not replace
  the primary direction label.

## Features and public Indicator binding

Baseline features use the existing lagged-return, Wilder ATR-fraction, and
prior-volume-ratio helpers. Enriched features use a typed public Indicator
binding. Version 21 currently admits the public Candle Stats
`candle_stats.directional_efficiency` metric with either identity or a sign
transform derived from a configured historical return. The evaluator executes
the registered Candle Stats runtime; it does not duplicate the metric formula.

At input origin and after each missing or invalid native interval, Indicator and
rolling feature state reset and rewarm. Fold boundaries do not reset causal
state. Delayed source rows cannot contribute until their retained `known_at`
clock is no later than the decision close.

## Paired model and dependence

Baseline and enriched models fit and score exactly the same complete rows.
Feature standardization and model fitting use training rows only. Configured
purge and embargo values are elapsed-time boundaries, and target endpoints stay
strictly inside their nominal train or validation ranges.

The primary uncertainty interval resamples fixed UTC-calendar clusters anchored
to a configured epoch. Sensitivity cluster spans are separate fixed results;
the implementation does not choose the narrower interval. Overlapping outcomes,
shared causal history, and recursive Indicator state remain explicit dependence
limitations. Buffers and cluster intervals do not certify independent trials.

## Execution and evidence

Evidence mode uses the existing requirements, prepare/freeze, Check job, and
replay boundaries. A request must bind exactly one frozen Dataset. The evaluator
fails instead of truncating when it exceeds 19,104 candles, 256 MiB input, 120
seconds, or 16 MiB result output. Configuration, typed Indicator binding,
population, predictions, definition, request, plan, and evidence remain hashed
by the existing canonical Check envelope.

Completion is a forecast screen. It does not grant execution, promotion,
profitability, causal, or scientific-confirmation authority.
