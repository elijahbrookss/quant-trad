# Candle-State Paired Comparison

`raw_forward_outcome@21`, `raw_forward_outcome@22`,
`raw_forward_outcome@23`, and `raw_forward_outcome@24` are versioned candle-only
Research Checks for paired forecast comparisons at ordinary native candle
closes. Version 21 asks whether causal public Indicator metrics add information
beyond a typed candle baseline. Version 22 also admits the existing typed candle
features on either side, including an Indicator-free enriched side, and retains
an outcome-blind causal selection audit. Version 23 adds the existing identity
and historical-return-sign transforms to those typed candle features. Version
24 scores one declared candle target and a fold-frozen forecaster against both
strictly historical rolling-global and rolling-calendar forecasts. Versions
21 through 23 remain unchanged and executable for their recorded requests and
evidence.

The procedure is generic. Instrument identity, timeframe, date ranges, elapsed
outcome horizon, baseline windows, public metric parameters, historical-sign
transform, folds, buffers, support screens, effect criteria, and bootstrap
settings are request material. They are not source constants.

Version 24 additionally makes the target, target-specific forecaster, rolling
history window and support, calendar bins, benchmark priors, loss, and criteria
request material. Its current target operators are terminal up versus non-up
and the sum of squared native log returns over a complete configured future
path. The latter is sampled variance at the source interval; it is neither the
absolute endpoint return nor the square root of that sum.

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
prior-volume-ratio helpers. Version 22 permits those same typed features in the
enriched arm, so volume or another declared candle-state feature can be compared
without fabricating a market Fact or requiring an Indicator. It also permits a
typed public Indicator binding in the enriched arm. Version 23 allows a typed
candle feature to retain its identity or multiply by the sign of a configured
past close change. The sign requires every native candle from the decision back
through its configured lookback, all known by the decision; a missing or late
intermediate candle makes the transformed feature unavailable. An exact flat
past change maps to zero. Version 21 currently admits
the public Candle Stats
`candle_stats.directional_efficiency` metric with either identity or a sign
transform derived from a configured historical return. The evaluator executes
the registered Candle Stats runtime; it does not duplicate the metric formula.

At input origin and after each missing or invalid native interval, Indicator and
rolling feature state reset and rewarm. Fold boundaries do not reset causal
state. Delayed source rows cannot contribute until their retained `known_at`
clock is no later than the decision close.

## Causal selection audit

Versions 22 and 23 record one selection-audit row for every nominal close slot. An
observed row carries only information available at that decision close:
retained source identity and known-at state, gap episode identity, causal close
and volume, typed feature values, missingness, and exclusion reasons. Missing
native slots remain explicit with unavailable context. The audit never stores
the future target, primary label, return, or path outcome, so it can inspect
selection and exclusion patterns without leaking the response into admission.
Its rows and typed feature bindings are hashed in the result provenance.

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

## Target and rolling-benchmark scoring

Version 24 fits its forecaster on the declared training rows only. Direction
uses the existing standardized L2 logistic fit. Sampled variance uses a
nonnegative train-only coefficient multiplying a declared raw candle feature
raised to its configured power. Coefficients, standardization, and calibration
remain frozen for each validation fold.

The two benchmarks are forecasts made independently at every scoring decision.
Their history is selected by historical decision time from the configured
elapsed window, and a historical label is admitted only when its entire outcome
was known strictly before the new prediction. Realized earlier validation
labels may therefore update a later benchmark, while the fold model remains
frozen. The calendar benchmark uses the declared UTC bin and shrinkage and falls
back to the current rolling-global forecast when its own support is insufficient.
It never uses a full-dataset average or a label known exactly at the prediction
clock.

Direction is scored with paired Bernoulli log loss and sampled variance with
paired squared error. Pooled gain and ratio skill use equal scoring-row weight;
fold ratios are not averaged in their place. Fixed UTC-calendar cluster draws
resample the model and both benchmark losses together. Results retain the typed
scoring rows, history membership receipts, shared clock and label manifests,
support exclusions, and fixed concentration diagnostics. Statistical skill is
not an economic or profitability claim.

## Execution and evidence

Evidence mode uses the existing requirements, prepare/freeze, Check job, and
replay boundaries. A request must bind exactly one frozen Dataset. The evaluator
fails instead of truncating when it exceeds 19,104 candles, 256 MiB input, 120
seconds, or 16 MiB result output. Configuration, typed feature and optional
Indicator bindings, population, selection audit, predictions, definition,
request, plan, and evidence remain hashed by the existing canonical Check
envelope.

Completion is a forecast screen. It does not grant execution, promotion,
profitability, causal, or scientific-confirmation authority.
