# Saved BTC comparisons: descriptive target progress

`raw_forward_outcome@15` reads the exact frozen BTC B3 selection and v13
first-passage evidence. It keeps all 999 pairs, original weights, barriers and
twelve-bar horizon. No recognition, matching, target optimization, acquisition,
future tail or historical result replacement occurs. This is a post-outcome
description, not a prediction or a hypothetical trading result.

## Fixed calculation

For saved anchor E, failure L and original continuation U=E+(E-L), report
`100*max(0,max(next12highs)-E)/(U-E)`. Include the anchor's zero progress and
allow values above 100%. Full-window progress includes movement after failure;
it never changes or credits the saved first-passage class.

For progress before the first failure candle, report two bounds. The lower
bound uses strictly earlier candles' highs; the upper also includes the first
failure candle's high. OHLC does not reveal that candle's high/low order. If
there is no failure in a complete horizon, both bounds equal full-window
progress. A prior continuation can be followed by a failure within the same
window; these bounds describe that path without revising the original class.

Keep a partial horizon's observed maximum as a lower bound, with observed bars,
end, censor reason and original weight. Exclude it explicitly from complete
three-hour quantiles/bins. A partial horizon can still have final pre-failure
bounds if failure was already observed. Never infer a missing tail. The saved
boundary control has five available bars and seven unavailable bars.

## Fixed reporting

Report old first-three blocks, new last-three blocks and combined views,
separately for formations and controls, for all rows and each arm's originally
neither subset. Use original pair weights without redistributing weight among
surviving chains at a completion clock. Normalize only within a named group or
its explicitly available endpoints. Show raw counts and original weight mass,
including unavailable mass and censored rows.

Quantiles are p10, p25, p50, p75 and p90, using the smallest value reaching the
requested cumulative weight, without interpolation. Fixed display bins are
[0,25), [25,50), [50,75), [75,100), [100,infinity) percent of the original target
distance. Apply the same displays separately to the full-window endpoint and
both pre-failure bounds. These are display bins, not replacement targets.

## Evidence, limits and stopping

Reuse canonical Candle preparation, v13's declared original Dataset support,
typed CheckResult/CheckEvidenceBinding validation and first_passage_path to
check the saved classes against the frozen path. Read the exact saved Checks
without the Scientific Attempt replay resolver. Both input bindings must match.
Check/result/evidence/manifest references are pinned in method configuration;
source Check IDs survive typed result metadata removal under source_check_id.

Retain saved support/ancestry dependence. No independent sample size,
significance, causality, profitability or beyond-three-hour claim follows.
Near misses do not establish eventual hits or a superior smaller target.

Use the ordinary qualified-source, fresh global admission/duplicate check,
single-attempt evidence Check and immutable readback route. Fixed limits are
19,104 candles, 999 pairs, 120 method seconds, 256 MiB input and 16 MiB result.
Stop on changed references, geometry, weights, classes, input/source clocks,
limits, uncertain submission or missing prerequisites. Do not relax limits,
resize targets, rematch, drop extremes or acquire a tail to rescue a result.
Integration, qualification and activation remain with the release owner.
