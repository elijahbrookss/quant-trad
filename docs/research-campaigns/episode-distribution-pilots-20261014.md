# Complete volume distributions and bounded liquidity episodes

These are two exposed-data descriptive questions. They do not certify a trading
edge, causality, independent experiments, or a sealed holdout. User approval on
2026-10-14 permits the bounded capability changes and subsequent research through
the ordinary admitted QT workflow. Deployment remains coordinated by the parent.

## C: Which observations explain the volume-cohort mean difference?

Use `raw_forward_outcome` base version 6, selected explicitly by
`outcomes.schema_version=raw_candle_distribution.v1`. Historical v3/v4/v5
definitions and result meanings remain unchanged. V6 composes v3's canonical
candle preparation, predicate and close-entry/outcome resolution with a private
complete-event collector. It does not introduce another price/outcome engine.

Bind the existing H05 frozen Dataset `mds_b7c35a1ac454e899031ec4a06dda41f3`,
Coinbase BTC-USD instrument `0a019a46-c679-4c30-b90c-41518c68e31f`, January 2022,
1m, one warmup candle, long direction, one-bar entry lag, five-bar horizon,
strict evaluation-end boundary and `continue_degraded`. Select the union of the
original bullish body-engulfing predicates and the two explicit volume
conditions `volume>previous_volume OR volume<=previous_volume`. Invalid/missing
volume is excluded with its reason, never assigned to the lower-volume cohort.
This is the H05 formation question; it does not replace completed BTC15m reversal
formation and first-passage question B.

Before interpreting new diagnostics, reconcile original Check counts and summary
values: increasing volume 1,948 complete events; non-increasing volume 1,559;
means `4.35973244588419e-05` and `-9.824254058256152e-05` respectively. Means,
medians, min/max and positive rates must agree within binary64 arithmetic
tolerance (absolute 1e-12 for fractional returns); counts must agree exactly.
Failure stops interpretation and initiates source/version reconciliation, not
parameter adjustment. Existing Checks are `b74c9e0b-03ef-468f-bd5f-eb9628b59f50`
and `f533a193-c9f9-480d-8b57-5372d02191c1`; Observation
`14169037-acb7-47c1-b431-397162583b63` remains immutable.

Retain every event, including unresolved events, with instrument/source/timeframe
identity, formation/decision/entry/target and outcome-known clocks, cohort,
actual volumes, body/range fractions, and prior-context availability. No event
table truncation is permitted; more than 10,000 events fails the operation.
Prior volatility uses the RMS of the 19 close-to-close returns from the preceding
20 completed candles. Missing history or a prior candle unavailable at formation
decision produces explicit null context. It does not remove the event or require
acquiring an earlier warmup range.

Quantiles are fixed at 1/5/25/50/75/95/99 percent using linear interpolation.
Lower/upper 1% and 5% tails use `ceil(n*fraction)` ranked observations, ties broken
by event identity. Emit tail sums and their contribution to the unchanged full
sample mean, with the full sample as denominator. These overlapping tail
diagnostics are not an additive four-part decomposition. Preserve valid extremes;
there is no trimming, winsorization or outcome-selected threshold.

Emit UTC formation-close-day counts, sums and contributions, equal-day means and
each leave-one-day-out mean. These are concentration/influence diagnostics, not
alternative estimates selected to rescue a claim. Overlap components join closed
previous-formation-open through target-close intervals transitively across both
cohorts, separately for each horizon. Component count is not an effective sample
size; common shocks and longer serial dependence remain possible. A tail-driven
effect may still describe meaningful market behavior.

## A: What order do price, displayed liquidity and executions exhibit?

Use `event_fact_analysis` base version 14, selected by
`outcomes.schema_version=liquidity_episode.v1`, with an explicit real
`analysis_as_of` cutoff. Begin with exactly 00:00-00:30 UTC on September 22 and 23,
2026, native BIP instrument `b2deb0a0-f292-408a-876d-3dadd8e3819b`. These first
half-hours are predetermined subsets of the exposed fixed six-hour windows.
No replacement window is selected after seeing sparse support or outcomes.

Bind exact canonical BBO (series 20), 5bps depth (21), one-second trade flow (2)
and trades (1). The planner continues to bind the existing 1m primary candle
surface, but this procedure's price displacement comes from joined BBO midpoints,
not candle-close execution. The procedure accepts 5-30 minute input ranges and
at most 50,000 revisions per alias. The first pilot uses 30 minutes per date.

The sharp-move label is an absolute 60-second midpoint log displacement reaching
10bps, after being below that cutoff at the preceding second. Ten basis points
is a round, interpretable 0.1% scale chosen before new episode outcomes; it is not
an empirically established universal definition of sharpness or an optimized
predictor. The distinct reverse question begins at the first bid or ask depth
crossing below 50% of its preceding 60-second median. Half-depth is a similarly
interpretable relative contraction, not an optimized boundary. Neither threshold
can be tuned using this pilot's outcomes. Depth bands follow the changing midpoint,
so changes may include price-band membership changes.

For each origin separately retain the first ten supported crossings, with a
300-second refractory interval. A supported anchor has all 241 materialized
seconds from -120 through +120, matching BBO/depth state and fingerprint,
one valid book interval, proven complete/non-late flow quality and raw-trade
count/side-volume reconciliation. Missing materialized seconds are excluded,
even if producer quiet evidence explains them: proof does not manufacture Facts.
This intentionally narrow valid-episode pilot does not demand continuous whole
days. An unsupported initial crossing must not become a later ongoing-threshold
event when coverage resumes. Retain exclusion counts without claiming the
unobserved number of true market events.

Match one earlier same-UTC-hour control at least 300 seconds away, separately
within each origin and without control reuse within that origin. Matching uses
only the -120 to -60-second context: sign of prior return, median spread, median
total side depth and RMS one-second midpoint returns. Spread/depth/volatility
ratios must be <=2 (both zero is permitted for volatility). The factor-of-two
caliper prevents order-of-magnitude context mismatches without fitting a model
or choosing a tight empirical optimum. Minimize the sum of absolute log ratios,
then choose the earliest timestamp on ties. Report unmatched anchors. Controls
may exhibit later moves: their outcomes cannot determine matching. The two
origins may reuse a control identity and are not independent studies.

Retain second sequences, raw executed-trade traces and actual availability
references. Separately label whether each thinning episode reaches a 10bps
absolute displacement from its anchor at any second in the following 60 seconds;
retain the no-move complement and controls with unrestricted futures. This
forward first-passage label differs explicitly from the rolling-60-second
origin detector. No attribution to cancellation is permitted: L2 quantity
updates have no cause/order link, and trades cannot uniquely explain each
displayed removal.

Event-time sequences use latest revisions at the declared analysis cutoff;
they are retrospective. Actual provider message, local receipt, acceptance and
known-at clocks are retained rather than overwritten. Derived depth/BBO receipt
and acceptance copy derived known-at; source publication copies source effective
time. Across-feed clock skew and batching prevent confident subsecond ordering;
separate sequence domains and within-second ties remain unresolved. The pilot
does not invent a historical availability-time trading decision. Any later causal
predictive question needs a distinct declared decision and entry protocol.

## Admission, budget and stopping

Proposed initial execution is one C Check on its existing frozen Dataset followed
by one A Check per fixed date, each single-attempt and serialized. Record these
three attempts in the existing scientific allocation/accounting surface before
dispatch. Failures count. No automatic replay, replacement date, acquisition,
new provider, concurrency change, sealed-data access or deployment is authorized
by method availability. Existing campaign/holdout rules continue unchanged.

Preparation/freezes and Check execution use ordinary QT admission and execution
limits. One heavy operation runs at a time; reconcile timeouts before retrying,
release ownership when terminal, and preserve raw receipts. Parent coordinates
combined-source deployment after engineering qualification. No science runs on
an unqualified source or from shell calculations.

Stop on identity/count reconciliation failure, malformed units/clocks, resource
limit, missing method capability or unresolved custody. Zero supported episodes
is a supported-data limitation, not a market null. Fewer episodes or only one
supported day remains a narrow description; do not extend the sample to obtain
a favorable or apparently powered result. D (depth incremental to spread) still
awaits identical joint cases/clocks/folds and is outside this change. The existing
60 Study records are not 60 independent experiments.
