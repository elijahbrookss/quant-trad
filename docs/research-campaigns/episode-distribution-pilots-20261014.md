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
These two half-hours are **feasibility runs with bounded descriptive examples**.
They test joint support, clock interpretation, trade reconciliation and whether
usable events/controls can be formed. They do not answer the original six-hour
question, estimate a full-window event rate or establish repeatability across
days. Two dates with sparse matched episodes do not become adequate repetition
merely because their signs agree. No replacement window is selected after seeing
sparse support or outcomes.

The narrowing is a prospective resource/support step, not an outcome selection.
The new pilot deliberately caps each alias at 50,000 frozen revisions and each
scope at 30 minutes while the joint raw-trade/flow/book path is unproven. At one
row per second, the three retained depth bands (5/10/25bps) would nominally yield
64,800 rows in six hours before revisions and before the evaluator filters 5bps,
exceeding that guard. A half-hour nominally has 5,400 such rows, plus 1,800 BBO and
1,800 flow rows; raw trade counts and revisions remain unknown until admission.
These are grid arithmetic, not measured coverage or a guarantee that preparation
fits runtime/memory limits. A cap failure stops; it does not trigger a shorter
outcome-selected interval. Extending to the original six-hour question needs a
separately fixed bounded-input/support plan, with the original rules preserved
unless a distinct question is declared. Feasibility outcomes must not tune that
plan's thresholds or select favorable periods.

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
crossing to at or below 50% of its preceding 60-second median. Half-depth is a similarly
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

All context and trajectory inputs stay inside the declared half-hour; no hidden
warmup or outcome tail is requested. The inclusive 241-second path permits
anchors only from 00:02:00 through 00:27:59 UTC. Matching context is the inclusive
61 observations from anchor-120 through anchor-60 (60 one-second returns), so it
ends before the sharp detector's anchor-60 through anchor move interval. Control
anchors use that same declared structural grid, but selection requires only the
61 pre-context observations; it never inspects their anchor or future path. The
earliest possible matched event
anchor is 00:07:00, with a control at 00:02:00; actual gaps can narrow this support
further. Unmatched early events remain explicit rather than extending inputs.
Once the best pre-context control is selected, retain its identity even if its
anchor, subsequent flow or displayed-book path is missing or crosses validity
intervals. Emit partial second rows, missing offsets and path-support reasons;
do not search for a future-valid replacement. The next-60-second label is null
when its own 0..60-second path is unavailable or invalid. Missing later seconds
outside that horizon can leave the forward label resolved while the full
trajectory remains incomplete. Selection and observed outcome support are
separate counts.

Match one earlier same-UTC-hour control at least 300 seconds away, separately
within each origin and without control reuse within that origin. Matching uses
only the -120 to -60-second context: positive versus nonpositive prior return
(zero belongs to nonpositive), median spread, median total side depth and RMS
one-second midpoint returns. For each nonnegative spread/depth/volatility value
pair `(x,y)`, use the symmetric absolute-scale ratio `max(x/y,y/x)<=2`, not a
signed ratio or fitted standardized distance. Equality at two passes. When both
values are zero they match with zero log-distance; when exactly one is zero they
do not match. Canonical admission requires positive spread and positive total
depth at every joined second, so zero medians for those two features cannot enter
this pilot; volatility may genuinely be zero. No epsilon, pseudocount or silent
replacement handles zero. The factor-of-two
caliper prevents order-of-magnitude context mismatches without fitting a model
or choosing a tight empirical optimum. Minimize the sum of absolute log ratios,
then choose the earliest timestamp on ties. Report unmatched anchors. Controls
may exhibit later moves: their outcomes cannot determine matching. The two
origins may reuse a control identity and are not independent studies.

Report unique event anchors, event anchors shared across origins, unique control
anchors, controls reused across origins and additional control occurrences.
Coalesce an anchor used in multiple roles/origins into one planned inclusive
[anchor-120, anchor+120] window. Count intersecting unique-window pairs, including
windows touching at an endpoint, and transitive overlap components. Retain window
roles and case IDs; missing support does not erase its planned window or its
shared identity. Episode rows, selected matches, unique windows and overlap
components are descriptive accounting, not independent trials or an effective
sample size. Common shocks and longer serial dependence remain outside these
short-window overlap counts.

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


## C2: volume after body/range/prior-volatility comparison

Status: authorized implementation and one separately allocated Check attempt,
zero automatic replay. This text specifies a prospective procedure, not a result.
Both A attempts retain their allocation. Reconcile the current ledger before
dispatch, serialize with engineering, and use the established release owner.
Do not cancel a running source qualification to add this implementation.

### Configuration versus code

`raw_forward_outcome@7` is selected by `volume_cohort_matched.v1`. The existing
study request supplies the frozen Dataset, source/instrument, dates, timeframe,
raw candle detector, direction, entry lag and single primary horizon. A different
supported sample does not require a code change. The comparison exposes:

```json
{"cohort":"volume_gt_previous_vs_lte", "prior_volatility_bars":20,
 "matching_policy":"same_date_hour", "caliper_ratios":{
   "formation_body_fraction":1.25,
   "formation_range_fraction":1.25,
   "prior_rms_return":1.25}}
```

The cohort and matching-policy fields name the currently supported algorithms;
they are not a universal expression framework. Supported raw predicates determine
the event universe from the input, including explicit current/previous-volume
predicates. No Python constant contains this study's candle IDs, calendar dates,
asset, venue, expected cohort counts or outcomes. Preserve the original C request
choices for C2, not a hardcoded list of candles. Matched counts are allowed to
differ; every missing-feature and unmatched original event remains visible.

For this C2 configuration retain the original frozen January 2022 Coinbase
BTC-USD one-minute input, inclusive body-engulfing predicate, current volume
greater than previous versus less than or equal, long next-close entry with lag
one, and five-bar target strictly before evaluation end. No new warmup, provider,
date expansion or holdout is requested. Historical inferred close availability
is not a measured live receipt/publication clock.

### Prospective selection and support

Use logs of positive absolute body fraction, high-low range fraction (both divided
by current open), and prior RMS. Twenty prior completed candles yield nineteen
close returns, exactly the C diagnostic definition. Zero/missing RMS or zero body
is feature-ineligible without epsilon or imputation; retain the original row and
its reason. Features must be known by formation close. Genuine zero volume is
valid cohort data, not a missing value. V6's baseline 20-candle context and the
configured matching context are labeled separately if their windows differ.

Pair increasing-volume events chronologically to unused non-increasing controls
within the same UTC formation-close date/hour. For each log feature enforce the
configured ratio caliper. Minimize summed squared normalized distances; break
ties by absolute event-time distance, then earlier control. Within-pair closed
nominal entry-to-target intervals must be disjoint; shared endpoints overlap.
Select solely from pre-outcome features and nominal times, including nominal
targets with unavailable futures. Freeze identities/hash before any outcome
resolution. Incomplete selected pairs stay incomplete, without replacement.

Report selected-pair and complete-pair balance separately. Inherit support gates:
100 complete pairs, 10 treated UTC days, matching fraction >=0.5 of feature-eligible
increasing-volume events, pair outcome completeness >=0.95, and all absolute SMDs
<=0.1 for both selected and complete pairs. Also emit matching fraction against
the original increasing-volume denominator, every calendar day's original,
eligible, missing, selected and complete counts for both cohorts, missing reasons,
and all unmatched treatments/unselected controls. The gates are not power claims.

Primary outcome is mean paired five-bar return difference. Emit median paired
difference and complete-pair arm distributions/tails alongside the full original
distribution. Retain extremes; no trimming, winsorization or outcome-based split.
Delete whole pairs for either arm's UTC day. Report closed outcome-support and
full prior-feature-through-target overlap components across both roles, for the
original and selected populations. Freeze a joint nonoverlap subset on full
feature support before outcomes; require 30 complete pairs over 6 days for that
sensitivity. Do not rematch. Components do not measure effective independent N.

If timing integrity, support or balance fails, close this formulation as
unsupported. Do not tighten/relax calipers, change feature windows, drop extremes,
expand dates or retry to rescue a claim. Any supported contrast concerns a
common-support subset, distinct from the original mean gap. Same-formation
volume/body conditioning does not establish causation or profitability.


### Interpretation and question separation

C remains closed with its original evidence; C2 is a separate question and must
have separate evidence and interpretation. The original detector has no prior
downtrend filter and is not the completed, visually validated reversal formation
question B. Higher total candle volume is not signed buying pressure.

The result includes original, feature-eligible, selected and complete-pair context
counts/means/medians for both cohorts, so the population change can be inspected.
A sparse matched subset must not be offered as an explanation of the entire
original gap. Day deletion and nonoverlap support influence/dependence diagnostics;
this bounded procedure supplies no dependence-adjusted confidence interval,
significance result or effective-independent-N estimate. Limit inference explicitly
rather than adding an uncertainty framework. A shrinking difference does not
prove that volume never matters; a persistent descriptive difference does not
establish causality or profitability. No automatic new-period execution follows.
