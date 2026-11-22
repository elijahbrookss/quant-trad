# Nearby-date volume comparison: fixed prospective protocol

Status: implementation authorized; no revised scientific outcome evaluated, no
Check dispatched, and no deployment authorized by this document. Integrate into
the existing `feature/research-computation-refactor` destination, review and
qualify the exact integrated source, then coordinate with its release owner
before any separately accounted scientific operation. C2 remains closed as
unsupported. This is a separate question, not a correction to its result.

## Question and fixed window

Among the original January bullish body-engulfing events, does increasing volume
have a descriptive five-bar return difference when controls may come from the
same UTC close hour within a fixed 72-hour radius on nearby dates, while body
size, candle range and prior volatility remain matched?

Fix `comparison_window_days=3`, `matching_slot_seconds=3600`, and
`matching_policy=same_utc_close_slot` before examining revised outcomes. The
radius is an inclusive absolute elapsed-time bound of 259200 seconds. At the
outer dates, same-hour candidates beyond exactly 72 hours are excluded. Controls
may precede or follow treatment. Use all in-scope candidates satisfying that
rule; do not require a full symmetric window at January's edges or acquire
December/February observations to make one.

Three days is a prospective comparability/support compromise: it adds nearby
dates while keeping temporal separation under half a week. Matching the UTC
hour retains the existing intraday-time restriction and includes the old
same-date/hour candidate pool. The exact-minute slot in v5 would replace that
pool rather than simply widen it; v8 reuses its matching calculation with an
explicit slot width. No empirical optimum, regime stationarity, calibrated power
or guaranteed matching improvement is claimed. Chronological greedy allocation
without control reuse means even a larger candidate pool need not increase the
number finally matched. Do not evaluate alternative radii or slots to rescue
support or a favorable outcome in this allocation.

## Inputs, formation and outcomes

Use only the original frozen Dataset
`mds_b7c35a1ac454e899031ec4a06dda41f3`, Coinbase BTC-USD spot, January 1 inclusive
through February 1 exclusive, 2022, one-minute candles with the original one-bar
warmup. Do not silently add February, other sources, instruments or acquisition.
The request, not reusable Python, owns this identity and scope.

Retain the exact original bullish inclusive body-engulfing detector, without a
downtrend filter, and the increasing-volume versus non-increasing-volume split.
This does not replace the completed visually validated reversal question B.
The entire original event ledger stays visible, including missing context and
unmatched cases. For future evidence reconcile the original 3507 events and
1948/1559 cohort counts, frozen fact-input identity and unchanged full outcome
distributions before interpreting the matched subset. A mismatch stops; it
does not authorize altered selection or replacement data.

Keep long entry at the next candle close (lag one), five-bar target close
strictly before evaluation end, and historical interval-close-inferred timing.
For formation open t: decision close t+1 minute, entry t+2, target t+7. There is
no measured live publication, executable-fill, fee or profitability claim.

## Unchanged feature matching and support

Use positive log absolute body fraction, high-low range fraction (divided by
current open), and RMS of the 19 returns among 20 prior completed candles.
Each feature ratio caliper remains 1.25. Missing/nonpositive matching features
remain explicit; no epsilon, imputation, extra warmup, trimming or winsorization.
Features must be available at formation close. Genuine zero volume remains
valid cohort data. Higher total volume is not signed buying pressure.

Keep chronological increasing-volume treatment order; choose minimum sum of
squared normalized feature distances, breaking ties by absolute temporal
distance then earlier control. No control reuse. Within-pair nominal closed
entry-to-target intervals must be disjoint. Freeze candidates, pairs, feature
hash and joint full-feature-support nonoverlap subset before resolving outcomes.
Failed future outcomes remain unresolved without a replacement control.

Unchanged gates: at least 100 complete pairs over 10 treated UTC dates, matching
coverage at least 50% of feature-eligible increasing-volume events, outcome
completeness at least 95%, and every absolute SMD at most 0.1 for both selected
and complete pairs. The full-feature nonoverlap sensitivity requires 30 complete
pairs over 6 treated dates. Gates are prospective diagnostics, not power proofs.
All v7 missingness, original-versus-eligible-versus-selected context, tails,
calendar coverage and both-arm overlap accounting remain emitted.

## Interpretation and stopping

Primary contrast remains the mean paired fractional return difference; retain
median paired contrast and complete-pair distributions, without discarding
extremes. Report treatment/control dates, signed control-time distances and
same/earlier/later-date pair counts. Delete the whole pair when either arm's
date is omitted, with no rematching. Both-arm date count is reported separately
from the unchanged treated-date support denominator.

Nearby dates can differ in trend, news, trading conditions and unmeasured state.
The UTC hour and declared features do not ensure regime comparability. One
day's treatment may match another day's control, coupling the day results.
Overlap components, day deletion and nonoverlap sensitivity do not establish
independent samples, dependence-adjusted confidence intervals or significance.
Matching to later controls is retrospective association, not a live decision.

If the fixed support/balance/timing gates fail, close this formulation as
unsupported. Failed support leaves this controlled question unresolved; it does
not establish that volume has no effect. A selected-subset difference cannot
explain or reverse the full-cohort association automatically. No caliper/radius
search, other month, automatic replay, retry, causal or profitability claim
follows. Preserve historical v7 and C2 evidence irrespective of this outcome.
