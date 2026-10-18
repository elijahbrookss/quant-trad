# Saved reversal comparisons: descriptive first passage

`raw_forward_outcome@13` consumes the original BTC B3 selection Check and its
original native 15-minute frozen Dataset. It does not recognize formations,
select controls, change barriers, remove unmatched chains or tune a horizon.
The explicit version binds the exact Check/result/evidence/selection-manifest
hashes in `reversal_matched_outcomes_evaluator.py`; substitution fails.

## Population and outcome

Keep the 999 saved pairs at 716 case completion moments. Use each saved pair's
equal-completion-moment weight. Report old first-three blocks, new last-three
blocks and combined populations, plus block/day descriptive breakdowns.
Within each reporting view, divide weights by their sum in that view. Report
all four case and control fractions and case-minus-control differences.

Anchor E is each saved formation/control close. Case L is its saved whole
formation low; control L is its saved own trailing-n low. Validate anchors and
barriers against original frozen input before consuming the future path.
Failure touches low <= L; continuation touches high >= E+(E-L). Equality counts.
Start with the next candle for twelve 15-minute bars. Reuse the extracted
`first_passage_path` calculation; the historical `first_passage` wrapper and
its v1 rules/output remain unchanged. Ordinary controls are not fabricated
recognition events.

Both barriers first touched in one OHLC candle are unresolved because intrabar
order is unknown. A complete path touching neither is neither. Missing or
boundary-truncated paths are unresolved unless a first touch was already known.
All four classes remain in denominators. One saved control's nominal path
crosses July 19 00:00; use this boundary rule, with no new BTC tail acquisition.
Completion close is an observational anchor, not a guaranteed fill.

## Evidence and dependence

Use existing canonical frozen Candle preparation and typed `CheckResult` and
`CheckEvidenceBinding` validation for the saved Check. Never call the Scientific
Attempt reference resolver: that resolver explicitly replays science and is not
needed to consume immutable selection evidence. The saved reference's exact
hashes are pinned in method configuration and copied into resulting evidence.
The original Dataset and bound watermark must match; mutable preview is rejected.

The historical v12 internal manifest hashes integer histogram keys before JSON
converts them to strings. Restore only those declared key types to verify that
historical internal hash; do not rewrite its result or hashes. The outer result
and evidence hashes validate directly under existing typed contracts.

Preserve the full-population selection audit and saved direct-support/ancestry
component counts. No coverage threshold is a scientific pass gate. This is a
finite selected-sample descriptive contrast. Short-duration concentration does
not imply continuation or failure. No independent-pair uncertainty, significance,
power, causal, predictive or profitability claim follows from it.

## ETH comparison

ETH-USD spot uses unchanged `raw_forward_outcome@12`, provider-native Coinbase
15-minute input from January 1 to July 19, 2025 exclusive (19,104 bars), and the
same February 1-July 19 completion window and six fixed 28-day blocks. Retain
every retreat-inclusive chain, matching features/calipers/strata, support,
no-control-clock-reuse and completion-moment weights. Match within ETH and
compare its old/new/combined selection summaries with the retained BTC result;
never rerun BTC selection for this comparison or pool symbols as independent
replications. Missing native coverage is explicit; no internal aggregation or
synthetic fill. Prior research exposure is recorded separately from outcome
findings. No ETH outcomes belong to this selection comparison.

## Limits and stopping

Fixed limits are 19,104 input bars, 10,000 pairs, 120 method seconds, 256 MiB
method input and 16 MiB scientific result. Fail on changed evidence, missing
anchors/history, mismatched source/availability, gaps, resource failure or
uncertain submission. Never truncate, automatically replay, rematch or change
dates/calipers to rescue a result. Reuse the normal qualified-source, global
admission, single-attempt Check and evidence readback path. Deployment and
integration stay with the coordinated release owner.
