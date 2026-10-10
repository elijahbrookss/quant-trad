---
component: adr-candle-risk-observability
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - research
  - checks
  - known-at
  - evidence
code_paths:
  - portal/backend/service/research/forward_risk_evaluator.py
  - portal/backend/service/research/crossing_state_comparison.py
  - tests/test_portal/test_crossing_state_comparison.py
  - portal/backend/service/research/registry.py
  - portal/backend/service/research/planning.py
  - portal/backend/service/research/execution.py
  - portal/backend/service/indicators/indicator_service/runtime_validation.py
  - tests/test_portal/test_forward_risk_check.py
  - tests/test_market_data/test_candle_only_check_workflow_db.py
---
# ADR 0076: Declare candle-risk observability and calendar boundaries

## Context

An event-only evidence list cannot distinguish an observed ordinary period from
an unready detector. Extending every outcome past a discovery window can also
consume a reserved evaluation period. Dropping all events around any gap loses
valid shorter measurements, while filling holes invents observations.

## Decision

Register event-fact definition 10 / evaluator 9 with explicit fixed candle-risk
semantics. Public Candle Stats snapshots own detection, the Check owns analytical
cohorts and risk measurements, and the existing frozen Dataset owns source and
gap identity. Capture readiness only when requested, include it in input hashes,
and keep original default output hashes unchanged. A declared no-tail boundary
limits materialization; independently censor entry or targets outside it.

Use per-dependency and per-horizon eligibility. Preserve undetectable clock
intervals and unknown event counts. Measure raw risk separately from future/prior
risk ratios and high-low paths. The baseline excludes the trigger, prices obey
known-at, and retrospective labels preserve their own availability clock. Late
complete paths remain usable outcomes with disclosed delays, never inputs to an
earlier decision. Report calendar
variation, missingness, stratum coverage and dependence without inference or
promotion. Store all-clock-row digests plus bounded examples; frozen replay
reconstructs detailed rows without another persisted candle-sized population.

## Consequences

No new Indicator, provider, persistence schema, live order path or independent
state engine is introduced. Finite seed/reset initialization is explicit, not
claimed equivalent to uninterrupted EMA history. Old definitions keep their
meaning. Exact source admission, disposable frozen-workflow qualification and
resource limits remain required before empirical use. Annual readiness does not
follow from a small-window test or from absence of data acquisition errors.

## Validation

Synthetic tests exercise closed-form risk, prior and future clock boundaries,
zero baselines, delayed availability, independently missing paths, calendar end,
identity/configuration rejection, deterministic hashes, deletion sensitivity and
real-engine reset/readiness. The disposable workflow fixture freezes an actual
recorded gap, runs the registered owner, corrects mutable source data, and checks
provider-free replay against the original evidence.

## Fixed crossing versus persistent-high comparison

Definition 11 / evaluator 10 adds an explicit
`candle_risk_matched_state.v1` request with
`matching_contract: crossing_state_matched_pairs.v1`. Definition 10 stays
unchanged. The new comparison reuses the existing public Candle Stats timeline,
prior baseline, price sampling and future-risk owner. It changes the question:
compare a newly observed crossing with an already-high state at similar current
ATR z-score and prior risk. It does not add an Indicator or provider.

Both observations require consecutive observable public snapshots in the same
reset segment. A crossing has the public event, current z > 2 and previous
z <= 2; a persistent-high observation has no event and previous z > 2. Unknown
entry after seed/gaps is excluded. An event/state contradiction fails. Baseline
risk must be positive and finite, the canonical entry must have zero delay from
its decision, and the nominal 120-minute endpoint stays strictly inside the
evaluation year. These rules define the pre-outcome crossing denominator.

Match crossings in decision-known-at/source-identity order to one unused control.
Require the same UTC month, six-hour block and existing prior-RMS bin, an absolute
z difference <= 0.25, a larger/smaller prior-RMS ratio <= 1.25, and at most seven
days between decisions. Reject same-episode pairs and positive-duration overlap
within a pair. Minimize the maximum of normalized z difference and absolute log
RMS ratio, followed by time distance, earliest control known-at and source
identity. Arithmetic is IEEE binary64 with inclusive calipers and exact tuple
ordering, without fuzzy ties. Controls may occur later in calendar time: each
feature is known at its own observation, but matching is retrospective and is
not a live signal available at the crossing.

Freeze this pair map before computing future outcomes. Drop the whole pair if
either risk outcome is unavailable; never rematch from future values or path
quality. The primary measure is the equal-pair mean difference in 120-minute
mean squared successive one-minute log returns. Report the future/prior-ratio
contrast separately. The result retains all pair identities, features, endpoints,
outcome known-at and exclusion reasons, plus the ordered feature-pair digest.
Existing result/evidence limits bound that output; no silent truncation is allowed.

Report common support by month, risk/time stratum and quarter-z intervals, plus
balance in current z and log prior RMS. Current public ATR ratio is an unmatched
balance diagnostic. Fixed support gates require 70% matching overall, 50% in
each of twelve months, 95% complete pairs, 200 complete pairs and ten per month,
and absolute standardized mean differences <= 0.10. Standardization uses the
pooled population standard deviation; zero dispersion is supported only for
identical means. Empty months fail rather than disappear. Failed support stays
visible as `unsupported`; a descriptive value cannot rescue that failure.

All primary pairs remain visible, with cross-arm overlap and calendar/episode
concentration. Day/week deletion removes a pair if either arm belongs to the
removed period. Joint nonoverlap traverses the already frozen complete pairs
and keeps a pair only when neither arm overlaps any arm of an earlier retained
pair. It never rematches. Its descriptive investment gate requires 50 pairs
across six months. Positive primary, at least nine positive months, all positive
week deletions and a positive supported joint sensitivity are per-asset
investment criteria, not significance or automatic promotion. Both fixed assets
must be assessed before proposing further validation. No effective independent
sample size or causal claim is emitted.

Candidate matching is bounded to twenty million comparisons, evaluated in
batches with cancellation/ownership checkpoints. Exceeding the cap fails rather
than loosening calipers or returning a partial match. Existing time, byte, row,
ownership, resource and frozen-binding controls still apply. The disposable
workflow verifies reuse of an original v9 frozen Dataset, Observation admission
and exact provider-free replay after mutable-source correction.
