# Completed reversal continuation/failure: fixed January development pilot

User decision `Sentinel_18b6178e0d348191b4324a120080389c` includes the first
retreat candle's high and locks the reference at that candle's close. This selects
the existing `rising_leg_plus_retreat` setting; it does not rewrite earlier
`provisional.1` events, rising-leg-only previews or raw Check v9 results. No
minimum formation size, expiry or winner policy is added. Every chain remains.

January27 verification on the same retained fixed-origin prefix gives reference
35996.37 from the 02:15–02:30 UTC retreat candle, price and role known at02:30.
The bound illustrated chain completes at03:00. The prior rising-leg-only35948.70
record remains separate. A four-bar reference regression and full-prefix preview
verify the rule without computing post-formation outcomes.

## Fixed protocol before new outcomes

Use existing frozen `mds_230a9cff937d9280043e893357842af3`, hash
`230a9cff937d9280043e893357842af30a1be3434d3610dc511f91b924396927`,
Coinbase BTC-USD **spot**,15m, source
`10fd0310823dc4e9526c3f12d3baf7a5f499ee17c5cc1c489bd37dcacf115293`.
Initialize at2021-12-30 23:45 UTC; include candidate completion closes in
`[2022-01-01,2022-02-01)` UTC. Existing3073bar frozen manifest only; no February
tail, providers, new acquisition, refreeze or sealed holdout. January has already
been exposed through related research and these visual examples. This is an
exposed development description, not preregistration on unseen evidence.

`raw_forward_outcome@10` explicitly selects `reversal_first_passage.v1` and
pins this protocol in material rules. Settings: retreat-inclusive reference,
retain-all ownership, max_bars3073,max_candidates3073. Resource caps fail instead
of truncating. Canonical preparation and the same sequential recognizer are
reused. The new code adds only the missing first-passage/overlap procedure.

The observational anchor E is the formation completion close, known at that
close under this frozen input's availability semantics. Tracking starts with the
next15m candle, for12candles/three hours. This covers the entire post-completion
path; E is not a guaranteed executable fill. The old body-pattern CR15 Check's
next-candle-close entry is not inherited into this new formation question.
Historical known-at is max pinned-input availability and interval close, not
measured receipt/publication latency.

At completion, lock L as the minimum low over the entire formation through that
completion candle. Failure is the first subsequent candle with low<=L.
Continuation is the first subsequent candle with high>=E+(E-L), a symmetric
one-risk-unit advance from E. Equality counts.
Verify E>L; a nonpositive risk or nonfinite computed upper barrier is retained
as unresolved with an explicit invalid-case reason, without inspecting outcomes.
No intrabar path is invented: if the first touched candle reaches both thresholds, label unresolved with
`intrabar_order_unknown`. A full12bar path touching neither is **neither**.
An evaluation-censored path is **unresolved** unless a first passage was already
known earlier. The per-chain procedure also reports missing/unavailable future
bars as unresolved; the Check's stricter whole-input recognition contract fails
the entire attempt on an interior gap, changed source or delayed-close input,
since a complete formation population then cannot be reconstructed. No chain or unresolved row
is dropped; all four classes remain in denominators. These choices are fixed
before new outcomes and will not be swept or tuned. Comparisons use retained
binary64 prices and computed barriers exactly, without decimal rounding or an
epsilon; the serialized barrier is the actual comparison threshold.

## Retain chains while exposing dependence

Before outcomes, give each chain the closed interval from formation_start to
completion+12bars, regardless of actual first-touch time. Transitive overlap
components include shared boundaries and simultaneous completions. Preserve
every source-linked chain and component membership. Report chain counts/shares,
component count, each component's four-class fractions and the equal-component
mean of those fractions. Neither/unresolved remain in every denominator. Report
UTC completion-day counts and the largest component's chain fraction. Components
also report chain-mass HHI and its inverse as descriptive concentration, never
as effective independent sample size. Components
are reporting units, not a claimed independent sample or effective sample size.
Old references may connect much or all of the month; disclose that rather than
remove them or redefine episodes after seeing results. No significance,
predictive advantage, causal, cost-adjusted profitability or live-fill claim.

## Execution and stopping

Preserve protocol, exact request/definition/plan/source hashes, approval,
allocation and frozen input identity in QT before dispatch. Existing requirements,
Dataset binding, global custody/admission, single-attempt job, Check evidence,
Observation and Study history paths own execution and retention. No new registry,
Study schema, queue, provider or scientific execution framework is introduced.

One bounded Check attempt including failure, zero automatic retry/replay or
alternative thresholds/horizons. Reconcile unknown submission outcomes before
any further action. Stop on missing capability/input/source qualification,
unavailable global slot, resource failure or completion regardless of counts or
class proportions. Never relax overlap or outcome rules to rescue support. Zero
events or a single connected component is an informative descriptive limitation;
it does not authorize another window, filter or parameter sweep. Source review,
integration, exact-source qualification and deployment remain with the existing
release owner and permissions. No live dispatch from unqualified child source.
