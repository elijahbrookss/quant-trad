# Completed reversal comparison selection only

`raw_forward_outcome@11` selects one fixed comparison population without
forward outcomes. Version 10 and B1 remain unchanged. The shared sequential
recognizer retains every retreat-inclusive completed chain, with no expiry,
minimum move, winner selection or block reset. This is a feasibility report,
not evidence of a predictive, causal, profitable or independent-trial effect.

## Fixed selected design

Use 15-minute canonical candles, an explicit computation origin, and 84
evaluation days divided into three consecutive 28-day blocks. Input from the
origin through evaluation end must fit 12,000 candles. Recognition starts at
that finite origin; pre-origin ancestry is outside this population. The
normal Check request is `outcomes.schema_version: reversal_selection_only.v1`;
statistics, assertions, additional inputs and gap rewarming are unsupported.

Decision time is the completed parent event's availability. A control has no
completion at its own decision clock; earlier completions, pending references
and later formations are allowed. Do not select against later completions or
use them to censor/rematch controls. For a case with n formation candles,
control L is its own minimum over n trailing candles. With own close E,
fractional boundary distance is r=(E-L)/E. This trailing-window low remains a
comparison convention rather than an established equivalent of a formation low.

Match only on sigma, r and strictly prior movement z. Sigma is RMS of the 96
returns ending before the decision candle, requiring 97 preceding closes.
For decision index t, z=log(C[t-1]/C[t-13])/(sigma*sqrt(12)). The fixed
calipers are sigma ratio <=1.25, r ratio <=1.10 and abs(delta z)<=0.25.
The squared distance sums (delta log sigma/log1.25)^2,
(delta log r/log1.10)^2 and (delta z/0.25)^2. These are fixed design
constants with no demonstrated feasibility, power or error-control authority.
Derived q=r/(sigma*sqrt(12)), prior96bar movement and decision body/range are
diagnostics only. Do not add them to calipers or the score.

Require the same 28-day block, UTC six-hour slot and prior-12-bar distinct
completion-count stratum (0,1,2+), plus calendar distance <=14 days. Case order
is decision time then event ID. Choose the minimum distance, breaking ties by
absolute calendar distance, control time and source identity. Use one control
per chain and never reuse a control timestamp, even across different n.

Within each candidate pair only, require disjoint closed direct-history to
nominal decision+12-bar support intervals. Their starts include every required
formation/trailing, volatility and prior-movement candle. The nominal endpoint
does not inspect touches or forward values. Long no-expiry formations can make
matching impossible; do not relax the rule to rescue coverage. Cross-pair
overlap is allowed and reported, not treated as independence.

## Retained population and dependence

Keep every completed chain as a record with explicit invalid-feature, unmatched
or matched status and rejection counts. Report clocks with any match, fully
matched clocks, partially matched clocks and no-match clocks. Retained full
population mass uses equal weight per original completion clock, split across
all chains there. Thus matching one of five simultaneous chains retains one
fifth of that moment's mass, not a wholly matched moment.

For describing the matched population, each retained completion clock receives
equal weight, split across its matched chains. Keep unmatched chains visible;
this matched distribution is not the entire formation population. Report raw
chain counts, moment counts, date/block/duration coverage and full versus
feature-eligible and matched context distributions with observed/missing-weight
denominators. Each eligible-population clock has equal weight, split over its
eligible chains. Duration remains observed for invalid-feature chains. Weighted
quantiles use first CDF crossing, without interpolation.

Direct-support components include interval intersections and matched-pair
links. Recognition ancestry is separately conservative: when complete state
ancestry is not represented, all records sharing the computation origin form
one ancestry-aware component. Report its case/control sizes, completion-moment
count and span from recognition origin, alongside its direct-component links.
Report that limitation. No component count,
moment count or inverse concentration measure is independent sample size.

There are no 200-pair, 90-percent, 80-percent-stratum, 20-day or SMD0.10
scientific pass gates. Report actual coverage and population shifts once, then
review whether the intended population is represented. No repeated matching
campaign, caliper changes, expiry changes or date expansion follows a poor fit.

## Availability and historical custody

Do not treat intermediate v1 stage known_at as an actionable role clock. New
selection records include a separately named `reversal_stage_availability.v2`
view: candle availability and role recognition are distinct, and parent event
availability is also required. Trough role is known at first rebound close;
rebound-end role at reference confirmation; the final-low role at completion.
The existing v1 event identity and historical evidence are not rewritten.

## Resource and execution boundary

The existing admitted Check owns preparation and selection; CLI/report scripts
must not calculate features or perform offline selection. Reuse canonical
frozen Dataset delivery and shared recognition preparation. No outcome module
is imported. The Check declares no outcome horizons or read tail; extra frozen
suffix data is not consumed by selection. One authorized selection attempt has
20 million candidate comparisons, 120 seconds method wall time, 256 MiB input
and 16 MiB scientific result ceilings. Stop explicitly on exceeded limits,
required gaps, unsupported timing/source or missing data; never truncate or
retry/tune to rescue the population.

The fixed user-selected 2025 proposal is origin January1, evaluation February1
through April26 exclusive, with candidate frozen source through April26 03:00
UTC. Before dispatch verify exposure records, exact frozen/source/normalization
identities, qualified producing source, heavy-slot ownership, duplicate and
attempt admission. A successful selection report authorizes no outcome run.


## Provider native source amendment before B2 selection

The user selected provider-supplied candles at the requested timeframe on
2026-10-17. B2 now uses native Coinbase15m spot candles, canonicalized and frozen
directly over the unchanged declared window. The planned1m-to15m preparation
route and capacity-extension proposal are cancelled. Retain the previously
frozen1m input and failed derivation refusal as preparation history; do not use
them to synthesize this new B2 input or rewrite B1.

The original selected protocol v1 JSON remains retained. The provenance-only
amendment is `reversal-selection-only-v2.json`; case/control rules, matching
tolerances, finite origin, windows, weighting and no-outcome scope are unchanged.
Native candles have historical close-inferred availability, not measured live
publication latency. The new input must have its own frozen manifest/source
identity; do not claim byte equivalence with historical derived candles.


## Fixed six-block extension

The separate [six-block selection procedure](reversal-six-block-selection.md)
uses version12. Version11 and its historical results remain unchanged. Extending
the reporting period does not create old-block controls, independent episodes
or permission to inspect future outcomes.
