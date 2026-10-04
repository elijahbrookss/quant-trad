---
component: adr-versioned-original-range-first-return
subsystem: indicator-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - indicators
  - research
  - known-at
code_paths:
  - src/indicators/market_profile/runtime/first_return.py
  - src/indicators/market_profile/manifest.py
  - src/indicators/registry.py
  - portal/backend/service/indicators/persistence_payload.py
  - portal/backend/service/research/first_return_evaluator.py
  - tests/test_indicators/test_market_profile_first_return.py
  - tests/test_portal/test_first_return_check.py
---
# ADR 0075: Pin first returns to their original range and Indicator version

## Context

A first close back inside a breakout's value area differs from the existing
confirmed outside-inside-outside reclaim. Replacing the active profile or
looking only at occupancy would change the event. Adding outputs to an existing
manifest would also invalidate frozen graphs and change old output hashes.

## Decision

Market Profile retains the v1 default manifest, parameters and outputs. An
explicit v2 instance adds `first_value_return` and `first_return_state` on the
same `initialize -> apply_bar -> snapshot` engine timeline. Creation and config
validation accept `version`; updates cannot change it. The existing reserved
Indicator metadata envelope persists `runtime_version`; absent means v1. No
schema migration or implicit data rewrite occurs. Factory, requirement plans,
runtime graphs and frozen evidence validation select the recorded manifest.
Semantic plan reconstruction must carry the version from the pinned manifest
into its preloaded metadata; dropping it would silently select the v1 default.

The v2 owner consumes the existing raw breakout builder and retains separate
origins, original reference identity, VAH/VAL/POC and clocks. A subsequent close
strictly between original VAL and VAH emits one first-return event. Boundary
equality is separate. Profile replacement does not replace the origin. Lifetime
uses the existing `reclaim_max_bars` parameter (1..100 for v2); expiry is disclosed
for one terminal snapshot. Gaps/unavailable profiles censor pending origins and
reset the independent ATR accumulator. The state context publishes strictly
prior warmed Wilder ATR using `retest_atr_period` and its availability watermark.
Current-bar volatility cannot change that denominator.

Registered event-fact Check definition 9 / evaluator 8 consumes these public
outputs. It compares returns observed by a fixed classification close with
observed still-outside origins. A later common sample precedes every original
endpoint. The outcome is reduction in absolute distance to original POC divided
by ATR known before the sample bar; positive means closer. Close-based center
crossings, already-at/beyond-center samples, equality, censoring, unavailable ATR,
initial distance, profile/day contributions, interval overlap and leave-one-
original-profile-out influence remain explicit. This is descriptive research,
not causal identification, effective-sample-size estimation or execution.

## Consequences

Old definitions and v1 frozen graphs keep their meaning. New instances opt into
v2 and new Checks pin definition 9. Version selection is a real runtime boundary,
not a consumer output toggle. The metadata envelope avoids a schema cutover but
must remain excluded from public Indicator parameters. Supported versions are
listed by the type-details API. Source/runtime admission still precedes research
execution; the new definition must never be sent to a runtime that ignores it.

## Validation

Synthetic tests cover strict/equality returns, fixed references through profile
changes, concurrent origins, prefix stability, prior ATR, gaps, old-output engine
equivalence, metadata round trips, fixed-clock outcomes, censoring and eligibility.
The disposable database workflow creates v2, freezes mixed-timeframe candles,
runs the registered Check and exactly replays it with provider access trapped.
