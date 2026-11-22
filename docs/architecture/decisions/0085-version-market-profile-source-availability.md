---
component: adr-market-profile-source-availability
subsystem: indicator-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - indicators
  - known-at
  - causality
  - research
code_paths:
  - src/indicators/market_profile/definition.py
  - src/indicators/market_profile/manifest.py
  - src/indicators/market_profile/compute/engine.py
  - src/indicators/market_profile/compute/internal/availability.py
  - src/indicators/market_profile/runtime/typed_indicator.py
  - portal/backend/service/indicators/indicator_service/runtime_graph.py
  - tests/test_indicators/test_market_profile_availability.py
  - tests/test_portal/test_indicator_runtime_source_fetch.py
---
# ADR 0085: Version Market Profile source availability

## Why a deterministic replay was still early

The deployed `cd58b2166542647d31052474cae29d61bfbedd92` implementation computes
daily profiles from native 30-minute candles. `Profile.end` is the final candle
open. Preparation projected that structural timestamp into `known_at`, and
runtime compared it with the chart candle open. On a 15-minute chart this
admitted a full daily profile on the 23:30 chart candle, available at 23:45,
even though the included 23:30 source candle closes at 00:00. This violates
[known-at prefix invariance](0044-enforce-known-at-prefix-invariance.md).

## Decision

An explicit v3 manifest preserves v2's outputs and all TPO/value-area/merge
calculations. Its preparation separately retains source availability and
structural geometry. Complete native UTC days become available at the maximum
of session close and all included source `known_at` values, then project to the
chart close grid. Runtime admits them using chart candle availability or close,
never its open. No runtime migration or historical instance update occurs.
V3 requires a fixed finite positive bin size, including the existing explicit
default. Automatic whole-window inference would itself violate prefix invariance
when future source prices change, so it is rejected for this version.

Incomplete native days are withheld with a diagnostic. Admitting a truncated
daily aggregate would make prefix outputs depend on future source rows. V3
therefore requires the 48 unique aligned 30-minute native bars; it does not
manufacture missing bars or claim a sparse session is complete. This is a new
version's readiness restriction, not a change to the profile calculations.
Canonical source clocks are validated; absent legacy clock columns use explicitly
labeled native-close inference. Whole-window continuity remains build evidence,
excluded from causal per-bar overlay snapshots.

The generic graph passes a pinned version to source builders that explicitly
support that keyword. Existing builders retain their calling contract. V1/v2
manifests and runtime behavior remain available for historical replay. Their
successful hashes cannot support new predictive causal claims. New research
creates a separate v3 instance and pins its graph and code revision.

## Evidence and scope

`tests/test_indicators/test_market_profile_availability.py` reproduces the old
23:45 leak beside v3, compares six chart timeframes, checks delayed earlier
source rows and chart availability, rejects malformed clocks, and compares every
typed output and overlay across native-source prefixes and changed future
suffixes in merged/unmerged mode. The graph source-fetch tests prove version
selection reaches preparation, rather than selecting v3 only at runtime.

The original November 7 native-MP evidence remains at
`C:/Users/elija/Documents/QT Reports/reversal-exploration-20261107/native_mp_followup`.
At assessment, `population_context_audit.json` had SHA-256
`5a5068ff8b7d2cf64204fbe8915dfa9ac40f4271bd5162d43e9ef7bbbe3cb234` and reported
16 exports, 10,430 candles, 10,398 context rows and 69 directly early rows. The
ETP V03 export hash was
`de0371b19cf1ef045b50f2150a382df360906ecc5c23f0564e3c69c9ee87487a`, Dataset
`mds_6f51270554e2e978221f7e0d8b1f7cea`, with four early rows. These are bounded
audit observations, not an exhaustive inventory of production research.

Directly early rows are a lower bound on affected output: breakout confirmation,
reclaim/retest, first-return origins and later state may inherit an early profile
transition. Recompute affected MP-derived evidence under a new v3 identity before
new causal claims; shifting or deleting the flagged rows cannot repair stateful
outputs. Frozen native candles, original reversal outcomes and non-MP evidence
are not rewritten. Original manifests, exports, Dataset identities, hashes and
source revision remain historical evidence. This change does not certify the
scientific interpretation of prior or future research.
