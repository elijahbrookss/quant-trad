---
component: adr-market-profile-observed-sessions
subsystem: indicator-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - indicators
  - research
  - known-at
  - causality
code_paths:
  - src/indicators/market_profile/manifest.py
  - src/indicators/market_profile/definition.py
  - src/indicators/market_profile/compute/internal/observed_sessions.py
  - src/indicators/market_profile/compute/internal/observed_runtime.py
  - src/indicators/market_profile/compute/internal/runtime_profiles.py
  - src/indicators/market_profile/runtime/typed_indicator.py
  - tests/test_indicators/test_market_profile_observed_sessions.py
---
# ADR 0086: Retain observed incomplete Market Profile sessions

## Why completeness and useful context are different

MP v3 correctly prevents source-open availability leakage, but its 48-bar gate
also excludes a fully elapsed day when just one native candle is absent. That
changes merge membership, POC means and merge eligibility for later days. A
useful observed-data measurement need not claim that missing observations were
quiet or that the session was complete.

The supplied November 7 frozen evidence does not establish scheduled Friday
closures. Its underlying receipts say `provider_missing_data`, detected during
November acquisition, without historical closure availability. The normalized
`disclosed_closure` label means the gap was disclosed for range admission. It is
not a producer completeness proof. Neither recurrence, instrument enrollment
metadata nor a successful acquisition retrospectively establishes such proof.

## Decision

An explicit `market_profile` v4 retains profiles computed from the observed
native bars of a closed UTC day. It publishes the observed fraction, exact
missing slots, source identity, lineage hash and availability methods through
the new typed `profile_quality` context. V1, v2 and v3 retain their manifests,
arithmetic, readiness and frozen replay behavior. No existing instance changes
version, and no historical evidence is rewritten.

V4 requires explicit timezone-bearing source `known_at`, native `close_time`, one source identity,
unique aligned native opens, valid OHLC and a fixed finite positive bin size.
Missing or unparseable availability fails preparation with
`market_profile_availability_unknown`; neither close inference inside MP nor
gap detection/import time substitutes for it. An explicit canonical candle
whose `known_at_method` is `interval_close_inferred` may still be used, with that
assumption exposed. It is not exact historical provider publication or receipt.
An unspecified method is also disclosed as an availability assumption.

The first profile event is no earlier than UTC session close and the included
rows' availability, projected onto the chart close grid. At each later source
availability, preparation may supply a new immutable revision computed only
from rows available by that cutoff. An unfinished day supplies no profile.
There are no synthesized missing bars or all-empty synthetic sessions.
Whole unobserved days between active member sessions are disclosed as compact
`unobserved_session_ranges` and included in that cluster span's missing-slot
count. They are not inserted as profile members or treated as quiet sessions.
There is no minimum coverage fraction beyond at least one observed bar. The
existing equal weighting of member daily POCs is retained even when a member is
incomplete; missingness and member-deletion influence must accompany interpretation.

The engine replaces that day's revision rather than appending another session.
It reuses the existing merge owner on the current available daily set in session
order. Earlier snapshots stay immutable. The active profile identity includes
the member-source lineage hashes, so an interior late row changes identity even
when the structural span or levels happen to remain equal. This replacement is
a new causal state transition, never a rewrite of previously emitted evidence.
Replacing an older day may consequently change today's merge boundaries or
eligibility, including the current representation of a previously closed
cluster. Previously published cluster snapshots and original first-return
references remain fixed.

## What quality does and does not establish

`complete_observed_slots` verifies that all expected native slots are represented
in the selected observed inputs. It does not certify venue history, correction
finality, exact transaction data or scientific suitability. `observed_incomplete`
retains a useful subset with explicit missingness. `closure_verified` remains
false; this version consumes no closure proof. Unknown availability is a source
admission failure, rather than a guessed usable profile.

Every ready quality output explicitly carries `predictive_qualification=false`.
Output readiness means the declared observed-data measurement is available;
it does not certify predictive research. The Check's pinned version, quality
interpretation, inputs and scientific authority remain separate obligations.

The merged POC sensitivity fields report the range and largest change when one
observed member session is omitted from the mean. They do not re-estimate a
missing candle, recompute alternative merge boundaries, provide confidence
intervals or bound missing-data bias. `missing_bar_price_impact` remains
`unknown`. A single member has no leave-one-session-out estimate.

## Input custody and limits

V4 consumes the canonical frame actually supplied by the pinned runtime graph.
The existing frozen candle reader's interval-close revision selection is
unchanged. This version does not acquire additional candles or retrieve an
unretained correction history. Its late-row revision mechanism applies when
late-known rows are present in the selected source input; it cannot recover a
row omitted by upstream admission. Duplicate source-open revisions are rejected
rather than silently selecting a correction. Chart intervals must be positive
and at most one day. Full-window diagnostics remain outside causal overlays.

## Validation

Tests retain sparse days with one or two missing native bars and compare their
post-close metric values with legacy observed-subset arithmetic. They cover six
chart intervals, unknown availability, unverified future gap labels, inferred
clock disclosure, incomplete window edges, mixed sources, late interior rows,
older-day replacement without duplicate session count, merge sensitivity and
immutable snapshots. Prefix tests compare every typed output, first-return
state and overlay under truncated input and changed future rows. Source-fetch
and frozen-plan tests verify explicit v4 selection. These proofs do not replace
research comparison or exact-release qualification.
