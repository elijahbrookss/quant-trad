---
component: adr-market-profile-raw-breakout-retests
subsystem: indicator-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - indicators
  - market-profile
  - lifecycle
  - research-validation
code_paths:
  - src/indicators/market_profile/runtime/signal_state.py
  - src/indicators/market_profile/runtime/typed_indicator.py
  - src/indicators/market_profile/manifest.py
  - tests/test_indicators/test_market_profile_signal_state.py
  - docs/architecture/indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md
  - docs/architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md
---
# ADR 0036: Anchor Market Profile Retests On Raw Breakouts

## Status

Accepted on 2026-06-13.

## Context

The Market Profile breakout/retest family originally formed retest lifecycle
candidates only after `confirmed_balance_breakout` emitted. Recent signal-family
audits showed that the confirmation gate reduced samples without clearly
improving forward outcomes. That made the retest lifecycle inherit a filter that
had not earned authority over retest quality.

Retest quality is a separate question from breakout confirmation quality. A
structural retest should be judged by whether price broke out, accepted outside
the value area, returned to the reference, and held. Waiting for the confirmed
breakout signal before starting the retest clock delays and filters that
candidate path for the wrong reason.

## Decision

Market Profile retest lifecycle candidates start from the raw
`balance_breakout` signal.

The retest lifecycle now records:

- `formed` when the raw breakout occurs,
- `eligible` when price has remained outside value long enough and moved far
  enough from the reference,
- `touched` when price returns to the retest band without exceeding maximum
  penetration,
- terminal `confirmed`, `invalidated`, or `expired` outcomes.

`confirmed_balance_breakout` remains a public signal, but it is no longer the
source event for retest lifecycle evidence. It may occur after the retest
candidate has already formed or become eligible. If retest parameters allow it,
a retest may confirm before the breakout confirmation signal.

Reclaim remains separate. It is reserved for confirmed breakouts that return
inside value before becoming accepted retest candidates. Once a candidate has
accepted outside value, a later reference touch and hold belongs to the retest
family rather than reclaim.

Retest metadata and lifecycle metrics use breakout-based names such as
`outside_bars_since_breakout` and `bars_since_breakout`. Confirmation fields are
optional sequence facts, not the source of retest truth.

## Consequences

- Retest research is no longer filtered by an unproven confirmation edge.
- Lifecycle checks can measure the full raw-breakout-to-retest funnel.
- Reclaim counts may fall because accepted pullbacks now resolve as retests.
- Historical reports generated before this ADR should not be compared to new
  retest reports as if the signal semantics were unchanged.
- Strategies that explicitly consume `confirmed_balance_breakout` still use the
  same outside-value confirmation rule, but regenerated runs may surface small
  count changes because retest/reclaim sequence ownership no longer treats the
  confirmed breakout as the retest parent.
- Strategies that consume `balance_retest` will see the new raw-anchored retest
  definition after indicators/runs are regenerated.

## References

- [Indicator Runtime Boundary](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md)
- [Research Memory Boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [ADR 0034: Use Research Checks As Analytical Memory Evidence](0034-use-research-checks-as-analytical-memory-evidence.md)
