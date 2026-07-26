---
component: adr-known-at-prefix-invariance
subsystem: engine
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - known-at
  - causality
  - no-lookahead
  - deterministic
  - cleanup
code_paths:
  - src/engines
  - portal/backend/service/research
  - portal/backend/service/reports
  - portal/backend/service/storage/repos/runtime_events.py
  - docs/architecture/engine/ENGINE_STATE_MODEL.md
---
# ADR 0044: Enforce Known-At Prefix Invariance

## Status

Accepted on 2026-07-25.

**Retroactive cleanup ADR:** this records the causal semantics already required
by the runtime and strengthened during baseline cleanup.

## Context

A deterministic replay is still invalid if an earlier output changes when
future candles are appended. Indicator state, strategy decisions, order
eligibility, execution, research checks, and reports need one auditable meaning
for when a fact became available.

## Decision

The platform uses one walk-forward timeline. At bar `t`, strategy evaluation
may consume only source rows and typed runtime outputs whose `known_at` is no
later than the decision boundary for `t`. Decisions, orders, fills, lifecycle
events, and report projections preserve their causal time; wall-clock ingestion
time never substitutes for missing market-time evidence.

For the same initial state and data prefix, extending the dataset with a future
suffix must not alter semantic outputs inside the original prefix. Reporting
projects persisted decisions and outputs; it does not rerun hidden indicator or
strategy state.

A decision produced from a completed candle is known only at that candle's
close. An immediate market entry may fill at the signal close, but the new
position cannot consume that candle's earlier high/low as stop or target
evidence. Its first ordinary exit-eligible price path begins with the next
candle. An explicit terminal close may still close a newly opened position at
the final bar close because it consumes the known close, not an earlier
intrabar range.

## Invariants

- Future source rows cannot change an earlier indicator snapshot or decision.
- Every strategy-visible input is a typed same-bar `RuntimeOutput`.
- Signal time, order eligibility time, fill time, event time, and persistence
  time remain distinct where their semantics differ.
- A signal-close maker order cannot fill from the already-known signal bar.
- No newly opened signal-close position can consume the signal bar's earlier
  range as executable exit evidence.
- Warmup and insufficient-history limitations are explicit evidence, not
  silently valid decisions.
- Missing causal timestamps fail or block certification; they do not fall back
  to wall clock.

## Consequences

Prefix-truncation tests become the primary proof against look-ahead leakage.
Cached or projected views may optimize reads only if they preserve the same
causal evidence. Features whose known-at contract cannot be stated cannot be
promoted as trustworthy research inputs.

## Rejected Alternatives

- Vectorized full-window evaluation without prefix-equivalence evidence.
- Use event creation time when market known-at is absent.
- Recompute historical decisions in reporting from today's mutable config.
- Treat visual playback order as execution causality.

## Enforcing Tests Or Evidence

- `tests/test_portal/test_research_checks.py::
  test_raw_event_detection_is_invariant_to_future_candle_suffix` proves prefix
  invariance for raw research checks.
- `tests/test_portal/test_report_data.py` proves decision-ledger known-at is
  durable and an existing prefix is unchanged by later events.
- `tests/test_portal/test_runtime_events_repo.py` rejects wall-clock known-at
  fallback for trade facts.
- `tests/integration/runtime/test_bot_runtime_entry_execution.py::
  test_limit_maker_entry_does_not_fill_from_signal_bar_range` protects the
  signal-close boundary.
- `tests/integration/runtime/test_reference_execution_scenarios.py::
  test_known_at_pipeline_is_invariant_to_future_candle_suffix` compares exact
  consumed candle fingerprints, indicator outputs and overlays, decisions,
  generated orders, fills, position lifecycle, and wallet accounting through
  the original cutoff for both backtest and paper adapters.
- `tests/integration/runtime/test_persisted_runtime_correctness.py` runs the
  production runtime, persistence, artifact, report-summary, backtest adapter,
  and paper adapter paths over a credential-free reference dataset. It proves
  exact semantic repeatability, prefix truncation through the known-at
  boundary, signal-close entry followed by next-bar target exit, lifecycle and
  wallet reconciliation, and persisted candle identity/continuity evidence.

## References

- [ADR 0002: One Walk-Forward Runtime Timeline](0002-use-one-walk-forward-runtime-timeline.md)
- [ADR 0007: Scoped Causal Clocks For Runtime Replay](0007-use-scoped-causal-clocks-for-runtime-replay.md)
- [Engine State Model](../engine/ENGINE_STATE_MODEL.md)
