---
component: adr-shared-wallet-arbitration-policy-runtime-mode
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - backtest
  - runtime-mode
  - shared-wallet
  - wallet
code_paths:
  - src/engines/bot_runtime/runtime/components/runtime_policy.py
  - src/engines/bot_runtime/runtime/components/entry_decision_ordering.py
  - src/engines/bot_runtime/runtime/mixins/setup_prepare.py
  - docs/architecture/execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md
---
# ADR 0014: Use Shared-Wallet Arbitration Policy By Runtime Mode

## Status

Accepted on 2026-05-13.

## Context

Shared-wallet ordering has two responsibilities that change at different rates.
The coordinator owns the mechanics of participants and candidates. Runtime mode
determines what it means to wait, release, fail, or time out.

Backtests need market-progress semantics. A correct backtest wait should not
degrade solely because a wall-clock timeout elapsed while slower participants
are still validly behind in market time. Paper and live modes are not
implemented yet, but they will likely need bounded real-time decision windows
and late-event policies.

## Decision

Shared-wallet ordering is coordinator-owned. Shared-wallet arbitration semantics
are policy-owned.

The entry-decision coordinator owns candidate arrival, participant progress,
candidate buffering, deterministic sorting, release orchestration, and
missing/blocking participant tracking.

The arbitration policy owns wait/release/fail semantics, timeout or deadlock
behavior, late or missing participant behavior, and diagnostic reason. The
backtest policy reasons about market progress. The current default
non-backtest policy preserves bounded wall-clock timeout behavior as a
compatibility fallback; it is not a paper or live implementation.

## Consequences

- Backtests can wait on portfolio market progress without reintroducing
  future-bar wallet overtakes.
- The coordinator does not grow runtime-mode branches for every arbitration
  semantic.
- Paper and live can attach bounded wall-clock decision-window policies later
  without renaming or replacing the coordinator.
- The seam remains small: it does not own wallet math, fills, execution
  adapters, candle ingestion, BotLens projection, or bounded async
  backpressure.
- Overbuilding a broad paper/live execution abstraction is deferred until those
  modes exist.

## References

- [Wallet and capital boundary](../execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md)
- [Runtime composition root](../execution-runtime/RUNTIME_COMPOSITION_ROOT.md)
- [ADR 0013: Use market-time ordering for shared-wallet backtests](0013-use-market-time-ordering-for-shared-wallet-backtests.md)
- [ADR 0012: Use a runtime composition root for mode-aware wiring](0012-use-runtime-composition-root-for-mode-aware-wiring.md)
