---
component: adr-shared-wallet-backtest-market-time-ordering
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - backtest
  - deterministic
  - shared-wallet
  - wallet
code_paths:
  - src/engines/bot_runtime/runtime/components/entry_decision_ordering.py
  - src/engines/bot_runtime/runtime/mixins/execution_loop.py
  - src/engines/bot_runtime/runtime/components/runtime_policy.py
  - docs/architecture/execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md
---
# ADR 0013: Use Market-Time Ordering For Shared-Wallet Backtests

## Status

Accepted on 2026-05-13.

## Context

Quant-Trad supports multi-symbol bots. In backtest mode, symbol workers may
advance candle, indicator, signal, and diagnostic computation at different
speeds. Those symbols can still share one wallet.

If a future-bar candidate reserves shared wallet margin before another
wallet-sharing participant has resolved earlier market time, repeated runs can
produce the same signals but different wallet state, sizing, rejection verdicts,
and trade lifecycle. Runtime publication order and `run_seq` are not semantic
trading clocks for this decision.

## Decision

Shared-wallet backtests commit wallet-affecting candidates in deterministic
market-time order.

A wallet-affecting candidate at bar time `T` cannot commit while any
wallet-sharing participant may still produce a wallet-affecting candidate at
time `<= T`. A participant with a next unresolved bar before or equal to `T`
blocks release unless it is complete, inactive, failed according to current
runtime semantics, or covered by valid no-bar/gap evidence.

This gates wallet mutation, not all symbol computation. Symbol workers may
compute ahead for candles, indicators, signals, candidate decisions, and
diagnostics, but shared wallet effects must wait until portfolio-time release
is safe.

## Consequences

- Repeated shared-wallet backtests cannot diverge because a future symbol
  reserved margin before unresolved earlier/equal participant work.
- Same-bar candidates still release in deterministic candidate order after
  relevant participants have resolved that bar.
- Sparse symbol timelines do not require full lockstep candle computation.
- Wallet replay continues to use wallet-scoped clocks and facts; `run_seq`
  remains operational ledger order, not semantic trading order.
- Missing or ambiguous participant progress must fail loud or remain blocked
  rather than silently allowing a future-bar wallet mutation.

## References

- [Wallet and capital boundary](../execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md)
- [Runtime contract](../../contracts/platform/01_runtime_contract.md)
- [ADR 0007: Use scoped causal clocks for runtime replay](0007-use-scoped-causal-clocks-for-runtime-replay.md)
- [ADR 0016: Treat runtime event ledger order as operational evidence](0016-treat-runtime-event-ledger-order-as-operational-evidence.md)
