---
component: adr-scoped-causal-clocks-runtime-replay
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - runtime-events
  - wallet
  - replay
  - deterministic
code_paths:
  - src/engines/bot_runtime/core/wallet_gateway.py
  - src/engines/bot_runtime/runtime/components/entry_decision_ordering.py
  - src/engines/bot_runtime/runtime/components/overlay_delta.py
  - src/engines/bot_runtime/runtime/mixins/runtime_events.py
  - portal/backend/service/storage/repos/runtime_events.py
  - docs/architecture/execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md
---
# ADR 0007: Use Scoped Causal Clocks For Runtime Replay

## Status

Accepted, backfilled on 2026-05-13.

## Context

Runtime event arrival order is not enough to replay every domain correctly.
Shared-wallet runs, symbol-sharded workers, indicator outputs, position
lifecycle events, and overlay deltas each have different causal scopes. Forcing
all domains through one global lock would be simple, but it would make sparse
symbols and high-volume projection paths block unnecessarily.

## Decision

Replay uses scoped causal clocks:

- `run_seq` orders durable event rows within a run.
- `wallet_commit_seq` plus `wallet_event_order` orders wallet ledger facts.
- `position_commit_seq` orders material lifecycle transitions for one trade.
- `indicator_commit_seq` orders indicator output transitions for one indicator.
- `overlay_commit_seq` orders viewport overlay projection deltas.

Each clock is owned by the boundary that mutates that state.

## Consequences

- Wallet replay does not infer order from runtime publication order.
- Closed-trade and wallet facts can dominate stale live projection messages.
- Symbol-sharded shared-wallet runs can arbitrate real entry candidates without
  turning sparse no-candidate bars into a global barrier.
- Missing or malformed modern clocks are correctness defects and should block
  certification instead of being patched by readers.

## References

- [Runtime contract: shared-wallet entry ordering](../../contracts/platform/01_runtime_contract.md)
- [Wallet and capital boundary](../execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)

