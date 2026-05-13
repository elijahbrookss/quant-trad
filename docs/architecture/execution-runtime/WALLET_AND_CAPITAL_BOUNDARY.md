---
component: wallet-and-capital-boundary
subsystem: execution-runtime
layer: engine
doc_type: architecture
status: active
tags:
  - wallet
  - margin
  - capital
  - settlement
  - runtime
code_paths:
  - portal/backend/service/bots/container_runtime.py
  - src/engines/bot_runtime/core/wallet.py
  - src/engines/bot_runtime/core/wallet_gateway.py
  - src/engines/bot_runtime/core/margin.py
  - src/engines/bot_runtime/core/fees.py
  - src/engines/bot_runtime/core/entry_settlement.py
  - src/engines/bot_runtime/core/exit_settlement.py
  - src/engines/bot_runtime/runtime/components/entry_decision_ordering.py
  - src/engines/bot_runtime/runtime/mixins/runtime_events.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - docs/architecture/execution-runtime/diagrams/wallet-capital-flow.mmd
---
# Wallet And Capital Boundary

## Purpose

The wallet and capital boundary protects deterministic capital accounting across single-symbol and symbol-sharded runs. It is where entry decisions become reservations, fills become exposure, and exits become settlement.

Related diagram: [wallet-capital-flow.mmd](diagrams/wallet-capital-flow.mmd).

## Boundary Contract

Decision artifacts cannot spend capital directly. Runtime asks wallet/capital components to reserve, validate, settle, and release capital.

This boundary owns:

- cash and collateral availability,
- margin checks,
- fee application,
- reservation lifecycle,
- entry and exit settlement,
- deterministic ordering for competing entry decisions.

It does not own:

- strategy rule evaluation,
- provider candles,
- BotLens display state,
- report aggregation.

Decision-time wallet evidence is part of the runtime contract. Accepted entry
decisions and wallet/margin rejections must carry the deterministic wallet
snapshot and margin requirement that caused the verdict, using simulated
bar/known-at time for causality and wall-clock time only as observation
metadata.

## Diagram Walkthrough

[wallet-capital-flow.mmd](diagrams/wallet-capital-flow.mmd) shows:

1. A selected entry decision asks for capital.
2. Deterministic ordering arbitrates competing candidates.
3. Wallet gateway validates and reserves margin/collateral.
4. Execution opens, updates, or closes trades.
5. Settlement releases or adjusts reservations.
6. Wallet/runtime events make capital state auditable.

## State And Truth

Wallet truth is runtime truth. Reports and BotLens can project wallet effects, but they should not recompute a different wallet history.

`WALLET_INITIALIZED` is a run-scoped ledger transition owned by the container
runtime/coordinator. A shared-wallet run must publish exactly one canonical
mutating initialization fact for the run wallet before worker execution. That
write goes through the canonical fact appender, not the live BotLens projection
transport, so durable wallet truth is not dependent on a symbol-series route.
Symbol workers attach to that shared wallet state; they must not emit their own
canonical `WALLET_INITIALIZED` rows. Duplicate initialization with different
state is a ledger defect, not a replay concern.
When a runtime engine attaches the shared wallet gateway, both entry settlement
and exit settlement must use that same gateway. Exit fills that bypass the
shared gateway cannot allocate `wallet_commit_seq`, cannot produce canonical
release facts, and must fail rather than emit clockless wallet ledger rows.

Reservation lifecycle should be inspectable:

```text
candidate decision -> reservation -> fill -> trade lifecycle -> settlement -> release
```

Wallet ledger facts derived from runtime events preserve two order concepts:

- `run_seq` is the durable event-row ordering assigned by persistence.
- `wallet_commit_seq` is the shared-wallet causal clock assigned by the wallet
  gateway or coordinator. Wallet replay must order ledger facts by this clock
  plus `wallet_event_order`; missing wallet commit clocks are malformed modern
  ledger facts, not rows to be inferred from runtime publication order.
- `position_commit_seq` is a position-scoped lifecycle clock carried on
  material trade facts and on related wallet facts when a position transition is
  involved. It gives position replay a durable causal sequence without turning
  every domain into a single global lock.
- the shared wallet gateway updates a committed wallet-state snapshot and
  appends a minimal internal fill marker under its process lock before
  runtime-event transport append; this locked snapshot is the source of current
  wallet state for subsequent decisions and settlements.
- `source_run_seq` remains diagnostic lineage only. It is not a wallet replay
  ordering fallback.

Runtime emits close settlement facts in this wallet order:

```text
MARGIN_RELEASED -> FEE_APPLIED -> REALIZED_PNL_APPLIED -> POSITION_CLOSED -> EQUITY_UPDATED
```

The absolute `wallet_before` for fill and release facts must come from the
wallet gateway's committed settlement metadata. Runtime-event append order is
transport, not the synchronization point for capital state. The persisted
before/after state and wallet replay must agree.

When multiple exit fills for the same trade are emitted before the next durable
wallet fact batch, wallet fact construction must continue from the prior
committed wallet fact for that trade. A stale per-exit source snapshot may not
reset the same trade's open quantity, locked margin, balance, or free
collateral. This keeps partial target releases replayable without relying on
live transport timing.

Consumed reservations remain inspectable for rollback/release diagnostics, but
they are not counted as active collateral holds after the committed wallet-state
snapshot has absorbed the fill.

## Failure And Recovery

- Insufficient cash/margin rejects the decision.
- Missing instrument metadata fails before settlement math depends on it.
- Reservation leaks are runtime defects and should be visible in diagnostics/reports.
- Shared-wallet ordering must not turn sparse symbol timelines into a global
  bar barrier. It arbitrates candidates within the same bar key; cross-bar
  wallet causality is proven by `wallet_commit_seq`.
- Same-bar shared-wallet candidates wait until expected participants have either
  arrived or advanced past that bar, then mutate wallet state in stable
  candidate order.

## Invariants

- Capital checks happen before a trade is opened.
- Fees and margin are execution concerns, not reporting-only adjustments.
- Wallet state changes are tied to runtime events and trade lifecycle.
- Wallet/margin decision verdicts are auditable from the canonical runtime
  event ledger without relying on arrival order or projection state.
- Wallet replay uses wallet commit ordering for ledger facts and must fail loud
  on missing clocks, malformed rows, or stale absolute state.
- Terminal close behavior must release or account for reserved capital.

## Related Docs

- [Execution runtime boundary](EXECUTION_RUNTIME_BOUNDARY.md)
- [Identity and correlation boundary](../identity/IDENTITY_AND_CORRELATION_BOUNDARY.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
