---
component: adr-canonical-accounting-reconciliation
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - accounting
  - reconciliation
  - fills
  - wallet
  - cleanup
code_paths:
  - src/engines/bot_runtime/core/execution_adapter.py
  - src/engines/bot_runtime/core/wallet.py
  - src/engines/bot_runtime/core/wallet_gateway.py
  - portal/backend/service/reports/instrument_semantics.py
  - portal/backend/service/reports/run_research_dataset.py
  - docs/architecture/execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md
---
# ADR 0043: Reconcile Accounting From Canonical Fills And Wallet Ledger

## Status

Accepted on 2026-07-25.

**Retroactive cleanup ADR:** this records the accounting and reconciliation
boundary proved during the cleanup correctness campaign.

## Context

Orders, fills, position state, trade rows, wallet effects, fees, margin, and
reports all describe parts of the same economic result. If any of those views
can invent or independently correct financial truth, a run may look plausible
while cash, position, P&L, or ending equity does not reconcile.

## Decision

An accepted canonical fill is the economic trigger. Runtime applies each fill
through the shared wallet gateway, commits wallet state in causal
`wallet_commit_seq` order, and then publishes the corresponding durable
evidence. Position and trade lifecycle rows describe execution state; they do
not independently own cash or fee truth.

Reporting rebuilds financial results from durable fill, wallet, position, and
lifecycle evidence and performs independent reconciliation. A mismatch is a
blocking correctness defect or explicit caveat, never an opportunity for the
report to silently repair runtime state.

Report instrument semantics prefer the persisted runtime-readiness execution
profile. Canonical fill `accounting_mode` may complete missing report
accounting metadata. Spot accounting also proves spot execution semantics;
margin accounting does not distinguish derivative from proxy-derivative and
cannot invent either. Untyped execution-semantics fields on fill payloads are
not authority. Duplicate, ambiguous, invalid, or conflicting configured/fill
evidence blocks report construction.

## Invariants

- Every accepted fill affects position and accounting exactly once.
- Entry and exit fees use the same canonical executed-notional contract and are
  applied exactly once.
- Margin reservation is collateral, not realized P&L, and is fully released
  when the associated exposure is settled.
- Wallet replay is causal by `wallet_commit_seq` and idempotent for duplicate
  durable fill evidence.
- Long and short positions use symmetric, explicit sign conventions.
- Cash, fees, realized P&L, unrealized P&L, locked collateral, free collateral,
  and ending equity must reconcile to the run's durable economic events.
- Terminal runs cannot retain unexplained open exposure or locked margin.
- Report instrument identity is unique by instrument ID, with symbol-only
  matching permitted only when an ID is unavailable.
- Spot fill accounting may complete spot execution semantics; margin fill
  accounting cannot infer derivative or proxy-derivative semantics.
- Untyped fill fields cannot change configured execution semantics or report
  semantic fingerprints.

## Consequences

Runtime and reporting can be checked independently against a small reference
dataset. Additional projections may summarize accounting, but must remain
rebuildable and must expose disagreement rather than choose a convenient view.
Stricter report reconstruction may reject runs whose configured instrument
profiles are ambiguous or internally contradictory.

## Rejected Alternatives

- Treat trade-row P&L as the sole accounting authority.
- Let reports derive a replacement wallet state when runtime evidence disagrees.
- Maintain separate backtest and paper accounting implementations.
- Apply fees in both execution and reporting.
- Resolve duplicate events by applying the latest value again.
- Trust an untyped fill payload to override configured execution semantics.
- Infer derivative execution semantics from margin accounting alone.
- Keep the first duplicate instrument profile and silently discard the rest.

## Enforcing Tests Or Evidence

- `tests/integration/runtime/test_reference_execution_scenarios.py` covers
  hand-verifiable accounting, repeatability, terminal reconciliation, and
  backtest/paper parity.
- `tests/test_portal/test_wallet_ledger.py` covers canonical replay, causal
  ordering, complete state, malformed initialization, and no float drift.
- `tests/test_portal/test_wallet_margin_locking.py` covers reservation,
  idempotent settlement, commit order, and terminal margin release.
- `tests/test_portal/test_fee_notional_cleanup.py` covers notional, fee
  symmetry, deterministic fees, and duplicate-fill protection.
- `tests/test_portal/test_report_instrument_semantics.py` and
  `tests/test_portal/test_run_research_dataset.py` prove canonical spot fills
  complete missing report instrument semantics deterministically and
  conflicting configured accounting evidence fails loudly.
- Cleanup evidence commit `a0e196e` records persisted wallet/fill/trade
  reconciliation for repeated representative runs.

## References

- [ADR 0013: Market-Time Ordering For Shared-Wallet Backtests](0013-use-market-time-ordering-for-shared-wallet-backtests.md)
- [ADR 0014: Shared-Wallet Arbitration By Runtime Mode](0014-use-shared-wallet-arbitration-policy-by-runtime-mode.md)
- [ADR 0027: Execution Profiles As Instrument Authority](0027-use-execution-profiles-as-runtime-instrument-authority.md)
- [Wallet And Capital Boundary](../execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md)
