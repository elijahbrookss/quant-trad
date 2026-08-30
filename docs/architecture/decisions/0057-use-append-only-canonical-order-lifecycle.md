---
component: adr-append-only-canonical-order-lifecycle
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - execution
  - orders
  - lifecycle
  - replay
  - accounting
code_paths:
  - src/engines/bot_runtime/core/order_lifecycle.py
  - src/engines/bot_runtime/core/execution_order.py
  - src/engines/bot_runtime/core/entry_execution.py
  - src/engines/bot_runtime/core/domain/position.py
  - src/engines/bot_runtime/core/runtime_events.py
  - portal/backend/service/reports/run_research_dataset.py
---
# ADR 0057: Use An Append-Only Canonical Order Lifecycle

## Status

Accepted on 2026-08-05 after canonical-lifecycle implementation and
acceptance testing.

## Context

`FillOrder` made immediate fill semantics explicit, and
`ResolvedExecutionContext` pinned the instrument, venue, fee, and model context. It still could not serve as a durable
order: it had no generic acceptance/open/partial/cancel/replace/expiry state,
residual ownership, restart replay, or idempotent event history. Adding book
execution or live reconciliation directly to that shape would either scatter
state across adapters or create a parallel ledger beside canonical accounting.

## Decision

Use one venue-neutral, immutable-request, append-only canonical order lifecycle.
Each order has a stable request identity, one or more immutable attempt
identities, a strict generic transition graph, cumulative and residual quantity,
replacement lineage, idempotent event and fill identities, and deterministic
replay hashes. The execution-context hash and order-policy hash remain
fixed for the request lifetime.

The lifecycle owns order state and lineage only. Fills remain inputs to the
existing canonical position, wallet, fee, PnL, event, and reconciliation owners.
`FillOrder` remains a transitional immediate-execution adapter behind this
authority; it is not renamed or expanded into a venue-order monolith.

Venue profiles translate lifecycle terminology and capabilities. They do not
own the generic transition graph. Generic code must not branch on venue names.

## Invariants

- Request, attempt, event, and fill identity is deterministic from pinned causal
  material for valid runtime orders.
- Order state is append-only; illegal transitions and divergent duplicate IDs
  fail closed.
- Sequence numbers are contiguous and replay reproduces identical manifests,
  state, cumulative quantity, residual, lineage, and hashes.
- Cumulative fill quantity never decreases or exceeds requested quantity.
- Replacement is atomic and admits exactly the predecessor residual.
- Fill wins a deterministic same-boundary race with cancel or replacement;
  later action uses the post-fill residual.
- A lifecycle fill is not accounting settlement. Every production-wired fill
  settles once through existing owners, and no alternate ledger is allowed.
- The runtime event ledger is canonical; BotLens and reports are projections.
- Current X0-X2 bar models remain full-fill. A partial entry cannot be abandoned
  until per-fill incremental entry accounting is implemented.
- Lifecycle evidence does not raise execution quality or authorize external
  order submission.

## Consequences

- Entry, pending maker fallback, exit, stop, target, terminal, and intrabar paths
  share one auditable order-state contract.
- Partial exits preserve residual position/order quantity and settle per fill.
- Runtime fills can name the exact filled lifecycle event as their parent.
- Restart and report reconstruction no longer infer order state from trade rows.
- L2, queue, shadow, and live-calibration work can reuse the contract without
  coupling matching mechanics or venue adapters to strategy semantics.
- Book-driven partial entry settlement remains unsupported; the current runtime
  fails closed rather than allowing filled quantity to disappear.

## Rejected alternatives

- Keep `FillOrder` as the permanent order abstraction.
- Put lifecycle state inside Coinbase, Kraken, paper, or backtest adapters.
- Derive order state from fills and trade rows after the fact.
- Create a second position/wallet ledger for realistic execution.
- Allow adapters to patch residual quantity or change a pinned context.
- Use random identities for valid replayable runtime orders.
- Enable partial entry cancellation before incremental accounting exists.

## Enforcing evidence

- `tests/integration/runtime/test_canonical_order_lifecycle.py`
- `tests/integration/runtime/test_bot_runtime_entry_execution.py`
- `tests/integration/runtime/test_persisted_runtime_correctness.py`
- `tests/integration/runtime/test_runtime_push_stream.py`
- `tests/test_portal/test_botlens_domain_events.py`
- `tests/test_portal/test_botlens_event_retention.py`
- `tests/test_portal/test_run_research_dataset.py`
- [Durable canonical order lifecycle](../execution-runtime/DURABLE_CANONICAL_ORDER_LIFECYCLE.md)

## References

- [ADR 0041](0041-use-canonical-execution-plan-and-order-fill-semantics.md)
- [ADR 0042](0042-use-runtime-event-ledger-as-lifecycle-truth.md)
- [ADR 0043](0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md)
- [ADR 0049](0049-keep-live-order-submission-closed.md)
- [ADR 0056](0056-pin-venue-neutral-execution-contexts-per-run.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
