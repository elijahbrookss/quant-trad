---
component: phase-2b-durable-canonical-order-lifecycle
subsystem: execution-runtime
layer: boundary
doc_type: architecture
status: active
tags:
  - execution
  - orders
  - lifecycle
  - replay
  - accounting
  - audit
  - autonomy
code_paths:
  - src/engines/bot_runtime/core/order_lifecycle.py
  - src/engines/bot_runtime/core/execution_order.py
  - src/engines/bot_runtime/core/entry_execution.py
  - src/engines/bot_runtime/core/domain/engine.py
  - src/engines/bot_runtime/core/domain/position.py
  - src/engines/bot_runtime/core/runtime_events.py
  - src/engines/bot_runtime/runtime/mixins/runtime_events.py
  - portal/backend/service/bots/botlens_domain_events.py
  - portal/backend/service/bots/botlens_event_retention.py
  - portal/backend/service/reports/run_research_dataset.py
  - tests/integration/runtime/test_canonical_order_lifecycle.py
  - tests/integration/runtime/test_persisted_runtime_correctness.py
---
# Phase 2B Durable Canonical Order Lifecycle

## Scope and status

Phase 2B is implemented for the current deterministic X0-X2 runtime. It replaces
the immediate-fill request as the authoritative long-term order abstraction with
an immutable request, immutable attempts, and an append-only lifecycle trace.
Entry and exit paths now produce this lifecycle through the existing execution
seam, while fills continue into the existing position, wallet, fee, PnL,
runtime-event, reconciliation, BotLens, and reporting owners.

This is an order-state and auditability boundary. It does not add spread or book
observation, L2 walking, queue estimates, latency, external submission, or a
higher execution-quality class. Current production bar models remain full-fill.
Phase 3 is the first campaign allowed to admit book-driven partial entry fills
and resting residual behavior after adding per-fill incremental entry settlement.

The composition is illustrated in
[canonical-order-lifecycle.mmd](diagrams/canonical-order-lifecycle.mmd).

## Authority and ownership

```text
causal strategy decision
  -> CanonicalOrderRequest
  -> CanonicalOrderAttempt
  -> CanonicalOrderLifecycle transition authority
  -> FillOrder compatibility adapter / pinned execution model
  -> canonical fill
  -> existing position + wallet + fee + PnL owners
  -> runtime event ledger
  -> BotLens and RunResearchDataset projections
```

The lifecycle owns order state, residual quantity, replacement lineage,
idempotency, and replay evidence. It does not own fill generation, cash,
positions, fees, realized PnL, or certification. Recording a lifecycle fill is
not settlement; the corresponding fill must still pass through the existing
canonical accounting path exactly once. No second execution ledger or
"realistic" position ledger is introduced.

The `ResolvedExecutionContext` from Phase 2A is pinned by hash on the request and
cannot change during the order lifetime. A separately hashed execution policy
binds order type, time in force, post-only intent, liquidity role, and price
source. Venue profiles may translate a canonical state into venue terminology,
but generic transition code contains no venue-name branches.

## Canonical identities and manifests

`canonical_order_lifecycle.v1` defines:

| Artifact | Identity and immutable binding |
| --- | --- |
| `CanonicalOrderRequest` | Request ID; run, bot, strategy, instrument, symbol, signal, decision, and trade identity; side and requested quantity/price; order policy; known-at time; context and policy hashes; manifest hash. |
| `CanonicalOrderAttempt` | Attempt ID and number; request ID; exact residual quantity admitted to that attempt; policy hash; predecessor attempt and replacement reason; known-at time; manifest hash. |
| `CanonicalOrderEvent` | Stable event ID; contiguous order sequence; attempt and request IDs; prior/next state; cumulative and residual quantities; optional fill/reason/replacement/venue evidence. |
| `CanonicalOrderSnapshot` | Replayed current state, active attempt, cumulative quantity, residual quantity, terminal status, and full replay hash. |

Runtime-generated valid entry request, attempt, trade, event, and fill identities
are deterministic functions of pinned causal material. The compatibility facade
uses explicit `compatibility:unbound_*` bindings only for direct legacy callers
that do not provide runtime identity; it never disguises those calls as fully
bound production evidence.

## State machine

The generic transition authority permits only the following progression:

| Current state | Allowed next states |
| --- | --- |
| none | `requested` |
| `requested` | `validated`, `rejected` |
| `validated` | `accepted`, `rejected` |
| `accepted` | `open`, `partially_filled`, `filled`, `rejected`, `canceled`, `expired`, `replaced` |
| `open` | `partially_filled`, `filled`, `rejected`, `canceled`, `expired`, `replaced` |
| `partially_filled` | `partially_filled`, `filled`, `rejected`, `canceled`, `expired`, `replaced` |
| terminal | none |

`filled`, `rejected`, `expired`, `canceled`, and `replaced` are terminal for an
attempt. Replacement atomically terminates the predecessor, registers a new
attempt for exactly the predecessor residual, and records both directions of
lineage. Invalid replacement quantity, predecessor, or policy input leaves no
partial mutation.

Illegal transitions, non-contiguous sequence numbers, context or policy
changes, overfills, negative residuals, divergent duplicate IDs, and malformed
replay input fail closed.

## Determinism, idempotency, and races

- Request and attempt manifests are stable hashes of normalized immutable
  fields. Deserialization recomputes and validates them.
- Event and fill IDs are deterministic. Reusing an ID with identical material
  is a no-op; reusing it with different material is an invariant failure.
- Every event carries a deterministic replay-prefix hash. The complete trace has
  a full replay hash independent of runtime batching or projection timing.
- Replay requires contiguous order-local sequence numbers and reproduces the
  same attempts, snapshot, cumulative quantity, residual, and hashes.
- Fill has priority over cancel or replacement at the same deterministic
  ordering boundary. A competing fill is applied first; cancellation terminates
  the remaining quantity and replacement must use the post-fill residual.
- Cumulative fill quantity may only increase and may never exceed the immutable
  request quantity. Attempt quantity equals the residual admitted to that
  attempt; residual is derived, never patched by callers.

## Production wiring

The lifecycle is authoritative in these current paths:

- immediate market and conservative bar entries;
- accepted maker entries, later-bar evaluation, expiry, and deterministic
  convert-to-market replacement;
- take-profit, stop, fixed-horizon, terminal-close, and intrabar exit paths;
- partial exit fills, where each fill reduces the canonical leg quantity and
  settles once while the order retains its residual;
- runtime `ORDER_LIFECYCLE_CHANGED` events before their corresponding
  `ENTRY_FILLED` or `EXIT_FILLED` event;
- immutable run artifacts through `order_lifecycle_traces`;
- Tier 1 retained BotLens lifecycle events; and
- `RunResearchDataset.execution.order_lifecycle`, including state distribution,
  open/residual orders, latest snapshots, all lifecycle events, and fill-parent
  links.

The runtime event ledger remains canonical. BotLens and reporting are typed
projections. Fill events use the exact filled lifecycle event as their parent,
while the original signal remains the causal root.

## Partial-entry safety boundary

Current production X0-X2 bar execution models emit either a full fill or no
fill. The lifecycle can represent partial quantities now so Phase 3 does not
need a new order contract, and canonical exits already settle partial fills per
fill.

Incremental entry settlement while an order still has residual quantity is not
enabled in Phase 2B. A custom or future adapter may retain a partial entry and
complete its residual, including through the explicit convert-to-market
replacement. It may not reject, cancel, or expire that residual: runtime fails
closed before such a disposition so the already-filled quantity cannot
disappear. Phase 3 must add an atomic lifecycle-to-position/wallet transaction
for each entry fill before admitting book-driven partial entries or residual
resting/cancel behavior.

## Compatibility, migration, and deprecation

- `FillOrder` and `execute_order(FillOrder)` remain a versioned compatibility
  facade for immediate adapters. `execute_fill_order_with_lifecycle` is the
  migration seam and returns both the existing fill/rejection result and the
  canonical lifecycle.
- Existing callers keep their X0-X2 bar economics. Phase 2B does not change
  economic claim intent, fee/slippage assumptions, or execution-quality class.
- Historical run snapshots and reports without lifecycle evidence remain
  readable. They are not retroactively assigned Phase 2B evidence.
- BotLens uses the existing typed event envelope, so no database schema
  migration is required. Consumers must tolerate the additive
  `ORDER_LIFECYCLE_CHANGED` event and additive report/artifact fields.
- Random UUID generation for valid runtime entry order and trade identities is
  deprecated; causal stable identities are required for replay equality.
- Treating `FillOrder` as a durable order, adding venue conditionals to generic
  lifecycle code, or creating an alternate fill/accounting ledger is prohibited.

## Rollout and rollback

The additive lifecycle is emitted beside the compatibility result, then used as
the parent and audit authority for production-wired entry and exit fills. New
order types are admitted only after transition, accounting, replay, and report
evidence passes under their pinned context.

Rollback may pin the prior immutable adapter/model and stop admitting new
durable simulated orders. Existing lifecycle events and artifacts remain
readable and must not be deleted. In-flight simulated attempts are drained,
canceled, or failed under their original pinned contract; rollback never
disables audit evidence or silently rewrites state.

## Operator behavior and evidence

Operators can inspect request and attempt manifests, ordered state history,
venue mapping, cumulative and residual quantity, replacement ancestry, exact
fill linkage, per-event replay-prefix hashes, and the full trace replay hash.
An order shown as open or partial in a report is not inferred from a trade row;
it is projected from the canonical lifecycle.

Required acceptance evidence includes transition-table and illegal-transition
tests, duplicate suppression, fill/cancel and fill/replace races, replacement
atomicity, residual accounting, context immutability, restart/replay equality,
compatibility facade parity, runtime serialization, persisted-run equality,
BotLens retention/round-trip coverage, report evidence, and reference scenario
regressions.

## Agent permission boundary

Phase 2B grants no external or capital authority. Agents may observe lifecycle
evidence, propose bounded lifecycle trials, create simulated orders only inside
approved protocols, compare compatible simulations, and automatically reject
invalid transitions. Agents may not mutate authoritative order state directly,
change pinned contexts or policies, publish profiles, certify their own work,
promote, deploy, transmit venue orders, or alter capital limits.

## Residual risks and next campaign

Phase 2B proves lifecycle determinism and quantity custody, not executable
liquidity. It does not prove spread, book depth, maker queue position, latency,
venue acknowledgements, or capacity. Phase 3 is the next coherent campaign:
consume replay-certified L2 facts behind this boundary, add spread-aware and
aggressive book walking, settle each admitted partial entry fill atomically,
make TIF/residual behavior explicit, and then add resting/queue/latency models
without raising claims beyond their supported X class.

## References

- [Autonomous research and promotion roadmap](../research-orchestration/AUTONOMOUS_RESEARCH_AND_PROMOTION_ROADMAP.md)
- [Phase 1 economic execution contract](PHASE_1_ECONOMIC_EXECUTION_CONTRACT.md)
- [Phase 2A venue-neutral execution context](PHASE_2A_VENUE_NEUTRAL_EXECUTION_CONTEXT.md)
- [Execution runtime boundary](EXECUTION_RUNTIME_BOUNDARY.md)
- [ADR 0057](../decisions/0057-use-append-only-canonical-order-lifecycle.md)
- [ADR 0043](../decisions/0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md)
- [ADR 0049](../decisions/0049-keep-live-order-submission-closed.md)
