---
component: adr-pin-venue-neutral-execution-contexts-per-run
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - execution
  - venues
  - fees
  - reproducibility
code_paths:
  - src/engines/bot_runtime/core/execution_context.py
  - src/engines/bot_runtime/core/fees.py
  - src/engines/bot_runtime/core/execution_profile.py
  - src/engines/bot_runtime/core/execution_order.py
  - portal/backend/service/bots/config_service.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/reports/run_research_dataset.py
  - tests/integration/runtime/test_execution_contexts.py
---
# ADR 0056: Pin Venue-Neutral Execution Contexts Per Run

## Status

Accepted on 2026-08-05 after roadmap ratification.

## Context

Phase 1 made bar economics explicit and reproducible, but the runtime profile
still combined instrument facts and legacy fee inputs while lacking a separate
venue rule contract. Extending that profile with every venue capability, fee
tier, fill model, and later calibration would create a monolith and invite
venue-name branches in generic execution code.

## Decision

Resolve and pin one immutable `ResolvedExecutionContext` per runtime series. It
binds independently versioned and hashed `InstrumentExecutionContract`,
`VenueExecutionProfile`, `FeeSchedule`, and `ExecutionModelArtifact` manifests.
The complete bundle is validated at startup, stored in the run snapshot, and
revalidated by runtime and reporting.

Venue profiles own order/TIF capability, post-only behavior, liquidity-role
classification, increment rules, protections, lifecycle mappings, and book
capability. Instrument contracts own product and accounting facts. Fee
schedules own rate, currency, basis, tier, and rounding. Model artifacts own
fill-model evidence and the execution-quality ceiling.

`SeriesExecutionProfile` remains a compatibility compiler into the instrument
slice. `FillOrder` remains the Phase 2A pre-fill compatibility adapter. ADR
0057 and Phase 2B now bind each durable request to the resolved context hash for
its full append-only lifecycle. External order submission remains disabled and
governed by ADR 0049.

## Invariants

- Generic execution code must not branch on a venue name.
- A run cannot start if its compiled order policy exceeds the selected venue
  profile's capabilities.
- Every component, resolved context, and bundle is immutable and hash-verified.
- Runtime-recomputed context facts must exactly match the startup snapshot.
- New-context fills must identify the exact context, profile, schedule, and
  model used; reporting mismatches force X0.
- The context does not replace canonical fill, position, wallet, or accounting
  ownership.
- A Phase 2A profile cannot authorize external order submission.

## Consequences

- A second venue can supply different rules and fees without changing the
  generic bar engine.
- Current behavior remains replayable through a versioned compatibility
  profile rather than silent defaults.
- Production venue profiles still require authoritative evidence; a valid
  manifest proves implemented rules, not realistic fills.
- Phase 2B adds lifecycle behind these contracts; Phase 3 can add book mechanics
  without coupling strategy semantics to a venue.

## Rejected alternatives

- Expand `SeriesExecutionProfile` into a venue/instrument/fee/model/calibration
  monolith.
- Put Coinbase, Kraken, or other venue conditionals in the generic engine.
- Store only hashes while discarding the manifests needed for replay.
- Rename current full-fill requests as durable orders before lifecycle state
  exists.
- Treat provider credential or `supportsOrders` metadata as execution authority.

## Enforcing evidence

- `tests/integration/runtime/test_execution_contexts.py`
- `tests/test_portal/test_bot_config_runtime_readiness.py`
- `tests/test_portal/test_bot_startup_orchestrator.py`
- `tests/test_portal/test_run_research_dataset.py`
- [Phase 2A venue-neutral execution context](../execution-runtime/PHASE_2A_VENUE_NEUTRAL_EXECUTION_CONTEXT.md)

## References

- [ADR 0027](0027-use-execution-profiles-as-runtime-instrument-authority.md)
- [ADR 0041](0041-use-canonical-execution-plan-and-order-fill-semantics.md)
- [ADR 0049](0049-keep-live-order-submission-closed.md)
- [ADR 0057](0057-use-append-only-canonical-order-lifecycle.md)
- [Phase 2B durable canonical order lifecycle](../execution-runtime/PHASE_2B_DURABLE_CANONICAL_ORDER_LIFECYCLE.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
