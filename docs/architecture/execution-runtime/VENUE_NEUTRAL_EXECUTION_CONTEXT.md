---
component: venue-neutral-execution-context
subsystem: execution-runtime
layer: boundary
doc_type: architecture
status: active
tags:
  - execution
  - venues
  - fees
  - orders
  - reproducibility
  - autonomy
code_paths:
  - src/engines/bot_runtime/core/execution_context.py
  - src/engines/bot_runtime/core/fees.py
  - src/engines/bot_runtime/core/execution_profile.py
  - src/engines/bot_runtime/core/execution_order.py
  - src/engines/bot_runtime/core/execution.py
  - src/engines/bot_runtime/core/execution_runtime.py
  - src/engines/bot_runtime/core/entry_execution.py
  - src/engines/bot_runtime/core/domain/engine.py
  - src/engines/bot_runtime/core/domain/position.py
  - src/engines/bot_runtime/strategy/series_builder_parts/series_construction.py
  - portal/backend/service/bots/config_service.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/reports/run_research_dataset.py
  - tests/integration/runtime/test_execution_contexts.py
---
# Venue-Neutral Execution Context

## Scope and status

The venue-neutral execution context is implemented for deterministic X0-X2 bar
execution. It separates instrument facts, venue rules, fee facts, and
fill-model evidence into immutable versioned contracts, resolves them before
run creation, and pins the complete bundle in the immutable run snapshot.
Generic execution code consumes the resolved context without branching on
venue names.

This is an architectural credibility boundary, not a claim of book or venue
fill realism. The durable canonical order lifecycle preserves this context
authority. Replay-certified book execution and passive queue bounds add their
own higher-class evidence without changing that authority. Shadow execution,
external order submission, empirical calibration, and expanded derivative
economics remain outside this capability.

The composition is illustrated in
[resolved-execution-context.mmd](diagrams/resolved-execution-context.mmd).

## Runtime authority

For each linked instrument, startup resolves:

```text
ResolvedExecutionContext
  = InstrumentExecutionContract
  + VenueExecutionProfile
  + FeeSchedule
  + ExecutionModelArtifact
```

Multiple contexts form one `ResolvedExecutionContextBundle`. Every component,
context, and bundle has a stable SHA-256 hash over normalized material fields.
Deserialization recomputes and verifies those hashes. The complete manifests,
not only their hashes, are retained in the run snapshot.

| Contract | Owns | Does not own |
| --- | --- | --- |
| `InstrumentExecutionContract` | Product identity, source/execution semantics, currencies, tick/contract values, quantity and notional constraints, product capabilities, accounting and margin facts. | Venue order rules, fee-tier policy, fill behavior, provider transport. |
| `VenueExecutionProfile` | Supported order types and TIF, post-only behavior, maker/taker classification, increment policies, market protections, lifecycle mappings, book-data capability, and the closed external-order flag. | Instrument economics, fill generation, accounting, credentials. |
| `FeeSchedule` | Maker/taker rates, schedule/profile identity, version, source, currency, basis, deterministic rounding, precision, tier, configuration status, and hash. | Liquidity classification or fill generation. |
| `ExecutionModelArtifact` | Bar-execution assumption-manifest reference, input capability, execution-quality ceiling, supported mechanics, calibration reference, version, and hash. | Venue rules or mutable calibration fitting. |
| `ResolvedExecutionContext` | Exact references binding the four contracts for one runtime series. | Strategy meaning, accounting ownership, or authorization. |

`SeriesExecutionProfile` remains the compatibility compiler for current
instrument, risk, margin, and legacy fee inputs. It is bound to the resolved
context before execution, but it is no longer the complete run-scoped execution
authority. New venue, fee, or model concerns must not be added to it as a
monolith.

## Resolution and pinning flow

1. Backend startup loads the typed strategy and linked canonical instruments.
2. It resolves the immutable bar-execution assumption manifest.
3. It compiles the existing `SeriesExecutionProfile` into the instrument slice.
4. It resolves an explicit venue profile and fee schedule from instrument
   metadata, or the versioned bar-simulation compatibility profile.
5. It compiles the normalized ATM policy and proves all potentially emitted
   order types, GTC behavior, and post-only requirements are supported.
6. It constructs and validates the context bundle before persisting the run.
7. The full bundle is included in readiness evidence, lifecycle dependency
   evidence, the immutable run config snapshot, and the container launch payload.
8. Series construction reloads the bundle, selects exactly one context by
   instrument identity and execution semantics, and recomputes it from the
   pinned runtime inputs. A mismatch fails before execution.

Newly resolved runs therefore cannot change profile, fee, or model versions in
place. Historical snapshots without this bundle remain readable as
`legacy_unavailable`; they are not retroactively assigned venue-neutral context
evidence.

## Order conformance and current lifecycle boundary

Run preparation validates compiled strategy capabilities before a run is
created. Per-order validation is repeated before the adapter can fill. It
enforces:

- supported order type and time in force;
- valid and required post-only flags;
- venue-owned maker/taker classification;
- instrument quantity step, minimum/maximum quantity, and minimum notional;
- venue-owned price and quantity increment policy; and
- configured market-order notional protection.

`FillOrder` carries TIF, post-only, fee-schedule identity, and the resolved
context as a compatibility request adapter. Current bar execution remains an
immediate deterministic full-fill path. The canonical order lifecycle wraps
that seam with durable requested, validated, accepted, open, partial, fill,
cancel, replace, reject, and expiry semantics. `FillOrder` is not a durable
order and must not accumulate lifecycle, venue, or calibration ownership.

## Execution and evidence wiring

The resolved context is supplied to bar entry, spot and derivative exit models,
backtest and paper adapters, risk/position execution, runtime events, BotLens
canonical fill facts, and reports. Every new-context fill records the context,
instrument, venue-profile, fee-schedule, and execution-model hashes plus the
applicable profile and fee attributes.

`RunResearchDataset` validates the snapshot bundle and checks each fill against
it. Missing, invalid, or contradictory evidence forces X0 for new-context runs.
The bar-execution economic contract still determines the X0-X2 ceiling and
attained class; the venue-neutral context does not grant X3 because it has no
spread observation or model.

`FeeSchedule` can describe quote-notional or base-quantity calculation,
arbitrary fee currency, and positive or negative rates. The resolved context
deliberately admits only non-negative, quote-notional fees denominated in the
instrument quote currency. Canonical wallet settlement currently debits that
quote balance and the canonical event contract rejects negative
`fee_paid`; admitting another currency, basis, or a rebate would therefore
overstate accounting support. Those schedules fail at context resolution until
a new canonical accounting/event contract can settle them. Admitted schedules
support deterministic `unrounded`, `half_even`, `half_up`, `down`, and `up`
rounding policies.

## Extensibility proof

Conformance tests resolve two deliberately different synthetic profiles
through the same implementation. They vary order/TIF support, post-only
capability, maker/taker classification, increment policy, market protection,
book capability, lifecycle mapping, and fee rounding/tier. Tests also verify
that unsupported fee currency, basis, and rebates fail closed, and they
scan generic execution modules to prohibit Coinbase or Kraken conditionals.

These are conformance fixtures, not verified production venue profiles. Adding
a real venue profile still requires authoritative rule and fee evidence and a
separately reviewed adapter when external interaction is eventually allowed.

## Migration, rollout, and rollback

- No database migration is required; the bundle lives in existing immutable
  JSON run snapshots and event/report evidence.
- The default `canonical_bar_simulation.v1` profile preserves existing X0-X2
  bar behavior while making its rules explicit and hashed.
- Existing `SeriesExecutionProfile`, `RuntimeExecutionPlan`, and `FillOrder`
  callers remain supported through adapters. New code should depend on
  `ResolvedExecutionContext` for venue, fee, and model facts.
- Explicit profile payloads fail closed when identity, version, capability,
  classification, source, or hash evidence is malformed.
- Rollback selects a previously pinned immutable bundle and may reduce the X
  class. A profile, schedule, or model is never edited in place, and rollback
  cannot preserve a class unsupported by its bundle.
- External order submission remains hard-false in every venue profile admitted
  by this context and remains prohibited by ADR 0049.

## Agent permission boundary

Under this context, agents may observe resolved context evidence, execute
approved simulations with allow-listed immutable profiles and models, compare
compatible execution classes, reject conformance failures, and propose a new
profile or model version for review. They may not publish or approve those
versions, change a pinned run, submit venue orders, mutate runtime/live state,
certify their own evidence, promote a strategy, or deploy capital.

## Residual risks and follow-on capabilities

The venue-neutral context proves rule conformance and reproducibility, not
empirical fill truth. It also does not settle non-quote fees or rebates; those
require an explicit canonical accounting and event-contract version rather
than a profile-only change. The canonical lifecycle retains the same context
and accounting owners. Replay-certified book execution adds aggressive book
walking and atomic per-fill entry settlement; passive queue bounds and latency
add bounded X5 research. Remaining follow-on capabilities include empirical
fill calibration, shadow and paper-book reconciliation, and any externally
authorized venue execution.

## References

- [Bar-execution economic contract](ECONOMIC_EXECUTION_CONTRACT.md)
- [Execution runtime boundary](EXECUTION_RUNTIME_BOUNDARY.md)
- [ADR 0056](../decisions/0056-pin-venue-neutral-execution-contexts-per-run.md)
- [Durable canonical order lifecycle](DURABLE_CANONICAL_ORDER_LIFECYCLE.md)
- [ADR 0057](../decisions/0057-use-append-only-canonical-order-lifecycle.md)
- [ADR 0049](../decisions/0049-keep-live-order-submission-closed.md)
