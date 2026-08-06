---
component: phase-3a-replay-certified-book-execution
subsystem: execution-runtime
layer: boundary
doc_type: architecture
status: active
tags:
  - execution
  - order-book
  - replay
  - accounting
  - reporting
  - autonomy
code_paths:
  - src/engines/bot_runtime/core/book_execution.py
  - src/engines/bot_runtime/core/execution_context.py
  - src/engines/bot_runtime/core/execution_order.py
  - src/engines/bot_runtime/core/entry_execution.py
  - src/engines/bot_runtime/core/domain/engine.py
  - src/engines/bot_runtime/core/domain/position.py
  - portal/backend/service/market/market_structure_service.py
  - portal/backend/service/bots/config_service.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/reports/run_research_dataset.py
  - tests/integration/runtime/test_book_execution.py
---
# Phase 3A Replay-Certified Book Execution

## Scope and status

Phase 3A is implemented for deterministic backtests. It adds X3 spread-aware
top-of-book execution and X4 aggressive aggregated-L2 execution behind the
Phase 2A `ResolvedExecutionContext` and Phase 2B canonical lifecycle. It does
not open external order submission, paper/live book execution, passive queue
claims, calibrated latency, or autonomous promotion.

The implementation preserves the existing strategy, lifecycle, accounting,
runtime-event, BotLens, report, and comparison owners. It does not create a
second execution ledger. Historical X0-X2 bar models remain readable and retain
their exact version-pinned behavior when no execution-book tape is bound.

## Boundary and data flow

```text
acknowledged raw market archive
  -> provider translation in market-data service
  -> deterministic Level2BookReconstructor
  -> database/checkpoint/feature replay equality
  -> immutable ExecutionBookTape
  -> run-pinned ExecutionBookTapeBundle
  -> ResolvedExecutionContext (X3 or X4 model artifact)
  -> BookExecutionModel at deterministic arrival
  -> exact price-level lifecycle fills
  -> existing position, wallet, fee, PnL, and reconciliation owners
  -> typed runtime events, BotLens, RunResearchDataset, comparison gates
```

Raw provider messages do not enter generic execution code. Raw L2 remains
ineligible for the frozen strategy dataset. The execution tape is a separate,
hash-verified replay artifact projected only after the market-data replay
boundary certifies the source session.

## Immutable execution-book contract

`execution_book_snapshot.v1` pins one normalized post-event state:

- instrument and series identity;
- validity interval and provider-neutral source position;
- product-definition and quantity-unit identity;
- effective and known-at timestamps;
- reconstruction state hash;
- ordered bid and ask levels; and
- a recomputed snapshot hash.

`execution_book_tape.v1` pins the causally ordered snapshots, L1/L2/L3 source
capability, reconstruction version, source replay fingerprint, replay-certified
flag, known validity closures, declared limitations, tape ID, and tape hash.
Known interval closure prevents stale pre-gap state from being selected after a
transport invalidation or clean close. Selection uses only the last state whose
`known_at` is not later than deterministic order arrival.

`execution_book_tape_bundle.v1` binds one unique tape per runtime instrument.
Startup validates every nested hash, rejects missing or extra instrument
bindings, and pins the bundle in the immutable run config snapshot. Phase 3A
admits this artifact only for backtests.

The replay-book API accepts an optional `execution_instrument_id`. When present,
it returns both a tape and a directly consumable single-tape bundle, but only
after raw-object checksum verification, full replay/database reconciliation,
checkpoint-plus-delta equality, and persisted feature equality complete.

## Execution model semantics

The generic `BookExecutionModel` contains no venue-name branch and performs no
provider I/O. The resolved instrument, venue, fee, and model contracts remain
the authority for conformance, liquidity role, fee calculation, capabilities,
and market protections.

### X3 — spread-aware top of book

An L1 tape selects the causal BBO and consumes at most the best opposing level.
It may return a partial fill when the visible top level is smaller than the
request. It never walks deeper levels even if a malformed or richer L1 artifact
contains them. Every result discloses zero modeled latency and the absence of a
passive queue model.

### X4 — aggressive aggregated L2

An L2 or L3 tape may walk eligible opposing levels in price priority. A market
or stop-market request may consume all visible opposing levels. A canonical
`limit_aggressive` request consumes only levels at or better than its limit and
receives price improvement when available. Each consumed level produces a
separate deterministic fill ID, fee calculation, lifecycle event, and evidence
record. Aggregate consumed quantity is checked against eligible visible depth.

`limit_maker` and `limit_resting` do not cross the book in Phase 3A. They remain
open with an explicit `resting_order_execution_not_admitted` limitation. This
prevents a marketable resting order from being incorrectly classified or
charged as maker execution. Resting progress belongs to Phase 3B/X5.

### Time in force and residuals

| Policy | Visible depth behavior | Residual behavior |
| --- | --- | --- |
| FOK | Fill only if all requested quantity is currently eligible | Cancel with no fill when insufficient |
| IOC | Consume eligible depth | Cancel residual |
| GTC aggressive limit | Consume eligible depth | Retain residual as open for later causal evaluation |
| Market or stop-market | Consume eligible depth | Cancel residual; it cannot rest |
| Resting/maker limit | No Phase 3A fill | Remain open until existing expiry/cancel policy acts |

A later GTC evaluation reuses the same immutable request/lifecycle, submits only
the derived residual, selects the new causal arrival snapshot, and may complete
the order. Existing Phase 2B replacement, cancellation, expiry, and deterministic
race rules remain authoritative.

## Per-fill accounting

Every admitted book entry level has an immutable fill ID. The entry engine
applies levels one at a time and immediately:

- settles the existing shared wallet using a fill-scoped correlation identity;
- opens or augments the existing canonical position;
- recomputes entry VWAP, stop, leg quantities and targets;
- applies the exact fee and updates net PnL inputs;
- updates cumulative and remaining request quantity; and
- records fill identity and evidence for idempotent retry.

Duplicate fill IDs are no-ops before wallet mutation. A partially filled entry
may remain active when its residual later fills, rests, cancels, or expires;
the filled exposure is never discarded. Legacy/custom partial-entry adapters
without this per-fill evidence retain the Phase 2B fail-closed guard.

Each level is also an immutable `ORDER_LIFECYCLE_CHANGED` runtime event. Wallet
and position effects remain owned by the existing ledgers. BotLens and reports
are projections of those owners, not alternative accounting authorities.

## Evidence and quality classification

`book_execution_evidence.v1` records, at minimum:

- context/model identity and quality ceiling;
- tape ID/hash, replay fingerprint, source capability, and certification;
- snapshot/state/validity hashes and provider-neutral source reference;
- book effective/known-at and deterministic order-arrival timestamps;
- BBO, spread, requested quantity, reference price, and order policy;
- eligible visible depth and exact consumed level;
- per-level price, quantity, notional, fee, and price improvement;
- aggregate VWAP, slippage, remaining quantity, and residual disposition where
  the batch terminates; and
- limitations, including zero latency and unavailable passive queue precision.

RunResearchDataset derives its maximum class from the weakest pinned model
context, not from configuration prose. X3 requires certified causal arrival and
valid spread evidence for every lifecycle fill. X4 additionally requires L2+
capability, exact per-level evidence, equality between lifecycle and consumed
quantity, and visible-depth bounds. Missing X4-only evidence downgrades to X3;
missing replay/arrival/spread evidence downgrades to X2 when the lower economic
floor remains valid; economic/context failures still downgrade to X0.

The report publishes separate X3 and X4 assessment artifacts, hashes, tape and
snapshot references, limitations, and deterministic blocking reasons.
Comparison endpoints and experiment plans may now require X3 or X4 while
selection/promotion plans retain X2 as their default minimum.

## Compatibility and migration

- Omitting `execution_book_tape_bundle` selects the existing bar artifact and
  preserves X0-X2 behavior.
- Existing assumption manifests remain the economic floor and pin the model
  artifact hash; a book model version is validated against its resolved context
  rather than misreported as the bar model version.
- The additive runtime-event evidence field is backward-readable. Historical
  events without it remain valid at their supported class.
- No database migration is required: tapes are immutable run artifacts and book
  evidence is carried in the existing typed event envelope.
- `FillOrder` remains a compatibility facade. The lifecycle remains the durable
  order authority.

## Rollout and rollback

Rollout is opt-in per backtest by binding a certified tape bundle. Operators can
compare the same strategy under its pinned X2 and X3/X4 models without changing
strategy intent. A selection protocol may require a higher minimum class only
after both candidates have compatible artifacts.

Rollback removes the tape binding and pins the previous immutable bar model for
new runs. Existing tapes, lifecycle events, wallet facts, and reports remain
readable and are never rewritten. Simulated open residuals are drained or
canceled under their original policy; no external venue state exists to clean
up in Phase 3A.

## Agent permission boundary

Agents may observe certified tape evidence, create and execute approved X3/X4
backtest variants, compare compatible results, and automatically reject results
that violate causal arrival, capability, visible-depth, or minimum-class gates.
They may not publish or alter venue profiles, choose unapproved calibrations,
claim X5 queue quality, submit external orders, mutate runtime state directly,
certify themselves, promote, deploy, or change capital limits.

## Explicit non-goals and residual risks

Phase 3A does not implement passive fill probability, resting queue progress,
L2 cancellation-ahead inference, order-level L3 queue position, nonzero or
stochastic latency, shadow execution, paper/live reconciliation, calibration,
hidden liquidity, external submission, derivatives expansion, or promotion
authority. Aggregated L2 proves visible aggressive liquidity only. It does not
prove exact venue fills, matching-engine priority, or capacity beyond the
visible arrival book.

## References

- [Autonomous research and promotion roadmap](../research-orchestration/AUTONOMOUS_RESEARCH_AND_PROMOTION_ROADMAP.md)
- [Phase 1 economic execution contract](PHASE_1_ECONOMIC_EXECUTION_CONTRACT.md)
- [Phase 2A venue-neutral execution context](PHASE_2A_VENUE_NEUTRAL_EXECUTION_CONTEXT.md)
- [Phase 2B durable canonical order lifecycle](PHASE_2B_DURABLE_CANONICAL_ORDER_LIFECYCLE.md)
- [ADR 0058](../decisions/0058-use-replay-certified-execution-book-tapes.md)
- [ADR 0049](../decisions/0049-keep-live-order-submission-closed.md)
