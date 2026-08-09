---
component: adr-replay-certified-execution-book-tapes
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - execution
  - order-book
  - replay
  - known-at
  - provider-isolation
code_paths:
  - src/engines/bot_runtime/core/book_execution.py
  - portal/backend/service/market/market_structure_service.py
  - portal/backend/service/bots/config_service.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/reports/run_research_dataset.py
---
# ADR 0058: Use Replay-Certified Execution-Book Tapes

## Status

Accepted on 2026-08-05 for Phase 3A deterministic backtests.

## Context

The market-data plane already owns provider translation, source sequencing,
book reconstruction, validity intervals, raw archive retention, checkpointing,
and replay equality. The execution runtime needs arrival-book state for X3/X4
simulation, but raw provider messages are intentionally not strategy-dataset
facts and generic execution code must not acquire provider dependencies.

Reading a live reconstructor directly would make a run depend on mutable service
state. Embedding provider events in the strategy dataset would weaken provider
isolation and confuse signal facts with execution evidence. Reducing the stream
to spread/depth summaries would make exact L2 walking irreproducible. A tape
built before replay validation could preserve deterministic but invalid books.

## Decision

Use a separate immutable `ExecutionBookTape` artifact at the certified replay
boundary.

The market-data service may project provider-neutral `BookStateView` facts into
an execution tape only after raw checksums, database reconciliation,
checkpoint-plus-delta equality, and derived-feature equality pass. The tape
contains normalized states, causal known-at timestamps, reconstruction and
source references, validity closures, source capability, replay fingerprint,
limitations, and recomputed hashes.

Backtest startup binds a hash-verified `ExecutionBookTapeBundle` alongside the
frozen strategy dataset and `ResolvedExecutionContext`. Exactly one tape must
match each participating instrument, and the model input capability may not
exceed either the tape or venue profile. The bundle is copied into the immutable
run config snapshot. Generic execution consumes only this contract and performs
no provider I/O or venue-name branching.

Raw L2 remains outside the strategy dataset. The dataset continues to carry
causal strategy facts; the tape is a separate execution artifact. A known
validity closure makes the affected interval unavailable at and after its
known-at boundary so future arrivals cannot reuse stale pre-gap state.

## Consequences

Positive consequences:

- persisted and offline book replay use the same reconstruction authority;
- strategy evaluation remains provider-free and frozen;
- execution can select only state known at deterministic arrival;
- tapes and fills carry exact provenance without provider-specific mechanics;
- X3/X4 results can be reproduced and compared from pinned artifacts; and
- a future venue adds translation/profile/calibration implementations rather
  than a separate execution engine.

Costs and limitations:

- L2 tapes may be large and require explicit run binding and retention policy;
- aggregated L2 cannot prove passive queue position or hidden liquidity;
- tape certification proves source reconstruction, not venue matching fidelity;
- Phase 3A uses deterministic zero latency; and
- a tape is not authorization to submit an order.

## Rejected alternatives

### Read the live market-data service from the execution loop

Rejected because it breaks frozen replay, introduces mutable timing/service
dependencies, and cannot reproduce historical order arrival exactly.

### Put raw provider L2 messages in the strategy dataset

Rejected because provider payloads would cross the normalization boundary and
mix execution evidence with strategy facts.

### Persist only BBO and depth summaries

Rejected for X4 because summaries cannot reproduce exact opposing-side level
walking or per-level fills.

### Allow execution to ignore known validity closures

Rejected because deterministic selection of a stale pre-gap book would be a
reproducible false claim.

## Follow-up

Phase 3B may add resting progress, bounded latency, and capability-aware queue
models behind the same tape/context/lifecycle boundary. Shadow and live
calibration remain later phases and require their own authorization decisions.

## References

- [Phase 3A replay-certified book execution](../execution-runtime/PHASE_3A_REPLAY_CERTIFIED_BOOK_EXECUTION.md)
- [ADR 0053](0053-use-tiered-market-structure-archive-and-replay-boundary.md)
- [ADR 0056](0056-pin-venue-neutral-execution-contexts-per-run.md)
- [ADR 0057](0057-use-append-only-canonical-order-lifecycle.md)
- [ADR 0049](0049-keep-live-order-submission-closed.md)
