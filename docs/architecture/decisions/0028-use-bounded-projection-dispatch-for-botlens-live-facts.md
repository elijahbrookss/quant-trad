---
component: adr-bounded-botlens-projection-dispatch
subsystem: botlens-projections
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - botlens
  - projections
  - runtime
  - performance
code_paths:
  - src/engines/bot_runtime/runtime/components/canonical_facts.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - src/engines/bot_runtime/runtime/mixins/setup_prepare.py
  - src/engines/bot_runtime/runtime/mixins/runtime_persistence.py
  - docs/architecture/botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md
  - docs/architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md
  - docs/architecture/persistence/PERSISTENCE_BOUNDARY.md
---
# ADR 0028: Use Bounded Projection Dispatch For BotLens Live Facts

## Status

Accepted on 2026-05-31.

Amended by [ADR 0029](0029-batch-and-degrade-botlens-projection-drain.md):
producer-side BotLens projection pressure degrades projection instead of failing
the run. Canonical persistence remains fail-loud.

## Context

Runtime bar steps produce compact BotLens fact batches from the same
walk-forward state timeline that drives indicators, decisions, wallet effects,
and trades. Before this decision, the bar step also paid the immediate
websocket/subscriber fanout cost for those live facts after assigning the
producer sequence. That kept projection work close to truth production, but it
also meant slow live transport could appear as execution-loop time.

BotLens is a debugger and projection surface. It should consume committed
runtime facts, not participate in execution timing. At the same time, moving
projection work must not introduce a second reconstruction path, a best-effort
drop path, or a hidden synchronous fallback.

## Decision

Keep fact construction and producer sequence assignment inside the runtime
timeline. After the runtime appends or enqueues the canonical batch, hand the
already stamped live payload to a bounded projection dispatcher.

The projection dispatcher owns live BotLens fanout to subscribers/projectors.
It runs outside the bar step, exposes queue/lag/error metrics, and drains during
runtime terminal flush. Queue overflow and terminal drain timeout mark BotLens
projection degraded and may drop stale projection handoffs; they do not fail the
execution run. The dispatcher does not fall back to synchronous fanout and does
not reconstruct from mutable runtime internals.

Durable canonical persistence remains a separate bounded buffer. Projection
dispatch consumes the committed batch; it does not assign durable `run_seq`, it
does not rewrite event identity, and it is not a second storage authority.

## Consequences

- Execution timing is less sensitive to websocket/subscriber fanout pressure.
- BotLens live projection remains downstream of runtime truth and cannot change
  strategy, wallet, order, trade, or report semantics.
- Projection pressure is visible through queue depth, lag, overflow, drop, and
  degraded metrics instead of being hidden inside bar-step elapsed time.
- Terminal run finalization must drain canonical persistence and attempt a
  bounded projection dispatch drain without treating BotLens projection as
  canonical truth.
- Rich projection payload construction still belongs on the runtime snapshot
  boundary until a separate immutable projection-input contract exists.

## References

- [Runtime Contract](../../contracts/platform/01_runtime_contract.md)
- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [BotLens Projection Boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Persistence Boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [ADR 0029](0029-batch-and-degrade-botlens-projection-drain.md)
