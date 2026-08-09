---
component: adr-fastapi-execution-boundary
subsystem: platform
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - api
  - fastapi
  - asyncio
  - responsiveness
  - threadpool
  - accepted
code_paths:
  - portal/backend/controller
  - portal/backend/service/bots/botlens_bootstrap_service.py
  - portal/backend/service/bots/botlens_symbol_service.py
  - portal/backend/service/bots/botlens_projector_registry.py
  - portal/backend/service/indicators/async_dispatch.py
  - tests/contract/test_fastapi_execution_boundary.py
  - tests/test_portal/test_api_execution_boundary.py
  - docs/contracts/platform/03_engineering_contract.md
---
# ADR 0054: Keep Blocking API Work Off The Event Loop

## Status

Accepted on 2026-08-03 after a full FastAPI controller audit and a live
concurrency proof against a 42-second cold report reconstruction.

## Context

The portal API uses one long-lived ASGI event loop for cooperative HTTP work,
BotLens projection ownership, SSE, WebSockets, health, and navigation. FastAPI
runs a plain `def` route in its worker threadpool, but it invokes an `async def`
route directly on that shared event loop. A synchronous function called from an
`async def` route remains synchronous and blocks the loop until it returns.

The failure was first proven when the research-evidence route invoked a
42-second `RunResearchDataset` reconstruction inline. During that build,
unrelated BotLens, exact-run, and stream responses could not progress. Moving
the reconstruction across a threadpool seam restored concurrent health in
0.003 seconds, exact-run inspection in 0.065 seconds, and BotLens bootstrap in
0.890 seconds while the same cold build continued for 42.292 seconds.

The subsequent controller audit found a systemic declaration problem rather
than one isolated route: 75 HTTP routes were `async def` functions containing
no await. The affected families were strategies (29), research (15), indicators
(14), instruments (9), and providers (8). High-risk examples included strategy
preview/compare, research checks and sweeps, indicator runtime validation,
instrument candle coverage, and provider metadata refresh. The audit also found
mixed async paths where BotLens performed synchronous run lookup or historical
ledger replay before/after a legitimate await, and indicator async-job routes
performed synchronous job-repository operations inline.

## Decision

Quant-Trad uses three explicit API execution shapes:

1. **Synchronous request/response route.** If a route calls synchronous SQL,
   filesystem, Docker, provider-client, subprocess, blocking wait, replay, or
   CPU-bound code, declare the route with `def`. FastAPI owns threadpool
   dispatch for the complete handler.
2. **Cooperative async route or stream.** Use `async def` only when the route
   genuinely awaits asynchronous I/O, queues, locks, timers, async jobs, SSE,
   or WebSockets. Its event-loop work must remain bounded.
3. **Mixed async route.** When a genuinely async path also needs a synchronous
   operation, invoke that operation through `run_in_threadpool` or
   `asyncio.to_thread`. Keep the offload at the synchronous ownership seam so
   its inputs, errors, timing, and cancellation behavior remain explicit.

Threadpool isolation preserves serving-loop responsiveness but does not make
work cheap. Long-running, unbounded, or horizontally scalable computation must
use an owned async-job/worker boundary. A request thread may serve bounded
compatibility work, but the threadpool must not become a second job system.

The remediation converts all 75 zero-await HTTP routes to synchronous FastAPI
handlers without changing their request or response contracts. Report routes
retain their existing explicit threadpool helper. Research evidence retains its
explicit offload. BotLens keeps its async projection APIs but offloads run-row
lookup and historical replay. Indicator overlay/signal routes keep async job
waiting but offload synchronous job repository calls.

## Invariants

- No HTTP route may be declared `async def` without at least one real await.
- Blocking service ownership is determined by implementation behavior, not by
  whether the caller happens to use async syntax.
- Synchronous SQLAlchemy, filesystem, Docker SDK, provider SDK, subprocess,
  replay, and blocking queue calls never execute on the ASGI event-loop thread.
- Async streams never use `time.sleep`, `threading.Condition.wait`, or blocking
  queue reads on the event loop.
- Pure bounded payload transformations may remain inline in async paths; any
  operation whose cost grows materially with persisted history is not bounded.
- One slow request may consume its own worker capacity but cannot prevent the
  serving loop from advancing unrelated requests and streams.
- Moving work across the execution seam cannot change canonical data,
  causality, errors, response contracts, or fingerprints.

## Consequences

UI navigation, health, BotLens, and live streams remain responsive when another
request performs slow synchronous work. Route declarations now communicate
their actual execution model. The worker threadpool can still saturate when too
many slow compatibility requests run concurrently, so expensive operations
must continue migrating to the shared async-job workers rather than increasing
thread counts blindly.

Synchronous routes may no longer be called as coroutines in direct unit tests.
Tests should exercise them through FastAPI or call the synchronous function
directly. Genuine async routes retain coroutine tests.

## Rejected Alternatives

- Declare every route `async def` for stylistic consistency.
- Add `await` around a synchronous call and treat syntax as non-blocking.
- Patch only known slow endpoints without a controller-wide guard.
- Increase Uvicorn workers or thread counts to hide event-loop blockage.
- Move every operation into the threadpool, including WebSocket and queue
  coordination that is already cooperative.
- Use the request threadpool as the long-running research/preview job platform.

## Enforcement And Evidence

- `tests/contract/test_fastapi_execution_boundary.py` rejects zero-await async
  HTTP routes and known direct blocking calls inside mixed async services.
- `tests/test_portal/test_api_execution_boundary.py` runs a deliberately slow
  synchronous service behind FastAPI and proves an unrelated async request
  advances before that service returns.
- Focused controller suites exercise the converted strategy, research,
  indicator, instrument, and provider contracts through FastAPI.
- BotLens service suites verify hot and historical projection behavior after
  run lookup and replay move to explicit thread offloads.
- Live concurrency tests must pair a deliberately slow blocking request with
  health, exact-run, and BotLens probes; unrelated probes must complete before
  the slow request.

## References

- [Engineering Contract](../../contracts/platform/03_engineering_contract.md)
- [Operator Console v2](../frontend/OPERATOR_CONSOLE_V2.md)
- [BotLens Projection Boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Reporting Boundary](../reporting/REPORTING_BOUNDARY.md)
