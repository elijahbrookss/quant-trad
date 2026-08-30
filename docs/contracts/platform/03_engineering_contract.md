# Engineering Contract

## Failure Semantics

- Fail loud with actionable context.
- Include IDs, symbol/timeframe, and phase.
- Do not hide invalid state transitions.

## Boundary Design

Use interfaces at real boundaries:
- providers
- storage
- execution adapters

Keep leaf logic explicit and simple.

Framework-style surfaces must be contract-driven, not domain-shaped. A generic
surface may render, rank, compare, route, or validate facts only through fields
declared by the producing contract. It must not import indicator, strategy, bot,
or report-family knowledge to make a generic workflow look smarter.

If a reusable surface needs ordering or interpretation, that intent must be
explicit in the request or emitted contract. Missing rank keys, metric
directions, grouping fields, or required semantics are contract errors, not
places to guess.

Hidden fallbacks are not allowed. A second path is valid only when it is an
explicit contract branch with clear inputs, outputs, and operator-visible
context.

## Schema Semantics

- No runtime migrations/backfills in app paths.
- Missing table: provision once with operator-visible warning.
- Missing columns: fail loud with actionable error.

Relational-schema authority is a reviewed stack rather than a choice between
one ORM file and one migrations folder:

1. This engineering contract owns platform schema behavior.
2. ORM metadata and code-owned schema registries define the clean current
   application model.
3. `Database._bootstrap_schema_contract` is the startup enforcement boundary
   that checks the deployed model and provisions only the explicitly permitted
   clean-install surface.
4. Manual SQL owns explicit historical cutovers and operator-run schema
   transitions. It is retained as historical/operational evidence and is not a
   second runtime migration path.
5. Generated seed SQL is derivative output and cannot override its generator
   or the current model.
6. Docker database bootstrap owns extensions and environment initialization;
   it does not redefine application tables.

When these layers disagree, startup or validation must fail with the exact
layer and object in conflict. No layer may silently patch another or claim to
be the entire schema authority by itself.

## Observability Contract

Lifecycle boundaries must be observable through structured logs with relevant
identities and timing context.

Backend, worker, and bot-runtime application logs write to their normal
stdout/stderr process streams. Each supported topology has exactly one normal
out-of-process shipper from those streams into Loki:

- the local-development composition uses Promtail;
- the native-Linux server composition uses Grafana Alloy.

Promtail remains a supported local-development component. Alloy is the supported
native-server shipper. A topology must not run both against the same application
container stream or otherwise create duplicate normal ingress.

Application processes must not synchronously post ordinary logs to Loki or
enable an in-process Loki handler on the runtime hot path. Configuration,
transport failure, or shipper absence must not silently activate a second
ingress path.

Loki and Grafana are observability projections, not runtime, execution, or
persistence truth. Missing ingestion remains explicit operational
unavailability; it must not be replaced with invented log evidence or inferred
healthy state.

## API Execution Contract

- A FastAPI HTTP handler that calls synchronous SQL, filesystem, Docker,
  provider-client, subprocess, or CPU-bound services must be declared with
  `def` so FastAPI executes it in the worker threadpool.
- `async def` HTTP handlers are reserved for genuinely cooperative work that
  awaits asynchronous I/O, queues, locks, timers, streams, or WebSockets.
- A genuinely async handler or service that also needs a synchronous operation
  must cross an explicit `run_in_threadpool` or `asyncio.to_thread` seam before
  invoking it. Calling a synchronous function from an async function does not
  make the operation asynchronous.
- Long-running or unbounded CPU work belongs in an owned background worker/job.
  The request threadpool is an isolation boundary, not a compute scheduler.
- Event-loop ownership is a responsiveness contract: one slow request must not
  delay health, navigation, unrelated BotLens projections, SSE, or WebSockets.

## Optimization Rule

Preserve correctness and determinism first.
Performance work is valid when semantics remain unchanged.
