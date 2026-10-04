---
component: research-async-job-boundary
subsystem: research-orchestration
layer: service
doc_type: architecture
status: active
tags:
  - research
  - orchestration
  - async-jobs
  - cli
  - workers
code_paths:
  - portal/backend/controller/research.py
  - portal/backend/service/research/async_dispatch.py
  - portal/backend/workers/research_worker.py
  - portal/backend/service/async_jobs
  - portal/backend/db/models.py
  - portal/backend/db/execution_control.py
  - src/core/execution_control.py
  - scripts/db/manual_migration_async_job_fencing_v1.sql
  - portal/backend/run_backend.py
  - src/core/settings.py
  - cli/main.py
  - cli/research_operations.py
---
# Research Async Job Boundary

## Purpose

Research async jobs let agents dispatch expensive research checks and sweeps
without holding a CLI HTTP request open. They are an orchestration surface over
the existing research check contracts.

## Contract

Async research jobs may:

- enqueue `research_check_run` and `research_check_sweep` jobs in
  `portal_async_jobs`,
- deduplicate in-flight identical requests by request fingerprint,
- execute jobs in `portal.backend.workers.research_worker`,
- expose compact job status and full completed results through backend routes,
- let `qt research check ... --dispatch` return immediately with a job id.

Async research jobs must not:

- add alternate detector semantics,
- compute indicator evidence outside the canonical runtime graph,
- fetch candles outside the data boundary,
- persist strategy/runtime/execution truth,
- hide worker errors or return synthetic success results.

## Worker Ownership

The backend supervisor starts a dedicated research worker pool configured by
`workers.research`. Research workers claim only research job types, so long
research sweeps do not consume indicator overlay/signal worker capacity.

Workers use the same research evaluation and persistence contracts as
synchronous routes. Job records hold queue status, attempts, timestamps,
errors, completed results, heartbeat time, and a monotonic claim generation.
The raw claim token stays in the worker; only its hash is persisted and neither
form is returned by job status.

[ADR 0047](../decisions/0047-fence-async-job-ownership.md) is enforced. Claims
heartbeat at a bounded interval. Heartbeat, completion, failure, and
job-owned effects compare the current owner/token/generation under a row lock.
Reclaim advances the generation, so a stale worker cannot commit after a newer
claim. Research-check observations, checks, links, and terminal result commit
atomically. Indicator jobs and research sweeps are read-only until their
terminal result. Lease timestamps come from PostgreSQL, not worker clocks.

The request fingerprint has canonical queue ownership rather than a
read-before-write advisory check. A partial unique index allows one in-flight
job per job type, partition, and request fingerprint, so concurrent
dispatchers reuse the same row. Completed and failed rows do not block a later
explicit submission. A bounded retry closes the transition race where the
conflicting row becomes terminal between the atomic insert and reuse lookup.

Retries restart from the immutable request and remain bounded by
`max_attempts`; timeout reclaim terminally fails a claim that has exhausted
that budget. Partial-progress resume is not supported; retries execute the pinned
request again.

Individual cancellation uses the existing row's result field for an explicit
`async_job_cancellation.v1` control receipt while work is in flight. It never
mutates the immutable request. Queued/retry rows become `cancelled` immediately;
running rows remain `running` with their claim and deduplication identity until
the owner has unwound computation and I/O. Public status separates the request
from `execution_stopped`; control receipts are never presented as Check results.
The same row lock orders cancellation against atomic result publication. A
completed result wins if its transaction acquired and committed the lock first.

Research heartbeats poll once per second, check cancellation/shutdown, and interrupt
only the current execution's psycopg2 statement. Engine steps and evaluator loops
check the execution-local stop signal. This is cooperative: a native computation
must return to a checkpoint; one second is the polling interval, not a universal
stop-latency guarantee. SQL interruption is bound to the owning connection until
its statement releases, preventing a cancellation from following pooled reuse.
Worker shutdown unwinds execution and uses the existing bounded retry policy;
an explicit cancellation never retries or publishes partial evidence.

Stale cancellation requests are not automatically reclaimed: an expired heartbeat
does not establish that the old CPU/DB work stopped. Their retained in-flight
identity exposes uncertainty and blocks duplicate resubmission until the owner can
acknowledge. Recovery of a dead owner requires separate proof of stopped execution;
there is no unsafe automatic acknowledgement or lease-based capacity credit.
Deploy the cancellation API and worker together after draining older workers.
No table migration is needed, but rolling mixed-worker cancellation is unsupported.

Existing databases use
`scripts/db/manual_migration_async_job_fencing_v1.sql` while all backend and
worker processes are stopped. The migration refuses concurrent client
sessions, requeues old running claims only on first installation, and is safe
to apply repeatedly.

## CLI Boundary

Synchronous check commands remain useful for small checks. Add `--dispatch` for
work that should be queued:

```bash
qt research check sweep ... --dispatch
qt research jobs status <job_id>
qt research jobs cancel <job_id>
qt research jobs result <job_id> --format table
```

The default dispatch output is human-readable and intentionally short. Use
`qt research jobs status <job_id> --json` or `qt research jobs result <job_id>
--format json` when automation needs the raw contract.


## Explicit single-attempt evidence dispatch

`qt research check run --request-json request.json --dispatch --single-attempt`
uses `POST /api/research/jobs/checks/run-once` and the existing queue's
`max_attempts=1`. A distinct route makes an older server reject the operation
before enqueueing instead of silently ignoring an unknown policy field. The
ordinary route retains two attempts; synchronous runs reject the CLI flag.

Scientific request/result semantics and request fingerprints do not change.
The dispatcher reads back the actual persisted attempt limit and claim count
for its receipt. If an identical in-flight job has a different limit, it fails
with that job's identity: it neither mutates the existing policy nor creates a
parallel scientific duplicate to evade deduplication. Reconcile that job before
another action. A missing dispatch readback is also an uncertain enqueue, not
permission to retry.

One winning transactional Check is not proof of one physical computation.
Claim counts are conservative budget evidence, not CPU-start instrumentation.
One attempt prevents automatic retry/reclaim; it does not implement a compute
window deadline, cancellation, or a lifetime ban on manual redispatch. A new
explicit job after terminal failure consumes a new budgeted attempt. Preserve
actual job IDs, failures and counters; a polling/client timeout never proves
that a worker stopped. Existing fencing, heartbeats and atomic effects remain
unchanged.
