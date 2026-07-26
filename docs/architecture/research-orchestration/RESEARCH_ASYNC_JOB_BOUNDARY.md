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
  - scripts/db/manual_migration_async_job_fencing_v1.sql
  - portal/backend/run_backend.py
  - src/core/settings.py
  - cli/main.py
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
that budget. Partial-progress checkpoints and mid-job cancellation are not
supported; operators must not infer them from retry status.

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
qt research jobs result <job_id> --format table
```

The default dispatch output is human-readable and intentionally short. Use
`qt research jobs status <job_id> --json` or `qt research jobs result <job_id>
--format json` when automation needs the raw contract.
