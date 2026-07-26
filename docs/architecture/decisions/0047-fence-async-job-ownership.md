---
component: adr-async-job-ownership-fencing
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - async-jobs
  - fencing
  - ownership
  - retries
code_paths:
  - portal/backend/service/async_jobs/repository.py
  - portal/backend/db/models.py
  - portal/backend/workers/indicator_worker.py
  - portal/backend/workers/research_worker.py
  - portal/backend/service/research/service.py
  - portal/backend/service/research/repository.py
  - scripts/db/manual_migration_async_job_fencing_v1.sql
  - docs/architecture/research-orchestration/RESEARCH_ASYNC_JOB_BOUNDARY.md
---
# ADR 0047: Fence Async Job Ownership

## Status

Accepted on 2026-07-25.

## Context

The shared async queue previously used row locking to claim work and
timeout-based reclaim for abandoned jobs. Completion and failure accepted only
a `job_id`, so a slow worker could finish after reclaim and overwrite a newer
owner. Long handlers also had no heartbeat, and a research-check job persisted
research items before its unfenced queue completion.

## Decision

Each claim returns an opaque token and monotonic generation. Only the token
hash is persisted. Heartbeat, completion, failure, and job-owned side-effect
commits lock the job row and compare job id, status, owner, token hash, and
generation. Lease timestamps use the PostgreSQL clock. Reclaim is based on
`heartbeat_at`, clears ownership, and advances the generation before a later
claim advances it again. A stale claim that has consumed `max_attempts` becomes
terminally failed instead of being requeued.

Dispatch idempotency is an atomic queue property. In-flight jobs carry a
validated request fingerprint, and a partial unique index admits only one
queued, running, or retry row for each
`(job_type, partition_key, request_fingerprint)` identity. Concurrent
dispatchers either create that row or reuse it. If the conflicting job becomes
terminal between the insert and reuse lookup, dispatch retries the insert a
bounded number of times and may create the next in-flight row.

Both worker pools renew claims at a bounded interval while synchronous
calculation is running. A research-check run evaluates outside the terminal
transaction, then verifies ownership, persists its observation/check/links,
and marks the job succeeded in one transaction. Indicator calculations and
research sweeps have no durable partial effects; retries restart from the
immutable request and commit only one terminal result.

Current job types do not expose partial-progress checkpoints or mid-job
cancellation. Those capabilities remain unsupported and require a new explicit
contract before partitioned or externally side-effecting handlers are admitted.

## Invariants

- At most one current claim generation may commit job-owned effects.
- A stale or unknown owner fails loudly without changing terminal state.
- Heartbeat expiry is reclaim evidence; wall-clock duration alone is not proof
  that a current owner may still commit.
- Retries are idempotent or resume from an explicit validated checkpoint.
- Completion/failure and every current job-owned durable effect are
  transactional.
- Concurrency and retry budgets remain bounded and observable.
- Concurrent identical dispatches create at most one in-flight job identity.
- Claim tokens and token hashes are not exposed by status APIs.

## Consequences

The queue schema and worker API carry generation, heartbeat, and token-hash
state. The explicit migration requires exclusive database access with backend
and worker processes stopped. On first installation only, it requeues
pre-fencing running rows because no old worker can possess a valid new claim
token; repeated applications preserve already fenced claims. A temporary
database or service failure causes the worker to abandon its terminal commit;
the row remains reclaimable instead of reporting synthetic success.
Startup validates the fencing-critical constraint and index definitions, not
only their names. Under its exclusive table lock, the migration deterministically
recreates those objects so a same-named malformed definition cannot survive.

## Rejected Alternatives

- Increase the running timeout and assume jobs finish in time.
- Trust `worker_id` without a unique claim generation.
- Let the latest result win.
- Add per-worker in-memory locks.
- Create a second queue only for research jobs.

## Enforcing Tests Or Evidence

- `tests/test_portal/test_async_jobs_partition_hash.py` protects stable
  partitioning, strict owner/partition validation, reclaim throttling, bounded
  heartbeats, and cross-thread ownership-loss propagation.
- `tests/test_portal/test_async_jobs_db.py` proves fresh and migrated PostgreSQL
  queues reject stale heartbeat/completion/failure, preserve a live heartbeat,
  advance generations on retry and reclaim, stop reclaim at `max_attempts`,
  suppress concurrent duplicate dispatch, recover when a conflicting job turns
  terminal between insert and lookup, roll back real research artifacts with
  failed owned completion, and commit a successful or duplicate effect at most
  once.
- `tests/test_portal/test_research_checks.py` proves queued research-check
  persistence receives the currently owned transaction.
- `tests/test_portal/test_database_bootstrap.py` protects the required queue
  objects on clean bootstrap and rejects missing or same-named malformed
  fencing constraints and indexes.
- `scripts/db/manual_migration_async_job_fencing_v1.sql` was applied twice to a
  disposable legacy-shaped queue; the running row was requeued once, generation
  advanced once, and constraints/indexes remained idempotent. A concurrent
  client probe was rejected before the schema lock or migration changes.

## References

- [ADR 0039: Shared Async Jobs For Research Dispatch](0039-use-shared-async-jobs-for-research-dispatch.md)
- [Research Async Job Boundary](../research-orchestration/RESEARCH_ASYNC_JOB_BOUNDARY.md)
