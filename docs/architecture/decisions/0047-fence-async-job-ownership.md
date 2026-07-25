---
component: adr-async-job-ownership-fencing
subsystem: research-orchestration
layer: decision
doc_type: adr
status: proposed
tags:
  - adr
  - async-jobs
  - fencing
  - ownership
  - retries
  - proposed
code_paths:
  - portal/backend/service/async_jobs/repository.py
  - portal/backend/workers
  - portal/backend/db/models.py
  - docs/architecture/research-orchestration/RESEARCH_ASYNC_JOB_BOUNDARY.md
---
# ADR 0047: Fence Async Job Ownership

## Status

Proposed on 2026-07-25. This is a required cleanup decision, not completed
behavior.

## Context

The shared async queue uses row locking to claim work and timeout-based reclaim
for abandoned jobs. A claim records `lock_owner` and `locked_at`, but completion
and failure currently accept only `job_id`. A slow worker can therefore finish
after its claim was reclaimed and overwrite the newer owner's result. Long jobs
also have no heartbeat to distinguish healthy work from abandonment.

## Decision

A claim will return an opaque ownership token and monotonic claim generation.
Heartbeat, completion, failure, checkpoint, and side-effect commits must compare
that token/generation in the same transaction as their update. Reclaim advances
the generation. A stale worker cannot commit a result or mutate job-owned
artifacts.

Long-running handlers will heartbeat at a bounded interval and store explicit,
idempotent checkpoints where the workload supports resume. Cancellation and
retry semantics will remain visible in the job record.

## Invariants

- At most one current claim generation may commit job-owned effects.
- A stale or unknown owner fails loudly without changing terminal state.
- Heartbeat expiry is reclaim evidence; wall-clock duration alone is not proof
  that a current owner may still commit.
- Retries are idempotent or resume from an explicit validated checkpoint.
- Completion/failure and the final owned side effect are transactional where
  possible, or use a durable idempotency key where not.
- Concurrency and retry budgets remain bounded and observable.

## Consequences

The queue schema and worker API will change. Every worker must propagate the
claim token and add heartbeat behavior for long jobs. This is intentional:
silent stale-worker success is less acceptable than an explicit failed claim.

## Rejected Alternatives

- Increase the running timeout and assume jobs finish in time.
- Trust `worker_id` without a unique claim generation.
- Let the latest result win.
- Add per-worker in-memory locks.
- Create a second queue only for research jobs.

## Enforcing Tests Or Evidence

Current evidence is incomplete:

- `tests/test_portal/test_async_jobs_partition_hash.py` protects stable
  partitioning and throttled timeout reclaim.
- `portal/backend/service/async_jobs/repository.py` shows current owner metadata
  is cleared on completion/failure without an owner comparison.

Required before acceptance:

- stale owner cannot complete or fail after reclaim;
- current owner heartbeat extends ownership;
- generation increments on retry/reclaim;
- duplicate execution has one committed side effect;
- checkpoint resume is deterministic and bounded.

## References

- [ADR 0039: Shared Async Jobs For Research Dispatch](0039-use-shared-async-jobs-for-research-dispatch.md)
- [Research Async Job Boundary](../research-orchestration/RESEARCH_ASYNC_JOB_BOUNDARY.md)
