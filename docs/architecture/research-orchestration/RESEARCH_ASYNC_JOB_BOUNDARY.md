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

Workers call the same `run_research_check` and `sweep_research_checks` service
functions used by synchronous routes. Job records hold queue status, attempts,
timestamps, errors, and completed results.

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
