---
component: adr-shared-async-jobs-research-dispatch
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
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
  - config/defaults.yaml
  - cli/main.py
  - docs/architecture/research-orchestration/RESEARCH_ASYNC_JOB_BOUNDARY.md
---
# ADR 0039: Use Shared Async Jobs for Research Dispatch

## Status

Accepted on 2026-06-16.

## Context

Research checks and sweeps can be cheap when scoped narrowly, but realistic
strategy exploration requires wider windows, multiple instruments, and variant
sweeps. Holding a CLI HTTP request open for that work makes agents waste time
polling and turns long-running research into terminal management instead of
analysis.

The platform already has a durable async job boundary for backend work. Adding
another research-specific queue would create a second lifecycle model for
claiming, retries, stale-job recovery, and status inspection.

## Decision

Research dispatch uses the shared `portal_async_jobs` queue.

The research API exposes dispatch/status/result routes for
`research_check_run` and `research_check_sweep`. Dispatch records the original
research request, computes a stable request fingerprint, and deduplicates
equivalent in-flight jobs. Dedicated research workers claim only research job
types and execute the existing `run_research_check` and
`sweep_research_checks` service functions.

The CLI adds `--dispatch` to research check commands and a `qt research jobs`
surface for status and completed results. Synchronous check routes remain
available for small checks and local debugging.

## Consequences

- Agents can launch expensive research work and return to analysis instead of
  holding a terminal open.
- Research worker capacity is independent from indicator worker capacity.
- Research checks keep one source of analytical truth because workers call the
  same service functions as synchronous routes.
- Job status and result payloads are durable enough for agent workflows, but
  full result retention still follows the shared async job storage tradeoffs.
- Future sweep orchestration should compose dispatch calls rather than adding
  new ad hoc polling loops.

## References

- [Research Async Job Boundary](../research-orchestration/RESEARCH_ASYNC_JOB_BOUNDARY.md)
- [Research Orchestration Boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
- [Research Memory Boundary](../research-memory/RESEARCH_MEMORY_BOUNDARY.md)
- [Engineering Contract](../../contracts/platform/03_engineering_contract.md)
