---
component: adr-per-run-leases
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - runtime
  - runner
  - lifecycle
  - leasing
code_paths:
  - portal/backend/db/models.py
  - portal/backend/db/session.py
  - portal/backend/service/storage/repos/run_leases.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/bots/run_lease.py
  - portal/backend/service/bots/bot_watchdog.py
  - portal/backend/service/bots/runner.py
  - scripts/db/manual_migration_bot_run_leases_v1.sql
---
# ADR 0025: Use Per-Run Leases

## Status

Accepted on 2026-05-19.

## Context

Bot runtime ownership was tracked mainly through `portal_bots.runner_id` and a
bot-level heartbeat. That was useful for local Docker supervision, but it was
too coarse for future runner agents:

- a bot can have historical and active runs with different ownership evidence,
- fixed Docker container names can make old-run evidence look like current-run
  evidence,
- backend heartbeats can go stale while a runtime process is still alive,
- future VPS/home-server runners need a database-level claim that is not tied
  to Docker.

## Decision

Use `portal_bot_run_leases` as the runner-agnostic active ownership guard for a
run.

The backend creates the run row, generates an opaque lease token, stores only
the token hash, and acquires the lease before launching the runtime. The token
is passed to the runner process. The runtime renews the lease until terminal
exit and releases it on clean shutdown.

The watchdog treats a fresh run lease as stronger evidence than a stale bot-row
heartbeat. A stale heartbeat with a fresh lease is logged and skipped instead
of being marked degraded.

## Consequences

- Run ownership is scoped to `run_id`, not only `bot_id`.
- Runner agents can be dumb: claim/renew/release a lease and emit lifecycle
  checkpoints, without declaring symbol/provider capability.
- Docker remains an implementation detail; lease semantics work for any future
  runner target.
- A runtime that loses or cannot renew its lease fails loud instead of
  continuing to emit facts after ownership has expired.
- Operators can inspect lease owner, generation, expiry, and release state
  without exposing the raw lease token.

## References

- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Persistence Boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Runtime Composition Root](../execution-runtime/RUNTIME_COMPOSITION_ROOT.md)
