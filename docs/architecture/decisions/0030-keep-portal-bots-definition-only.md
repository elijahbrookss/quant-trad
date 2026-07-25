---
component: adr-portal-bots-definition-only
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - storage
  - persistence
  - bots
  - lifecycle
  - leasing
code_paths:
  - portal/backend/db/models.py
  - portal/backend/db/session.py
  - portal/backend/service/storage/repos/bots.py
  - portal/backend/service/storage/repos/lifecycle.py
  - portal/backend/service/storage/repos/run_leases.py
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/bots/bot_state_projection.py
  - portal/backend/service/bots/bot_watchdog.py
  - portal/backend/service/bots/runtime_control_service.py
  - portal/backend/service/bots/startup_service.py
  - scripts/db/manual_migration_portal_bot_definition_only_v1.sql
  - scripts/db/manual_migration_portal_bot_definition_only_indexes_v1.sql
  - scripts/db/manual_migration_canonical_lifecycle_ledger_v1.sql
---
# ADR 0030: Keep Portal Bots Definition Only

## Status

Accepted on 2026-05-31. Lifecycle storage ownership amended on 2026-07-24.

## Context

`portal_bots` had become a mixed table: bot definition, runtime status, runner
ownership, heartbeat, compact stats, and large `last_run_artifact` payloads all
shared one row. That made ordinary fleet reads expensive, amplified write
churn on a tiny table, and encouraged services to recover runtime truth from a
configuration row instead of the run-owned tables.

The run-owned tables already exist:

- `portal_bot_runs` for run identity, config snapshot, status, and summary,
- canonical lifecycle events in `portal_bot_run_events` for phase/status
  checkpoint history,
- `portal_bot_run_leases` for runner ownership and liveness,
- report materialization rows for report artifact readiness.

## Decision

`portal_bots` stores bot definitions only. Runtime readers must project bot
cards and API responses from bot definitions plus run, lifecycle, lease, and
report rows. Lifecycle history comes only from `portal_bot_run_events`;
`portal_bot_runs` stores a rebuildable current status/timestamp projection.
Writers must not write runtime status, summaries, artifacts, runner ownership,
or liveness to `portal_bots`.

The removed bot-row fields are `status`, `last_run_at`, `last_stats`,
`last_run_artifact`, `runner_id`, and `heartbeat_at`.

Watchdog recovery and container ownership checks read active or expired
`portal_bot_run_leases`, then join to the matching run and latest canonical
lifecycle event. Startup and runtime control append lifecycle events and update
the explicit run summary projection.

## Consequences

- Fleet reads avoid the large `portal_bots` toast payload and definition-row
  churn.
- Runtime ownership has one source: the per-run lease.
- Bot definitions remain stable and safe to edit independently of run state.
- Lifecycle history has one source: the canonical runtime-event ledger.
- Current run status is reconstructable from the latest lifecycle event.
- Report artifacts and summaries are recovered from report/run tables instead
  of cached on the bot row.
- Existing databases need a coordinated column-drop migration after matching
  backend/frontend code is deployed.

## References

- [Persistence Boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [ADR 0025: Use Per-Run Leases](0025-use-per-run-leases.md)
