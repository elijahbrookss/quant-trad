---
component: bot-startup-lifecycle-contract
subsystem: portal-runtime
layer: contract
doc_type: architecture
status: active
tags:
  - runtime
  - startup
  - lifecycle
  - contract
code_paths:
  - portal/backend/service/bots/startup_lifecycle.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/bots/bot_run_diagnostics_projection.py
  - portal/backend/service/bots/runtime_control_service.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/bots/runner.py
  - portal/backend/service/bots/startup_validation.py
  - portal/backend/service/storage/repos/lifecycle.py
  - portal/backend/db/models.py
---
# Bot Startup Lifecycle Contract

## Purpose

Define the backend-owned startup contract for bot runs.

This contract makes the portal backend the single authoritative owner of:
- startup orchestration,
- `run_id` creation,
- persisted lifecycle phase truth,
- backend/container bootstrap handoff,
- startup failure semantics.

## `start_bot` Guarantee

When `start_bot(bot_id)` returns successfully:
- the backend has already created the `run_id`,
- the run row exists in `portal_bot_runs`,
- the current lifecycle row exists in `portal_bot_run_lifecycle`,
- the active lifecycle phase is at least `awaiting_container_boot`,
- the response includes `active_run_id`,
- the bot is no longer represented as vague `starting` without phase detail.

Successful return does not guarantee that workers are live.
It guarantees that backend-owned startup completed and the container accepted the backend-owned contract.

## Phase Ownership

Backend-owned phases:
- `start_requested`
- `validating_configuration`
- `resolving_strategy`
- `resolving_runtime_dependencies`
- `preparing_run`
- `stamping_starting_state`
- `launching_container`
- `container_launched`
- `awaiting_container_boot`

Container/runtime-reported phases:
- `container_booting`
- `loading_bot_config`
- `claiming_run`
- `loading_strategy_snapshot`
- `preparing_wallet`
- `planning_series_workers`
- `spawning_series_workers`
- `waiting_for_series_bootstrap`
- `warming_up_runtime`
- `runtime_subscribing`
- `awaiting_first_snapshot`
- `live`

Terminal or abnormal phases:
- `degraded`
- `telemetry_degraded`
- `startup_failed`
- `crashed`
- `stopped`
- `completed`

## Persistence Model

Current lifecycle state:
- `portal_bot_run_lifecycle`

Append-only checkpoint trail:
- `portal_bot_run_lifecycle_events`

Related run/report state:
- `portal_bot_runs`
- `portal_bot_run_steps`
- `portal_bot_run_events`
- `portal_bot_run_view_state`

The lifecycle row is the current durable startup/runtime truth.
The lifecycle event table is the ordered checkpoint trail.
The diagnostics projection derives a UI-facing run summary from that ordered trail without changing the durable schema.

## Projection Rules

Bot projection is pure.

Projection consumes:
- bot row,
- current run row,
- current lifecycle row,
- latest view-state row,
- explicitly supplied container state.

Projection does not call Docker inspect.
Infrastructure I/O must happen before projection and be passed in explicitly.

## Backend/Container Handshake

The backend passes at minimum:
- `QT_BOT_RUNTIME_BOT_ID`
- `QT_BOT_RUNTIME_RUN_ID`

The container must:
- claim the injected `run_id`,
- warn if it had to generate a guarded fallback,
- report lifecycle checkpoints back into the backend-owned lifecycle tables,
- treat bootstrap progress as updates to the backend contract rather than local-only logs.

## Series Bootstrap Progress

Lifecycle metadata carries structured startup progress:
- `total_series`
- `workers_planned`
- `workers_spawned`
- `bootstrapped_series`
- `warming_series`
- `awaiting_first_snapshot_series`
- `live_series`
- `failed_series`
- per-symbol/per-series status detail

This metadata is intentionally backend-shaped rather than frontend-polished.

## Diagnostics Projection

`GET /bots/{bot_id}/runs/{run_id}/lifecycle-events` now returns:
- enriched `events` with local `checkpoint_status`,
- a `summary` block with root failure, last success, container boot facts, and worker failure breakdown,
- synthesized `checkpoints` with `completed` / `running` / `failed` / `pending` / `skipped`.

This projection keeps raw lifecycle event ordering intact while separating local checkpoint outcome from overall run status.

## Failure Semantics

`startup_failed` means startup never reached `live`.
Examples:
- invalid startup configuration,
- missing strategy snapshot,
- wallet/bootstrap preparation failure,
- worker failure before first live snapshot.
- a container supervision pass that ended with failed workers before any series became live.

`crashed` means the run had already escaped startup ownership and then died or was orphan-recovered.

Crash/orphan recovery updates the current lifecycle row so the portal can explain:
- which run died,
- the latest known phase,
- the crash reason source (`watchdog`, container exit, stale heartbeat).
