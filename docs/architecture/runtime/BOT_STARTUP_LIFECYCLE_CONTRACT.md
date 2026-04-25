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
  - portal/backend/service/bots/botlens_runtime_state.py
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
- persisted canonical lifecycle truth,
- backend/container bootstrap handoff,
- startup failure semantics.

## `start_bot` Guarantee

When `start_bot(bot_id)` returns successfully:
- the backend has already created the `run_id`,
- the run row exists in `portal_bot_runs`,
- the latest lifecycle state is durably queryable from canonical lifecycle rows in `portal_bot_run_events`,
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

Canonical lifecycle truth:
- `portal_bot_run_events` with `event_type=botlens_domain.run_*`

Synchronized helper lifecycle views:
- `portal_bot_run_lifecycle`
- `portal_bot_run_lifecycle_events`

Related run/report state:
- `portal_bot_runs`
- `portal_bot_run_steps`
- `portal_bot_run_events`

The runtime ledger is the durable startup/runtime truth.
The helper lifecycle row and helper event table are synchronized convenience projections of that canonical append path.
The diagnostics projection derives a UI-facing run summary from canonical lifecycle ordering without changing the durable schema.

Lifecycle checkpoint ordering rule:
- canonical lifecycle `seq` is allocated inside the storage boundary against `portal_bot_run_events` under a per-run lock so backend and container writers cannot race the same sequence number,
- `portal_bot_run_lifecycle_events.seq` mirrors that committed ordering for helper diagnostics reads,
- and lifecycle `message` fields are bounded to the durable column contract while full structured error detail remains in `failure`.

Terminal truth rule:
- the latest canonical lifecycle event is the only authoritative current run state,
- `completed` is only valid when every planned worker explicitly reports terminal status `completed`,
- supervision drain by itself is not allowed to infer `completed`,
- missing worker terminal reports resolve deterministically to `startup_failed` before `live` or `crashed` after `live`,
- mixed explicit worker terminal outcomes resolve by precedence: failure/crash, then degraded, then stopped, then completed.

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
- append lifecycle truth through `record_bot_run_lifecycle_checkpoint(...)`, with helper tables synchronized afterward,
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
- `runtime_observability.runtime_state`
- `runtime_observability.progress_state`
- `runtime_observability.last_useful_progress_at`
- `runtime_observability.recent_transitions`
- `runtime_observability.degraded`
- `runtime_observability.churn`
- `runtime_observability.pressure`
- `runtime_observability.terminal`

This metadata is intentionally backend-shaped rather than frontend-polished.

Runtime lifecycle guardrails:
- startup bootstrap is only legal while runtime state is `initializing` or `awaiting_first_snapshot`,
- that startup admission rule is phase-aware and shared across container runtime, intake routing, run projection, and symbol projection,
- first-runtime-facts lifecycle progress is a control-plane signal and must not depend on delivery of bulky live fact payloads,
- the supervisor must drain lifecycle/control messages before runtime-facts data messages so backtest fact pressure cannot leave a run stuck at `awaiting_first_snapshot`,
- startup-live readiness must be reconciled against canonical `portal_bot_run_events` facts when the run is still awaiting first runtime facts,
- bootstrap facts do not satisfy startup-live readiness; a series becomes live only from a canonical live fact committed after that series bootstrap seq,
- bridge `runtime_facts_started` messages are hints to run canonical reconciliation, not standalone authority to mark a run live,
- once the run reaches `live`, lifecycle/runtime state must not regress back to `awaiting_first_snapshot`,
- post-live continuity gaps are modeled as degraded runtime state instead of startup restart semantics,
- degraded recovery returns to `live` from real forward runtime facts rather than by replaying cold-start bootstrap,
- lifecycle checkpoint payloads must use the canonical `status_for_phase(phase)` result; mismatched phase/status pairs are invalid,
- `portal_bots.status` and `portal_bot_runs.status` are lifecycle-owned fields synchronized from the latest checkpoint rather than independently stamped side paths,
- and terminal lifecycle resolution must consume explicit worker terminal reports rather than inferring completion from process exit alone.

## Diagnostics Projection

`GET /bots/{bot_id}/runs/{run_id}/lifecycle-events` now returns:
- enriched `events` with local `checkpoint_status`,
- a `summary` block with root failure, last success, container boot facts, and worker failure breakdown,
- a `runtime` block with current runtime state, recent runtime transitions, degraded markers, churn/progress facts, last useful progress time, top pressure, and terminal classification,
- a `consistency` block with lifecycle and run-view snapshot markers (`lifecycle_event_seq`, `runtime_view_seq`, `runtime_known_at`, `runtime_event_time`, `read_completed_at`) so transient cross-read drift is explicit instead of silent,
- synthesized `checkpoints` with `completed` / `running` / `failed` / `pending` / `skipped`.

This projection keeps raw lifecycle event ordering intact while separating local checkpoint outcome from overall run status.
It also ranks root cause toward the earliest concrete structured worker failure instead of surfacing only the final generic degraded/crashed terminal text.

Rejected runtime-state transitions are also part of durable lifecycle truth:
- they append a canonical lifecycle event at the current phase/status,
- they carry structured failure fields including `from_state`, `attempted_to_state`, `transition_reason`, and `source_component`,
- and they remain queryable through the lifecycle diagnostics trail instead of being log-only observer output.

Worker/runtime failures may also carry:
- `component`
- `operation`
- `path`
- `errno`

These fields exist so diagnostics can point at the failing boundary directly, for example artifact-finalize cleanup races, bridge transport issues, or other classified runtime exceptions.

## Failure Semantics

`startup_failed` means startup never reached `live`.
Examples:
- invalid startup configuration,
- missing strategy snapshot,
- wallet/bootstrap preparation failure,
- worker failure before first live snapshot.
- a container supervision pass that ended with failed workers before any series became live.

`crashed` means the run had already escaped startup ownership and then died or was orphan-recovered.

Crash/orphan recovery updates current lifecycle state through the canonical append path so the portal can explain:
- which run died,
- the latest known phase,
- the crash reason source (`watchdog`, container exit, stale heartbeat).

Crash/orphan recovery must not invent terminal truth:
- watchdog crash marking requires an active latest run context,
- it may not overwrite runs already marked `completed`, `stopped`, `startup_failed`, or `crashed`,
- and terminal bot/run status fields stay derived from canonical lifecycle appends rather than ad hoc status mutation.
