---
component: botlens-observability-migration-checklist
subsystem: storage
layer: reference
doc_type: architecture
status: active
tags:
  - storage
  - runtime
  - observability
  - migration
  - botlens
code_paths:
  - portal/backend/db/models.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/storage/repos/lifecycle.py
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/container_runtime.py
---
# BotLens Observability Migration Checklist

This checklist is the manual follow-up artifact for Observability Contract v1.

The backend changes do **not** execute destructive DDL automatically.

## Target Ownership Model

Target schemas:

- `public`: operator-facing domain rows and control-plane metadata
- `runtime_state`: authoritative runtime state, read models, lifecycle checkpoints, replay/event ledgers
- `observability_events`: append-only diagnostic/event records that are not domain truth
- `observability_metrics`: optional persisted rollups if metrics are ever stored in Postgres instead of exported elsewhere

Current implementation still stores the affected tables in the default schema. The classifications below describe the intended ownership after manual migration.

## Table Classification

| Current table | Recommended ownership | Decision | Notes |
|---|---|---|---|
| `portal_bots` | `public` | stay where it is logically | Bot identity and operator control state are domain/control-plane truth. |
| `portal_bot_runs` | `public` | stay where it is logically | Run registry and operator-facing status belong with control-plane domain rows. |
| `portal_bot_run_lifecycle` | `runtime_state` | move schema | Durable current run lifecycle state; consumed as runtime truth. |
| `portal_bot_run_lifecycle_events` | `runtime_state` | move schema | Append-only lifecycle checkpoint trail; runtime replay/audit, not observability-only metrics. |
| `portal_bot_run_view_state` | `runtime_state` | move schema | BotLens read-model cache for run/symbol latest state. |
| `portal_bot_run_events` | mixed today | split / replace | Currently mixes raw runtime/BotLens replay facts and observability drift inside JSON payloads. |
| `portal_bot_run_steps` | `observability_events` | deprecate or move schema | Legacy step-trace profiling table, not BotLens runtime truth. |

## Table Move / Rename Recommendations

### Stay

- `portal_bots`
- `portal_bot_runs`

### Move Schema

- `portal_bot_run_lifecycle` -> `runtime_state.portal_bot_run_lifecycle`
- `portal_bot_run_lifecycle_events` -> `runtime_state.portal_bot_run_lifecycle_events`
- `portal_bot_run_view_state` -> `runtime_state.portal_bot_run_view_state`

### Split / Replace

- `portal_bot_run_events`
  - keep raw runtime and BotLens replay facts as runtime truth in `runtime_state.bot_runtime_events` or an equivalent final name,
  - remove observability summaries from the raw payload contract,
  - do not reuse this ledger as a metrics warehouse.

### Deprecate

- `portal_bot_run_steps`
  - preferred direction: move to `observability_events.portal_bot_run_steps`,
  - acceptable alternative: drop after consumers are confirmed gone.

## Column Cleanup Recommendations

### `portal_bot_run_events`

Likely normalize out of heavy JSON:

- `series_key`
- `bridge_session_id`
- `bridge_seq`
- `run_seq`

Retain as JSON for now:

- `facts`
- lifecycle detail blobs that are event-type specific
- engine/runtime event payloads that are naturally sparse or heterogeneous

Recommendation:

- keep `event_type`, `known_at`, `event_time`, `critical`, `schema_version`, `run_id`, `bot_id`, `seq` as typed columns,
- add typed columns for the routing fields above before any large replay/query workload depends on JSON scans.

### `portal_bot_run_view_state`

Retain as JSON for now:

- `payload`

Potential later split only if pressure appears:

- selected run summary health counts,
- symbol activity markers used for coarse filtering.

Today there is not enough proven pressure to justify deconstructing the view-state payload eagerly.

### `portal_bot_run_lifecycle` and `portal_bot_run_lifecycle_events`

Retain as typed columns:

- `phase`
- `status`
- `owner`
- `checkpoint_at`

Retain as JSON for now:

- `metadata`
- `failure`

Rename consideration:

- current `metadata` key is acceptable, but `lifecycle_metadata` is the clearer application name and already used in the ORM layer.

### `portal_bot_run_steps`

Keep typed:

- `step_name`
- `started_at`
- `ended_at`
- `duration_ms`
- `ok`

Retain as JSON for now:

- `context`

## Backfill / Compatibility Considerations

| Change | Backfill needed | Data can be dropped | Dual-write needed | Coordinated release needed | Notes |
|---|---|---|---|---|---|
| Stop persisting `typed_delta_metrics` in raw BotLens event payloads | No | Yes, for the legacy field | No | No | New code already stops writing it. Existing rows can remain until cleanup. |
| Move lifecycle tables into `runtime_state` schema | No | No | Prefer short temporary dual-read or table rename window | Yes | Readers and writers must switch together or use DB views during cutover. |
| Move `portal_bot_run_view_state` into `runtime_state` schema | No | No | Prefer short temporary dual-read or view-based compatibility | Yes | Bootstraps and projector writes must see the same table. |
| Split `portal_bot_run_events` into runtime truth vs observability storage | Maybe, depends on preserved history requirements | Observability-only payload fragments can be dropped | Possibly | Yes | If historical replay must survive intact, copy raw rows first and only then cut reads. |
| Deprecate or move `portal_bot_run_steps` | No, unless external reports still depend on it | Yes, if profiling history is non-essential | No | Yes, if consumers still query it | Confirm dashboards/jobs before drop. |

## Destructive Actions To Plan Manually

Do **not** run these from application code:

- drop legacy `typed_delta_metrics` from historical `portal_bot_run_events.payload` rows if storage savings matter,
- rename or move `portal_bot_run_lifecycle`,
- rename or move `portal_bot_run_lifecycle_events`,
- rename or move `portal_bot_run_view_state`,
- split and/or rename `portal_bot_run_events`,
- move or drop `portal_bot_run_steps`,
- create compatibility views only for the migration window and remove them after cutover.

## Legacy Path Decisions

| Legacy item | Classification | Manual action |
|---|---|---|
| `typed_delta_metrics` inside raw event payloads | REPLACE IMMEDIATELY | Existing rows may be left in place short-term; no new writes. |
| `portal_bot_run_steps` as BotLens observability storage | DEPRECATE | Confirm consumers, then move to `observability_events` or drop. |
| `bot_projection_refresh` compatibility envelope | DEPRECATE | Remove once worker/runtime stops sending it; active runtime logs should use lifecycle-event terminology only. |
| Queue-full/drop log-only observability | REMOVE | No DB action; already replaced by structured events + metrics. |

## Rollout Order

1. Deploy the backend observability contract code without DDL changes.
2. Confirm dashboards/alerting read the new metrics/events instead of legacy logs.
3. Stop any downstream dependency on `typed_delta_metrics`.
4. Plan manual schema moves for `portal_bot_run_lifecycle`, `portal_bot_run_lifecycle_events`, and `portal_bot_run_view_state`.
5. Decide whether `portal_bot_run_events` is renamed in place or replaced with a new runtime-state ledger table.
6. Confirm whether `portal_bot_run_steps` still has consumers before moving or dropping it.
7. Remove temporary compatibility views or dual-read bridges after cutover.
