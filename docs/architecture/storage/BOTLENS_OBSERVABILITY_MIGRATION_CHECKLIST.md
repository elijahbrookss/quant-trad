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
  - portal/backend/db/session.py
  - portal/backend/service/observability.py
  - portal/backend/service/observability_exporter.py
  - portal/backend/service/storage/repos/observability.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/storage/repos/lifecycle.py
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/container_runtime.py
  - scripts/db/manual_migration_botlens_observability_persistence_v1.sql
---
# BotLens Observability Migration Checklist

This checklist is the manual follow-up artifact for the durable BotLens backend observability/export pass.

The backend changes do **not** execute destructive DDL automatically.

Owner-facing DDL artifact:

- `scripts/db/manual_migration_botlens_observability_persistence_v1.sql`

## Target Ownership Model

Target schemas:

- `public`: operator-facing domain rows and control-plane metadata
- `runtime_state`: authoritative runtime state, read models, lifecycle checkpoints, replay/event ledgers
- `observability_events`: append-only diagnostic/event records that are not domain truth
- `observability_metrics`: append-only dashboard metric samples/measurements

Current application code now persists backend observability into `observability_events` and `observability_metrics`.

Runtime-state tables are still physically in the default schema until the owner performs the manual move/cutover.

## Table Classification

| Current table | Recommended ownership | Decision | Notes |
|---|---|---|---|
| `portal_bots` | `public` | stay where it is logically | Bot identity and operator control state are domain/control-plane truth. |
| `portal_bot_runs` | `public` | stay where it is logically | Run registry and operator-facing status belong with control-plane domain rows. |
| `portal_bot_run_lifecycle` | `runtime_state` | move schema | Durable current run lifecycle state; consumed as runtime truth. |
| `portal_bot_run_lifecycle_events` | `runtime_state` | move schema | Append-only lifecycle checkpoint trail; runtime replay/audit, not observability-only metrics. |
| `portal_bot_run_view_state` | `runtime_state` | move schema | BotLens read-model cache for run/symbol latest state. |
| `portal_bot_run_events` | `runtime_state` | keep as runtime truth, then move schema manually | Runtime/BotLens replay ledger. Common routing fields are now projected out for app/query consumers, but the table is still physically in `public` until manual cutover. |
| `portal_bot_run_steps` | `observability_events` | KEEP TEMPORARILY, then move or drop manually | Legacy step-trace profiling table. Not BotLens runtime truth. |
| `observability_events.botlens_backend_events_v1` | `observability_events` | REPLACE IMMEDIATELY | Durable backend event store for Grafana and incident queries. |
| `observability_metrics.botlens_backend_metric_samples_v1` | `observability_metrics` | REPLACE IMMEDIATELY | Durable backend metric sample store for Grafana. |

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
  - keep raw runtime and BotLens replay facts as runtime truth,
  - do not reuse this ledger as the backend observability warehouse,
  - expose migration-window compatibility through `runtime_state` views,
  - and plan the eventual physical move to `runtime_state`.

- Observability storage
  - `observability_events.botlens_backend_events_v1`
  - `observability_metrics.botlens_backend_metric_samples_v1`

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
- use the manual migration SQL to add `runtime_state.bot_runtime_events_v1` as the query-facing typed view during the migration window,
- optionally promote the routing fields to real typed columns later if replay query pressure justifies it.

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

### Durable observability tables

Keep typed:

- `observed_at`
- `metric_name` / `event_name`
- `metric_kind`
- `value`
- `level`
- `component`
- `bot_id`
- `run_id`
- `instrument_id`
- `series_key`
- `queue_name`
- `pipeline_stage`
- `message_kind`
- `storage_target`
- `failure_mode`

Retain as JSON:

- `labels` on metric samples
- `details` on events

## Backfill / Compatibility Considerations

| Change | Backfill needed | Data can be dropped | Dual-write needed | Coordinated release needed | Notes |
|---|---|---|---|---|---|
| Stop persisting `typed_delta_metrics` in raw BotLens event payloads | No | Yes, for the legacy field | No | No | New code already stops writing it. Existing rows can remain until cleanup. |
| Move lifecycle tables into `runtime_state` schema | No | No | Prefer short temporary dual-read or table rename window | Yes | Readers and writers must switch together or use DB views during cutover. |
| Move `portal_bot_run_view_state` into `runtime_state` schema | No | No | Prefer short temporary dual-read or view-based compatibility | Yes | Bootstraps and projector writes must see the same table. |
| Create `observability_events.botlens_backend_events_v1` | No | No | No | Yes | App now writes here; owner should create indexes/views first in production. |
| Create `observability_metrics.botlens_backend_metric_samples_v1` | No | No | No | Yes | App now writes here; owner should create indexes/views first in production. |
| Keep `portal_bot_run_events` as runtime truth and stop using it for observability | No | Observability-only payload fragments can be dropped | No | Yes | New backend observability writes no longer go here. |
| Deprecate or move `portal_bot_run_steps` | No, unless external reports still depend on it | Yes, if profiling history is non-essential | No | Yes, if consumers still query it | Confirm dashboards/jobs before drop. |

## Destructive Actions To Plan Manually

Do **not** run these from application code:

- drop legacy `typed_delta_metrics` from historical `portal_bot_run_events.payload` rows if storage savings matter,
- rename or move `portal_bot_run_lifecycle`,
- rename or move `portal_bot_run_lifecycle_events`,
- rename or move `portal_bot_run_view_state`,
- move `portal_bot_run_events` into `runtime_state` or replace it with a final runtime-state table name,
- move or drop `portal_bot_run_steps`,
- create compatibility views only for the migration window and remove them after cutover.

## Legacy Path Decisions

| Legacy item | Classification | Manual action |
|---|---|---|
| `typed_delta_metrics` inside raw event payloads | REPLACE IMMEDIATELY | Existing rows may be left in place short-term; no new writes. |
| `portal_bot_run_steps` as BotLens observability storage | DEPRECATE | Confirm consumers, then move to `observability_events` or drop. |
| `bot_projection_refresh` compatibility envelope | DEPRECATE | Remove once worker/runtime stops sending it; active runtime logs should use lifecycle-event terminology only. |
| Queue-full/drop log-only observability | REMOVE | No DB action; already replaced by structured events + metrics. |
| Grafana reading payload-derived BotLens observability from `portal_bot_run_events` | REMOVE | Point dashboards to `observability_events.botlens_backend_events_v1` and `observability_metrics.botlens_backend_metric_samples_v1`. |

## Rollout Order

1. Apply `scripts/db/manual_migration_botlens_observability_persistence_v1.sql` in the target database.
2. Deploy the backend code that includes the exporter and durable observability writers.
3. Confirm new rows appear in `observability_events.botlens_backend_events_v1` and `observability_metrics.botlens_backend_metric_samples_v1`.
4. Point Grafana/alerts at the new observability tables and stop querying payload-derived logic.
5. Stop any downstream dependency on `typed_delta_metrics`.
6. Use the `runtime_state.*_v1` compatibility views during the migration window while planning physical table moves for:
   - `portal_bot_run_lifecycle`
   - `portal_bot_run_lifecycle_events`
   - `portal_bot_run_view_state`
   - `portal_bot_run_events`
7. Confirm whether `portal_bot_run_steps` still has consumers before moving or dropping it.
8. Perform the physical runtime-state table moves or replacements manually.
9. Remove temporary compatibility views after reads and writes are fully cut over.
