---
component: reporting-runtime-dependency-seams
subsystem: reporting
layer: service
doc_type: architecture
status: active
tags:
  - reporting
  - runtime
  - seams
code_paths:
  - portal/backend/service/reports/report_service.py
  - portal/backend/service/reports/artifacts.py
  - portal/backend/controller/reports.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/bots/runtime_composition.py
---

# Reporting Runtime Dependency Seams

Reporting remains a downstream collaborator of runtime execution.

## Seam Guidance

- Runtime orchestration emits canonical runtime artifacts/events first.
- Reporting derives from persisted runtime outputs and should not drive runtime decisions.
- Wiring points for report persistence should be explicit at composition/runtime orchestration boundaries, not hidden in unrelated imports.
- Report exports are run-scoped artifact bundles under `reports/bot_id=<bot_id>/run_id=<run_id>/`.
- Report exports and report views depend on the persisted bot/run/trade graph plus finalized run-artifact bundles; they should not rely on best-effort writes or silently dropped trade events.

## Bot Report Sources

Bot-run reporting now uses two explicit sources:

- runtime-authored bundle artifacts
- post-run DB enrichments

Runtime-authored bundle artifacts:
- `run/metadata.json`, `run/config.json`, `run/series.json`, `run/indicators.json`
- `execution/runtime_events.jsonl`
- `execution/decision_trace.{csv|parquet}`
- `series/.../candles.{csv|parquet}`
- `series/.../indicators/*.{csv|parquet}`
- optional `series/.../overlays/*.jsonl`
- `summary/summary.json` and `summary/run_summary.md`
- `manifest.json` with bundle status and artifact provenance

Post-run DB enrichments:
- `portal_bot_runs`
  - run index and summary surface
- `portal_bot_trades` and `portal_bot_trade_events`
  - trade state and lifecycle
- `portal_bot_run_steps`
  - profiling/dashboard telemetry when needed for operator/debugger views

Important seam rule:
- indicator history belongs to the run artifact bundle, not to dedicated research tables,
- and any DB-derived file included in the bundle must be marked as `postrun_db` provenance in `manifest.json`.
