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
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/bots/runtime_composition.py
---

# Reporting Runtime Dependency Seams

Reporting remains a downstream collaborator of runtime execution.

## Seam Guidance

- Runtime orchestration emits canonical runtime artifacts/events first.
- Reporting derives from persisted runtime outputs and should not drive runtime decisions.
- Wiring points for report persistence should be explicit at composition/runtime orchestration boundaries, not hidden in unrelated imports.
- Report exports and report views depend on the persisted bot/run/trade graph; they should not rely on best-effort writes or silently dropped trade events.

## Bot Report Sources

Bot-run reporting now uses the bot ledger directly:

- `portal_bot_runs`
  - run metadata and summary surface
- `portal_bot_run_events`
  - `runtime.*` for decisions/execution/degradation/error truth
  - `series_bar.telemetry` for per-bar runtime telemetry/debug snapshots
  - `botlens.*` only when a report explicitly wants debugger/view artifacts
- `portal_bot_trades` and `portal_bot_trade_events`
  - trade state and lifecycle
- `portal_bot_run_steps`
  - profiling/dashboard telemetry

Research tables such as `candle_stats`, `regime_stats`, and `regime_blocks` are QuantLab surfaces.
Bot reports should use those DB tables directly when they need stats/regime exports rather than treating runtime snapshot payloads as the authoritative analytics source.
