---
component: run-report-artifact-bundle
subsystem: reporting
layer: service
doc_type: architecture
status: active
tags:
  - reporting
  - runtime
  - artifacts
  - provenance
code_paths:
  - portal/backend/service/reports/artifacts.py
  - portal/backend/controller/reports.py
  - src/engines/bot_runtime/runtime/mixins/execution_loop.py
  - src/engines/bot_runtime/runtime/mixins/runtime_events.py
  - config/defaults.yaml
---
# Run Report Artifact Bundle Architecture

## Purpose

Quant-Trad report exports are now run-scoped artifact bundles rooted at:

`reports/bot_id=<bot_id>/run_id=<run_id>/`

The bundle is the canonical analysis package for a run. Exporting a report means zipping that run directory.

## Lifecycle

### In Progress

During runtime, the bundle is created with:

- `manifest.json` with `status=in_progress`
- `run/metadata.json`
- `run/config.json`
- `run/series.json`
- `run/indicators.json`
- append-friendly spool files under `.spool/`

Runtime writes are incremental so aborted or failed runs still leave analyzable partial artifacts.

### Finalize

On completion, abort, or failure:

- pending spool data is materialized into final files,
- `summary/summary.json` and `summary/run_summary.md` are written,
- `.spool/` is removed,
- `manifest.json` is updated to `completed`, `aborted`, or `failed`,
- and the directory may be zipped if `reports.artifacts.compress_zip_on_finalize` is enabled.

## Artifact Provenance

Each file recorded in `manifest.json` carries provenance:

- `runtime`
  - emitted directly from the running timeline
- `postrun_db`
  - derived after the run from persisted DB rows such as trades/trade events
- `postrun_derived`
  - summaries or compactions generated after runtime completion

This keeps runtime truth separate from post-run enrichment.

## Storage Contract

The report bundle may contain:

- `execution/runtime_events.jsonl`
- `execution/decision_trace.{csv|parquet}`
- `execution/trades.{csv|parquet}`
- `execution/trade_events.{csv|parquet}`
- `series/symbol=.../timeframe=.../candles.{csv|parquet}`
- `series/symbol=.../timeframe=.../indicators/*.{csv|parquet}`
- optional overlay streams under `series/.../overlays/`
- summary and manifest files

`reports.artifacts.output_format` controls tabular output format and currently supports:

- `parquet`
- `csv`

Event streams remain JSONL and metadata remains JSON/Markdown.

## Source-of-Truth Rules

- Indicator history belongs in the run bundle, not dedicated per-indicator DB tables.
- Candle/regime analytics are captured from the same runtime frames that drove decisions.
- Report consumers must prefer run-bundle artifacts over alternate reconstruction paths.
- DB enrichments are allowed, but only as explicit post-run files with provenance.
