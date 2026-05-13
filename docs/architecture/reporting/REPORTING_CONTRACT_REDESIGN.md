---
component: reporting-contract-redesign
subsystem: reporting
layer: contract
doc_type: architecture
status: active
tags:
  - reporting
  - research-dataset
  - diagnostics
  - export
  - comparison
  - automation-ready
code_paths:
  - portal/backend/controller/reports.py
  - portal/backend/service/reports/contract.py
  - portal/backend/service/reports/run_research_dataset.py
  - portal/backend/service/reports/export_bundle.py
  - portal/backend/service/reports/schemas.py
  - portal/backend/service/storage/repos/candles.py
  - portal/frontend/src/components/reports
---

# Reporting Contract Redesign

## Purpose

The reporting layer exposes a user-agnostic, deterministic reporting data
product for frontend views, downloads, CLI/debug workflows, and external
analysis tools.

`RunResearchDataset v1` is the canonical report payload. Frontend-shaped report
payloads are not part of the contract.

## Contract Shape

The public API exposes typed, versioned report surfaces:

- `ReportReadiness`
- `RunResearchDataset`
- `RunReportSummary`
- `ReportSections`
- `TradeDataset`
- `DecisionDataset`
- `SignalDataset`
- `ReportTimeseries`
- `ReportContext`
- `CandleCatalog`
- `CandleDataset`
- `MetricExplanation`
- `PortfolioMetrics`
- `RunComparisonResult`
- `ReportDiagnostics`
- `OperationalHealth`
- `ExportManifest`
- `ExportBundle`

Raw database tables, BotLens snapshots, and frontend-only view models are
supporting/internal sources. They are not the reporting contract.

## Routes

| Route | Contract | Notes |
|---|---|---|
| `GET /api/reports` | `ReportList` | Lightweight catalog rows with summary and readiness state. |
| `GET /api/reports/{run_id}` | `RunResearchDataset` | Canonical complete dataset. |
| `GET /api/reports/{run_id}/readiness` | `ReportReadiness` | Cheap readiness/status read. |
| `GET /api/reports/{run_id}/summary` | `RunReportSummary` | Compact run summary. |
| `GET /api/reports/{run_id}/sections` | `ReportSections` | Section availability, row counts, and unsupported states. |
| `GET /api/reports/{run_id}/trades` | `TradeDataset` | Paged/filterable trade rows. |
| `GET /api/reports/{run_id}/decisions` | `DecisionDataset` | Paged/filterable decision rows. |
| `GET /api/reports/{run_id}/signals` | `SignalDataset` | Paged/filterable signal rows when available. |
| `GET /api/reports/{run_id}/timeseries/{section}` | `ReportTimeseries` | Paged canonical chart/analysis series such as equity, drawdown, returns, exposure, positions, and rolling metrics. |
| `GET /api/reports/{run_id}/context` | `ReportContext` | Paged context/world-state sections such as decision context, trade context, indicator snapshots, and market state. |
| `GET /api/reports/{run_id}/candles/catalog` | `CandleCatalog` | Run-scoped candle availability and continuity summary without full candle rows. |
| `GET /api/reports/{run_id}/candles` | `CandleDataset` | Bounded candle rows for an explicit instrument/timeframe/window. |
| `GET /api/reports/{run_id}/trades/{trade_id}/candle-window` | `CandleDataset` | Bounded entry/exit context window. |
| `GET /api/reports/{run_id}/decisions/{decision_id}/candle-window` | `CandleDataset` | Bounded decision context window. |
| `GET /api/reports/{run_id}/signals/{signal_id}/candle-window` | `CandleDataset` | Bounded signal context window. |
| `GET /api/reports/{run_id}/diagnostics` | `ReportDiagnostics` | Normalized diagnostics relevant to the report. |
| `GET /api/reports/{run_id}/metrics` | `ReportMetrics` | Metrics, accounting, data-quality, execution, and operational sections. |
| `GET /api/reports/{run_id}/operational-health` | `OperationalHealth` | Runtime/load/projection health for scale assessment. |
| `GET /api/reports/{run_id}/metrics/{metric_name}/explanation` | `MetricExplanation` | Formula and source references for supported metrics. |
| `GET /api/reports/{run_id}/export/manifest` | `ExportManifest` | Files and unavailable sections for export. |
| `POST /api/reports/{run_id}/export` | `ExportBundle` | Zip generated from the reporting data product. |
| `POST /api/reports/compare` | `RunComparisonResult` | Gated result; blocked comparisons do not return deltas. |

## Readiness

Readiness uses explicit sectioned status, not a single ready boolean:

- `dataset_status`: `ready`, `partial`, `blocked`, or `failed`.
- `results_status`: `ready`, `partial`, `blocked`, or `failed`.
- `comparison_status`: `ready`, `ready_with_caveats`, or `blocked`.
- `export_status`: `available`, `partial`, or `unavailable`.
- `data_quality_status`: `clean`, `degraded`, `blocked`, or `unknown`.
- `execution_quality_status`: `clean`, `degraded`, `blocked`, or `unknown`.
- `golden_candidate_status`: `certified`, `blocked`, `failed`, or `unknown`.
- `golden_blocking_reasons`: deterministic reasons a reproducible golden run
  cannot be certified.
- `repeatability_status`: material identity and fingerprint readiness.
- `material_fingerprint`: stable fingerprint over the material report data
  product when strategy, config, data, execution, diagnostics, candle continuity,
  compact indicator/market-state context, and section availability evidence are
  present.

The legacy booleans remain as summary flags for existing clients, but the
status fields are the reporting contract. Candle gaps, intrabar fallback,
missing indicator/world-state context, unavailable lifecycle rows, and
unsupported metrics must appear as caveats, degraded sections, unavailable
sections, or diagnostics.

`safe_to_compare=true` only means the standard comparison surface can return
results with its declared caveats. It does not certify a golden run. Golden
certification is stricter and must block on unclassified `RUN_FAILED` or
`FAULT_RECORDED` events, unresolved projection failures, notification queue
overflow, unknown or degraded candle continuity, wallet/accounting evidence
gaps, timestamp semantic confusion, and missing material identity.

## Diagnostics

`ReportDiagnostics` is a first-class section inside `RunResearchDataset`.

Diagnostics normalize:

- readiness blockers,
- export readiness,
- candle continuity and gap facts,
- intrabar fallback warnings,
- BotLens projection failures,
- lifecycle anomalies,
- persistence/query caveats,
- unsupported or incomplete metric sections,
- comparison blockers.

Each diagnostic contains:

- severity,
- source,
- code,
- message,
- affected identity,
- timestamp or known-at time when available,
- readiness impact,
- suggested next step when obvious.

BotLens projection failures are normalized as diagnostics. A stale BotLens
projection cannot by itself block normal report results, export, or standard
comparison unless the canonical reporting dataset also sees a matching
durable-truth defect. Golden certification is stricter: unresolved projection
failures block the golden candidate until durable ledger truth and projection
state are reconciled.

## Metrics

`RunResearchDataset` contains a first-class `portfolio_metrics` section for
canonical computed metrics such as Sharpe, Sortino, Calmar, annualized
volatility, drawdown duration, exposure, and daily best/worst PnL. These values
are computed from closed-trade net PnL, starting capital, and the simulated run
window.

Consumers should not have to recompute these metrics to render standard report
views. Raw trade rows remain available so external analysis tools can audit or
recompute metrics independently.

## Timeseries

The dataset exposes canonical timeseries sections under `timeseries.items`.
These are not frontend-shaped chart blobs. Rows carry run identity, time,
optional instrument/symbol/timeframe identity, value fields, and source fields.

Initial sections are:

- equity curve,
- drawdown curve,
- returns series,
- capital timeline,
- exposure timeline,
- positions timeline,
- rolling win rate,
- rolling expectancy,
- rolling profit factor,
- rolling Sharpe,
- rolling volatility,
- rolling drawdown.

Unavailable timeseries are returned as explicit unavailable sections with a
reason instead of being silently omitted.

## Context And World State

The `context` section exposes deterministic context datasets where durable
runtime context exists:

- `indicator_snapshots`,
- `decision_context`,
- `trade_context`,
- `market_state`.

Indicator and market-state rows come from compact typed-output snapshots captured
at the decision boundary. Runtime signal facts preserve the selected decision
artifact, including `referenced_outputs`, so reporting can expose the known-at
signal/context/metric evidence without replaying hidden indicator internals or
dumping arbitrary raw blobs. Missing runtime capture is reported as a section
caveat and diagnostic.

## Candles

The main dataset includes a `candle_catalog` only. Full candles are not included
in the default report payload.

Consumers can request bounded windows through reporting candle routes. The
routes expose instrument/timeframe/time-window rows from the reporting candle
service and do not expose candle-store internals.

Catalog rows are scoped to one instrument/timeframe/provider/source identity.
Reporting derives symbol/instrument pairs from trace identities first and only
falls back to paired run metadata when trace identity is absent. It must never
cross-join metadata symbol and instrument arrays. Counts, missing-value counts,
duplicate counts, gap counts, available resolutions, and continuity status come
from candle storage facts when storage is available.

## Comparison Gate

Comparison must not return deltas until every requested run is safe to compare
and compatibility checks pass.

Required checks:

- run finalization,
- dataset/report readiness,
- strategy identity,
- strategy hash or material config hash where available,
- symbol/instrument and timeframe compatibility,
- dataset schema version,
- comparable metrics,
- data and execution quality blockers,
- diagnostics that block comparison.

When blocked, `RunComparisonResult` returns `status=blocked`,
`blocked_reasons`, `readiness`, and `compatibility`. The `comparisons` array is
empty.

## Export

Export is generated from `RunResearchDataset`. The zip includes:

- `manifest.json`,
- metadata,
- readiness,
- summary,
- diagnostics,
- trades,
- decisions,
- signals,
- metrics and accounting sections.
- timeseries sections,
- candle catalog,
- context/world-state sections,
- operational health.

CSV files are included for tabular sections. JSON files are included for the
sectioned structured contract. The export intentionally does not include a
single full `run_research_dataset.json` by default because table sections can
grow large over time. Consumers should reconstruct the dataset from the manifest
and section files when they need an offline bundle.

Full candle files are optional. Standard exports omit full candles. Research
exports can pass `include_candles=true`, which adds per-series candle files and
manifest entries when candle identity and storage access are available.

The manifest lists unavailable sections explicitly and describes each included
file with section, format, row count when applicable, byte size, and SHA-256
hash. `manifest.json` is listed without a self-hash to avoid circular metadata.

## Frontend Flow

The reporting UI loads the contract in staged sections rather than issuing every
report request on modal open:

1. Catalog list.
2. Readiness, then summary, sections, and diagnostics for the opened run.
3. Trade pages only when the trade tab is opened.
4. Decision and signal pages only when the decision trace tab is opened.
5. Timeseries, context, candle catalog, and operational health only when those
   tabs are opened.
6. Export manifest only when the export tab is opened.
7. Export zip only when requested.
8. Comparison only after readiness allows it.

The UI does not depend on old frontend-shaped report payloads. It may use the
complete `RunResearchDataset` route for full external/debug reads, but the
interactive report view should prefer sectioned contract reads so large tables
and export metadata do not block the first useful render.

## Request Isolation

Report dataset assembly is synchronous and can be expensive for runs with many
events. API routes run that work in the FastAPI threadpool so one report build
does not block unrelated event-loop requests.

The report contract service also keeps a short-lived per-process
`RunResearchDataset` cache with in-flight request coalescing. This is not a
durable materialization layer; it only absorbs bursts where clients request
readiness, summary, sections, diagnostics, table pages, manifest, or full
dataset for the same run in quick succession. The canonical contract and route
shape remain unchanged.
