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
  - portal/backend/service/reports/comparison.py
  - portal/backend/service/reports/golden_evidence.py
  - portal/backend/service/reports/run_research_dataset.py
  - portal/backend/service/reports/export_bundle.py
  - portal/backend/service/reports/schemas.py
  - portal/backend/service/reports/materialization.py
  - portal/backend/service/storage/repos/report_materializations.py
  - portal/backend/service/storage/repos/candles.py
  - portal/frontend/src/components/reports
  - scripts/reporting/golden_repeatability.py
---

# Reporting Contract Redesign

## Purpose

The reporting layer exposes a user-agnostic, deterministic reporting data
product for frontend views, downloads, CLI/debug workflows, and external
analysis tools.

`RunResearchDataset v1` is the canonical report payload. Frontend-shaped report
payloads are not part of the contract.

`RunReportDTO v2` is the typed frontend/MCP-safe view over the canonical
dataset. It groups deterministic facts into research trust, performance,
behavior, wallet, symbol breakdown, coordinator-wait, and operational
diagnostic sections without making raw internals the primary UX contract.

## Contract Shape

The public API exposes typed, versioned report surfaces:

- `ReportReadiness`
- `RunResearchDataset`
- `RunReportDTO`
- `RunComparisonDTO`
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
| `GET /api/reports/{run_id}/run-report` | `RunReportDTO` or materialization status | Typed research cockpit contract over the canonical dataset. Terminal runs return a ready materialized artifact when available, enqueue materialization and return `202` when building, and reject active runs. |
| `GET /api/reports/{run_id}/run-report/status` | `ReportMaterializationStatusDTO` | Materialized report artifact lifecycle (`not_started`, `building`, `ready`, `failed`, `stale`) for UI actions and automation. |
| `GET /api/reports/compare?left_run_id=...&right_run_id=...` | `RunComparisonDTO` | Frontend comparison contract over ready materialized `RunReportDTO v2` artifacts. It returns structured blocked states when either artifact is unavailable, reads existing golden evidence when available, and does not enqueue cold report or golden builds by default. |
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
| `POST /api/reports/compare` | `RunComparisonResult` | Legacy dataset comparison result; blocked comparisons do not return deltas. |

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
- `semantic_fingerprint`: stable trading-behavior fingerprint over strategy,
  config, data identity, logical decisions/signals, trade lifecycle, summary
  metrics, and compact decision-boundary context.
- `operational_fingerprint`: runtime evidence fingerprint over diagnostics,
  section availability, candle continuity evidence, generated IDs, and other
  operational traces.
- `material_fingerprint`: compatibility alias for `semantic_fingerprint`.

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

`RunReportDTO v2` wraps display-facing metrics in `MetricValueDTO` so ratios and
caveated values are never exposed as naked numbers. Each metric carries value,
validity, unit, method, source, method metadata, sample counts, minimum sample
count, invalid reason, and caveats. Missing or unsupported fields such as
slippage, unrealized PnL, and margin time-series usage are returned as invalid
or `not_available` with an explicit reason rather than inferred by the
frontend.

Slippage remains unavailable unless execution facts or an explicit zero-slippage
configuration make the value explainable. Fee accounting should aggregate role,
rate, source, and sanity-check evidence from durable trade metrics and runtime
fill/fee events when those facts exist.

## Materialized Run Comparison

`RunComparisonDTO` compares exactly two ready terminal report artifacts from
`portal_report_materializations_v1`. The comparison endpoint checks
materialization status first and returns a blocked contract for
`run_not_terminal`, `left_report_not_ready`, `right_report_not_ready`,
`left_report_building`, `right_report_building`, `left_report_failed`, or
`right_report_failed`. It must not call report materialization builders unless a
future explicit rebuild parameter is added.

Golden evidence is integrated through a read-only artifact adapter. The adapter
scans existing golden repeatability `comparison_summary*.json` artifacts,
selects the latest artifact whose `run_ids` match the requested pair, and
normalizes stable script output into `GoldenEvidenceDTO`. Normal comparison
does not invoke `scripts/reporting/golden_repeatability.py`. `include_golden`
controls artifact reads, and `require_golden` can block when no existing
artifact is available.

Golden repeatability artifacts preserve legacy decision summary fields and the
full decision-difference evidence needed by downstream comparison consumers:
`missing_decision_ids`, `extra_decision_ids`, and compact `verdict_changes`
rows. Readers must prefer the full arrays when present and fall back to legacy
first-example fields for older artifacts without changing PASS/FAIL semantics.

The DTO separates semantic comparison from operational drift:

- trust comparison carries lifecycle, readiness, golden, semantic fingerprint,
  operational fingerprint, data snapshot hash, config/strategy hash, runtime
  ordering, wallet trace, candle continuity, and observer safety fields;
- performance deltas compare `MetricValueDTO` fields and preserve invalid or
  not-comparable states instead of subtracting naked ratios;
- behavior, wallet, symbol, and coordinator wait deltas are report-level views
  over the two materialized artifacts;
- operational drift is diagnostic-only when semantic fingerprints match;
- first divergence is sourced from report comparison when fingerprints differ,
  and from golden evidence when an existing artifact provides decision/trade or
  material divergence details. If golden evidence confirms no semantic
  divergence, first divergence is explicitly reported as absent even when
  operational diagnostics differ.

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

Signal datasets may include a direct `indicator_context` view over those typed
outputs. Indicator-coupled metadata, including value-area references, breakout
time, confirmation bars, and distance from the referenced level, belongs inside
the indicator output payload rather than as generic strategy signal columns.

Trade datasets may include report-only research enrichment for stop distance and
R at entry, persisted runtime MAE/MFE, bounded candle-derived MAE/MFE before
trade or leg exit, intrabar fallback flags on trades and legs, and entry/exit
market-state lookup status. Exit market state is exact only when a captured
world-state row exists at the exit time; otherwise reporting may expose the
latest captured state before exit with explicit staleness and caveats.

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
`RunResearchDataset` cache with in-flight request coalescing. This complements
the terminal `RunReportDTO v2` materialization table; the cache only absorbs
bursts where clients request
readiness, summary, sections, diagnostics, table pages, manifest, or full
dataset for the same run in quick succession. The canonical contract and route
shape remain unchanged.
