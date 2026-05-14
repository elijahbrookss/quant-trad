---
component: reporting-boundary
subsystem: reporting
layer: boundary
doc_type: architecture
status: active
tags:
  - reporting
  - research-dataset
  - compare
  - diagnostics
  - export
code_paths:
  - portal/backend/service/reports
  - portal/backend/controller/reports.py
  - portal/backend/service/storage/repos/candles.py
  - portal/backend/service/storage/repos/runtime_events.py
  - docs/architecture/reporting/diagrams/run-research-dataset-flow.mmd
---
# Reporting Boundary

## Purpose

The reporting boundary turns durable run truth into research, comparison, export, diagnostics, and analysis views.

Related diagram: [run-research-dataset-flow.mmd](diagrams/run-research-dataset-flow.mmd).

## Boundary Contract

Reports are views. `RunResearchDataset v1` is the canonical run-level data product. Export bundles are generated from the dataset and are not comparison truth.

Reporting does not mutate strategy, execution, fee, wallet, trade, or BotLens semantics.

## Diagram Walkthrough

[run-research-dataset-flow.mmd](diagrams/run-research-dataset-flow.mmd) shows:

1. Run, trade, runtime-event, and step rows are read from durable storage.
2. `RunResearchDataset v1` normalizes metadata, readiness, summary metrics, timeseries, decisions, signals, trades, context/world-state rows, candle catalog, diagnostics, candle gaps, runtime performance, operational health, and insights.
3. Reports, compare views, exports, and external analysis tools read from the dataset.

## Dataset Truth

The dataset is rebuildable from durable DB/read-model truth:

- `portal_bot_runs` for metadata and config snapshots,
- `portal_bot_trades` and trade events for trade lifecycle and financial outcomes,
- `portal_bot_run_events` for decisions, execution diagnostics, wallet/fallback facts, and BotLens-domain facts,
- `portal_bot_run_step_rollups_v1` for profiler timings and mergeable p95/p99
  histogram estimates when present,
- observability events for normalized report diagnostics.
- the reporting candle service for bounded candle windows when requested.

Decision-boundary indicator and market-state context comes from compact typed
runtime output snapshots embedded in selected decision artifacts. `observed_outputs`
captures the current signal, context, and metric outputs from the same indicator
frame; `referenced_outputs` captures the narrower rule lineage. Reporting
extracts indicator snapshots and market-state rows from those durable snapshots
and must not replay hidden indicator state or read mutable engine internals.

Computed portfolio metrics are part of reporting truth. Standard values such as
Sharpe, Sortino, Calmar, annualized volatility, drawdown duration, and exposure
are derived by the reporting layer from closed trades and the simulated run
window, with raw trades retained for audit and independent recompute.

## Readiness Vocabulary

- `dataset_status`: `ready`, `partial`, `blocked`, or `failed`.
- `results_status`: `ready`, `partial`, `blocked`, or `failed`.
- `comparison_status`: `ready`, `ready_with_caveats`, or `blocked`.
- `export_status`: `available`, `partial`, or `unavailable`.
- `data_quality_status`: `clean`, `degraded`, `blocked`, or `unknown`.
- `execution_quality_status`: `clean`, `degraded`, `blocked`, or `unknown`.
- `golden_candidate_status`: `certified`, `blocked`, `failed`, or `unknown`.
- `golden_blocking_reasons`: deterministic reasons a run cannot be used as a
  golden run.
- `repeatability_status`: status of material identity and fingerprint evidence.
- `semantic_fingerprint`: stable trading-behavior fingerprint when required
  material identity fields are available. It covers deterministic strategy/data
  identity, summary metrics, logical decision/signal order, trade lifecycle, and
  compact decision-boundary indicator/market-state context while excluding
  run-instance identifiers such as generated signal or trade IDs.
- `operational_fingerprint`: runtime evidence fingerprint for diagnostics,
  section availability, candle continuity evidence, and run-instance/runtime
  identifiers. Differences here are useful audit evidence but do not by
  themselves prove trading-behavior divergence.
- `material_fingerprint`: compatibility alias for `semantic_fingerprint`.

Boolean fields such as `dataset_ready`, `results_ready`, and `safe_to_compare`
are compatibility summaries. Consumers should prefer the status fields for new
workflows.

`safe_to_compare=true` does not certify a golden run. Golden certification is a
stricter reporting surface for reproducible run validation and must block on
unclassified lifecycle failures, unresolved projection failures, queue
overflow, unavailable wallet/accounting evidence, incomplete wallet decision
trace evidence, unknown/runtime/projection/ingestion candle gaps, and missing
material identity. Provider-backed sparse candle evidence is reported as
degraded data quality, but it is not treated as pipeline loss when the gap has
provider-agnostic evidence such as a closure, empty provider response, or
provider response metadata.

Reporting must expose runtime ordering health for canonical events. Missing,
duplicate, gapped, non-monotonic, or mixed `run_seq` ordering blocks golden-run
certification. Backfilled ordering may be usable for legacy inspection only when
it is explicitly caveated and not treated as runtime-assigned repeatability
evidence.

Position/trade ordering diagnostics are scoped differently from the run ledger.
Missing, invalid-status, duplicate, or conflicting `position_commit_seq` values
block certification because trade lifecycle replay cannot prove causal order.
Sparse position-clock gaps and run-sequence interleaving are informational when
each trade's own `position_commit_seq` chain is valid; they should be reported
for audit but must not be treated as proof of lifecycle contradiction.

Projection diagnostics are not execution truth, but unresolved projection
failures are operational blockers. Reporting should emit
`projection_truth_mismatch` when BotLens reports open trades that durable trade
events show as closed. Queue overflow and projector failure remain
golden-blocking until a later reconciliation/replay event proves the projection
was rebuilt from canonical runtime events.

## Outputs

- report API payloads,
- compare payloads,
- downloadable/export bundles,
- normalized diagnostics,
- readiness and caveat explanations.
- optional research exports with candle files.

## Failure And Recovery

- Missing durable run/trade/event truth blocks readiness.
- Terminal open trades block safe comparison unless explicitly modeled.
- Reports should explain which section is missing instead of returning optimistic partial truth.
- A completed run with unclassified `RUN_FAILED` or `FAULT_RECORDED` facts blocks
  golden-run certification. Recoverable watchdog stale-heartbeat facts are
  reported as degraded lifecycle health instead of lifecycle contradiction.
  Recoverable startup container-ownership ambiguity is treated the same way
  when it is explicitly classified and later runtime truth proves the run
  continued.
- Accepted decisions and wallet/margin rejections without decision-time wallet
  snapshots block golden-run certification because margin verdicts cannot be
  replayed or explained.

## Invariants

- Reporting is downstream of runtime truth.
- Compare uses canonical dataset readiness, not ad hoc report-file existence.
- Standard computed metrics are exposed by the dataset; consumers should not need
  private formula implementations for normal report views.
- Narrative summaries are bounded views over dataset facts.
- Export bundles are generated from the reporting data product, not the source of comparison truth.
- Candle catalog rows are scoped to one instrument/timeframe/provider/source
  identity. Reporting must not cross-join symbols and instruments from metadata
  arrays.
- Candle catalog counts, gaps, missing values, and continuity status come from
  candle storage facts when storage is available; compact continuity summaries
  supply classification and first-gap evidence. If a compact continuity summary
  lost provider classification, reporting may reclassify unknown gaps from
  `portal_candle_closures` evidence for the same instrument/timeframe/window.
  Unknown continuity is a data quality caveat, and unclassified/runtime/
  projection/ingestion gaps block golden readiness.

## Related Docs

- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)

## Known Gaps

- Indicator/world-state context depends on structured runtime capture. When it
  is absent from decision artifact `observed_outputs`/`referenced_outputs`,
  reports expose explicit unavailable sections rather than replaying hidden
  indicator state.
- Candle windows require instrument identity and candle storage coverage. The
  catalog reports unavailable sections when either is missing.
