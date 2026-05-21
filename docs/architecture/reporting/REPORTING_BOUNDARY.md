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
  - portal/backend/service/reports/comparison.py
  - portal/backend/service/reports/golden_evidence.py
  - portal/backend/service/storage/repos/report_materializations.py
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

`RunReportDTO v2` is the typed single-run report contract for frontend and
future MCP consumers. It wraps canonical dataset facts into research-trust and
research-performance sections while keeping raw rows available only as
referenced/debug data.

Terminal report artifacts are materialized separately from run lifecycle truth.
When a run reaches a terminal status, the backend may enqueue a
`RunReportDTO v2` build and persist the artifact/status in
`portal_report_materializations_v1`. Report states (`not_started`, `building`,
`ready`, `failed`, `stale`) do not alter run terminal status; report build
failure is a reporting condition, not a runtime failure.

Paired run-report comparison reads ready `RunReportDTO v2` artifacts from
`portal_report_materializations_v1`. It returns structured blockers for
non-terminal, missing, building, failed, or stale report artifacts and does not
enqueue cold report builds by default. Semantic, performance, behavior, wallet,
symbol, coordinator-wait, and operational drift deltas are derived from the
materialized report contracts. When an existing golden repeatability
`comparison_summary*.json` artifact is available for the pair, comparison may
include it as read-only evidence for decision equality, verdict changes, trade
lifecycle equality, wallet/order checks, runtime ordering, and first semantic
divergence. The comparison API must not generate golden artifacts unless a
future explicit build path is requested.

CLI/agent consumers use compact projections for orchestration:
`run_research_summary.v1` for single-run summary, an explicit
`run-report/build` route for materialization status without returning the full
artifact, and `run_report_comparison_summary.v1` for pairwise comparison. These
projections are derived from the same dataset and materialized report truth;
they are not alternate report semantics.

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

Run configuration metadata preserves strategy variant provenance when available.
`run_strategy_snapshot` records the exact effective strategy configuration at run
start, including `effective_params`, `output_filters`, `base_params`, and
`param_source_map`. Reports expose this as provenance only. Reporting must not
re-resolve variants from mutable strategy storage or let provenance enrichment
change evaluator or execution behavior.

Decision-boundary indicator and market-state context comes from compact typed
runtime output snapshots embedded in selected decision artifacts. `observed_outputs`
captures the current signal, context, and metric outputs from the same indicator
frame; `referenced_outputs` captures the narrower rule lineage. Reporting
extracts indicator snapshots and market-state rows from those durable snapshots
and must not replay hidden indicator state or read mutable engine internals.
When a strategy variant materializes output filters into rule guards, selected
decision artifacts may also expose compact `output_filter_trace` records. These
records are research provenance for the already-evaluated guard results; they
must not be used to re-evaluate or override strategy decisions inside reporting.

Computed portfolio metrics are part of reporting truth. Standard values such as
Sharpe, Sortino, Calmar, annualized volatility, drawdown duration, and exposure
are derived by the reporting layer from closed trades and the simulated run
window, with raw trades retained for audit and independent recompute.

Trade rows may include report-only lifecycle enrichment such as entry stop
distance, entry R, persisted runtime MAE/MFE, bounded candle-derived excursion,
per-leg excursion, and intrabar fallback flags. These fields are downstream
research evidence only. They must not mutate order fills, wallet accounting,
fee/slippage semantics, stop/target behavior, or trade lifecycle truth. When
bounded candle evidence is missing or truncated, reporting marks the enrichment
unavailable or caveated instead of inferring hidden intrabar state.

Signal rows may expose `indicator_context` extracted from the typed runtime
outputs embedded in the selected decision artifact. Indicator-owned signal event
metadata, such as breakout timing, confirmation counters, value-area references,
and distance-from-reference values, remains part of indicator output context;
reporting must not add strategy-specific signal fields that reinterpret an
indicator's private state.

Display-facing metrics must use `MetricValueDTO` validity metadata. Consumers
must render `invalid`, `not_available`, or `not_computed` states instead of
inventing values or silently treating missing ratios as zero.

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

Material report and golden identity run in strict canonical-input mode. Candle
continuity evidence is material only when it is terminal `run_final` continuity
evidence. Observer/debug facts such as `selected_symbol_snapshot`,
`run_bootstrap_selected_symbol`, `message_kind=ephemeral`, viewer-triggered
continuity rows, and non-terminal bootstrap snapshots stay operational
diagnostics. They must not feed `data_snapshot_hash`, semantic fingerprints,
golden certification, or research-valid status. If terminal `run_final`
continuity evidence is absent, reporting blocks certification with
`missing_canonical_continuity_evidence` instead of silently certifying from
observer facts.

## Outputs

- report API payloads,
- compare payloads,
- compact research summary and comparison-summary payloads for CLI/agent
  workflows,
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
- Legacy dataset compare uses canonical dataset readiness, not ad hoc
  report-file existence. Materialized run-report compare additionally requires
  both `RunReportDTO v2` artifacts to be `ready` so the comparison UI can use
  the same artifact truth source as single-run reports.
- Standard computed metrics are exposed by the dataset; consumers should not need
  private formula implementations for normal report views.
- Narrative summaries are bounded views over dataset facts.
- Export bundles are generated from the reporting data product, not the source of comparison truth.
- Candle catalog rows are scoped to one instrument/timeframe/provider/source
  identity. Reporting must not cross-join symbols and instruments from metadata
  arrays.
- Candle catalog counts, gaps, missing values, and continuity status come from
  candle storage facts when storage is available; terminal `run_final` compact
  continuity summaries supply classification and first-gap evidence. If a
  compact continuity summary lost provider classification, reporting may
  reclassify unknown gaps from `portal_candle_closures` evidence for the same
  instrument/timeframe/window. Unknown continuity is a data quality caveat, and
  unclassified/runtime/projection/ingestion gaps block golden readiness.
- Headless research runs must emit canonical `run_final` continuity evidence
  without requiring BotLens to be opened.
- Strategy rows in report config snapshots must preserve the run-start
  `run_strategy_snapshot`/`effective_strategy_config` when provided by runtime
  series metadata. Worker aggregation must not replace known rules, params, ATM,
  or variant provenance with empty placeholders.

## Related Docs

- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [ADR 0015: Split semantic and operational golden fingerprints](../decisions/0015-split-semantic-and-operational-golden-fingerprints.md)
- [ADR 0016: Treat runtime event ledger order as operational evidence](../decisions/0016-treat-runtime-event-ledger-order-as-operational-evidence.md)

## Known Gaps

- Indicator/world-state context depends on structured runtime capture. When it
  is absent from decision artifact `observed_outputs`/`referenced_outputs`,
  reports expose explicit unavailable sections rather than replaying hidden
  indicator state.
- Candle windows require instrument identity and candle storage coverage. The
  catalog reports unavailable sections when either is missing.
