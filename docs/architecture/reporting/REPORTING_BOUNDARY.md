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
  - portal/backend/service/provenance.py
  - portal/backend/service/reports
  - portal/backend/service/reports/candle_continuity.py
  - portal/backend/controller/reports.py
  - portal/backend/controller/research.py
  - portal/frontend/src/adapters/report.adapter.js
  - portal/frontend/src/components/reports/reportComparisonViewModel.js
  - portal/backend/service/reports/comparison.py
  - portal/backend/service/reports/golden_evidence.py
  - portal/backend/service/storage/repos/report_materializations.py
  - portal/backend/service/storage/repos/candles.py
  - portal/backend/service/storage/repos/runtime_events.py
  - src/engines/bot_runtime/core/wallet.py
  - portal/backend/service/bots/botlens_intake_router.py
  - config/defaults.yaml
  - src/core/settings.py
  - docs/architecture/reporting/diagrams/run-research-dataset-flow.mmd
---
# Reporting Boundary

## Purpose

The reporting boundary turns durable run truth into research, comparison, export, diagnostics, and analysis views.

Related diagram: [run-research-dataset-flow.mmd](diagrams/run-research-dataset-flow.mmd).

## Boundary Contract

Reports are views. `RunResearchDataset v1` is the canonical run-level data product. Export bundles are generated from the dataset and are not comparison truth.

`RunReportDTO` with `contract_version=run_report.v2` is the typed single-run report contract for frontend and
future MCP consumers. It wraps canonical dataset facts into research-trust and
research-performance sections while keeping raw rows available only as
referenced/debug data.

Terminal report artifacts are materialized separately from run lifecycle truth.
When a run reaches a terminal status, the backend enqueues a
`RunReportDTO` build that persists the artifact/status in
`portal_report_materializations`. Report states (`not_started`, `building`,
`ready`, `failed`, `stale`) do not alter run terminal status; report build
failure is a reporting condition, not a runtime failure.

The default enqueue is asynchronous and delayed one second after terminal
lifecycle intake. The delay lets immediately preceding bounded projection
intake settle without putting report work on the execution critical path. A
manual `POST /run-report/build` remains the retry/explicit-build boundary. The
build's durable input fingerprint is authoritative: late durable input makes an
artifact stale and forces a rebuild rather than allowing an old report to look
ready.

Materialized report artifacts are valid only for the exact durable input
fingerprint recorded with the artifact. The fingerprint includes the run row,
runtime-event high-water mark, and trade count/update boundary. A ready artifact
with a missing or changed input fingerprint is stale and must be rebuilt before
serving or comparing. Contract/schema version alone is not a cache validity
boundary. Report materializations also record the builder source revision; a
ready artifact built by another source revision is stale for the current backend
and must be rebuilt before serving or comparing.

Interactive report pages share one bounded in-process `RunResearchDataset`
reconstruction cache. Active runs, alternate builders, and sources whose durable
fingerprint cannot be proven receive only the 15-second request-burst cache.
Canonical terminal-run datasets may remain in an eight-entry LRU for at most 15
minutes only when the run/event/trade input fingerprint is identical before and
after reconstruction. After the burst window, every reuse rechecks that durable
fingerprint. A mismatch or unavailable proof evicts the entry and rebuilds from
durable truth; this cache is never persisted, never crosses backend processes,
and never becomes report or comparison truth.

Cold reconstruction is synchronous internally because it reads one coherent
durable input boundary, but HTTP controllers must not execute it on the ASGI
event-loop thread. Controllers either dispatch reconstruction to the bounded
threadpool or use the existing asynchronous materialization boundary. This
isolation preserves portal, BotLens, health, and stream responsiveness; it does
not make reconstruction itself faster or change its canonical output.

Cold reconstruction reads each run-scoped source set once and reuses the loaded
run, runtime events, projected decision ledger, and stored trades for readiness
evaluation. Trade excursion evidence is grouped by instrument/timeframe and
read through bounded multi-window candle queries. Each trade window retains its
own exclusive end, latest-revision selection, row limit, fallback timeframe,
and partial/unavailable result; batching changes round trips, not evidence.
Wallet state validation advances the canonical wallet reducer once per ordered
event and compares the same before/after snapshots that growing-prefix replay
previously produced. Full replay and validation must share that reducer.

These optimizations are admissible only when representative cold builds retain
the exact semantic and operational fingerprints and the same diagnostics. They
do not permit using mutable overlay state, skipping malformed wallet events, or
turning truncated candle evidence into complete excursion evidence.

Within one backend process, concurrent requests for the same reconstruction
key share one single-flight owner. Followers wait for that owner's result and
must log their wait duration rather than launching duplicate builds. The cache
owner's completion log records total duration, durable-fingerprint checks, and
the canonical dataset-build duration. The dataset builder's completion log
records total duration and phase durations for source loading, trade
enrichment, readiness, wallet accounting, observability, remaining assembly,
and serialization on the same structured log event. These timings are
operational evidence only and cannot enter dataset identity.

Paired run-report comparison reads ready `RunReportDTO` artifacts from
`portal_report_materializations`. It returns structured blockers for
non-terminal, missing, building, failed, or stale report artifacts and does not
enqueue cold report builds by default. Semantic, performance, behavior, wallet,
symbol, coordinator-wait, and operational drift deltas are derived from the
materialized report contracts. When an existing golden repeatability
`comparison_summary*.json` artifact is available for the pair, comparison may
include it as read-only evidence for decision equality, verdict changes, trade
lifecycle equality, wallet/order checks, runtime ordering, and first semantic
divergence. The comparison API must not generate golden artifacts unless a
future explicit build path is requested.

Comparison first publishes `semantic_eligibility` from execution semantics,
dataset identity, strategy identity, and material configuration identity.
Mismatched execution semantics make repeatability equivalence incompatible even
when decision and trade counts happen to match. Such runs remain available for
descriptive performance/behavior inspection, but the UI must explain that equal
counts do not prove equal fills, sizing, fees, wallet accounting, or P&L.
Incomplete identity evidence produces `unknown`, not an optimistic eligibility
claim. The first divergence points at `identity.execution_semantics` when that
is the earliest proven incompatibility.

CLI/agent consumers use compact projections for orchestration:
`run_research_summary.v1` for single-run summary, an explicit
`run-report/build` route for materialization status without returning the full
artifact, and `run_report_comparison_summary.v1` for pairwise comparison. These
projections are derived from the same dataset and materialized report truth;
they are not alternate report semantics.

The compact research summary preserves the frozen source `dataset_id` and
`dataset_hash`, exact runtime-consumed data snapshot hash, semantic and
operational fingerprints, repeatability state, data and execution quality
states, blockers, degraded/unavailable sections, and caveats. Persisted
experiment summaries carry that projection unchanged so CLI and MCP research
workflows cannot mistake missing or degraded evidence for a complete run.

Reporting does not mutate strategy, execution, fee, wallet, trade, or BotLens semantics.

## Diagram Walkthrough

[run-research-dataset-flow.mmd](diagrams/run-research-dataset-flow.mmd) shows:

1. Run, trade, runtime-event, and step rows are read from durable storage.
2. `RunResearchDataset v1` normalizes metadata, readiness, summary metrics, timeseries, decisions, signals, trades, context/world-state rows, candidate lifecycle evidence, candle catalog, diagnostics, candle gaps, runtime performance, operational health, and insights.
3. Reports, compare views, exports, and external analysis tools read from the dataset.

## Dataset Rebuilds From Durable Run Facts

The dataset is rebuildable from durable DB/read-model truth:

- `portal_bot_runs` for the rebuildable current-run lifecycle summary projection,
  metadata, lean provenance hashes, and bounded config snapshots,
- `portal_bot_trades` and trade events for trade lifecycle and financial outcomes,
- `portal_bot_run_events` for decisions, execution diagnostics, wallet/fallback facts, and BotLens-domain facts,
- `portal_bot_run_step_rollups` for phase-duration profiler timings and
  mergeable p95/p99 histogram estimates when present,
- observability events for normalized report diagnostics.
- the reporting candle service for bounded candle windows when requested.

Run-scoped chart history remains a separate bounded BotLens concern. Historical
candles, trades, and overlay deltas are requested for the visible range and are
not regenerated through the indicator engine during report reconstruction.
Candidate-lifecycle report rows filter finalized indicator-output artifacts;
they do not require loading or replaying every visual overlay.

Run configuration metadata preserves strategy variant provenance when available
without embedding the full raw run snapshot in report artifacts.
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

Maximum drawdown uses every ordered closed-trade equity transition. Daily-close
aggregation remains appropriate for daily return ratios and drawdown duration,
but it cannot replace intraday trade-path peaks and troughs in the canonical
maximum-drawdown metric. Missing fee-role, fee-rate, or slippage facts remain
explicit report and trust caveats even when the deterministic simulator applies
zero cost.

Trade rows may include report-only lifecycle enrichment such as entry stop
distance, entry R, persisted runtime MAE/MFE, bounded candle-derived excursion,
per-leg excursion, and intrabar fallback flags. These fields are downstream
research evidence only. They must not mutate order fills, wallet accounting,
fee/slippage semantics, stop/target behavior, or trade lifecycle truth. When
bounded candle evidence is missing or truncated, reporting marks the enrichment
unavailable or caveated instead of inferring hidden intrabar state.

Instrument semantics prefer the persisted runtime-readiness execution profile.
When older or reduced run snapshots omit accounting semantics, canonical
`ENTRY_FILLED` and `EXIT_FILLED` evidence may complete the matching report row.
Spot fill accounting proves spot execution semantics; margin accounting alone
does not distinguish derivative from proxy-derivative execution. Conflicting
configured and fill evidence fails report construction instead of choosing a
convenient value. Untyped `execution_semantics` fields on fill payloads are not
authority and cannot alter report identity.

Signal rows may expose `indicator_context` extracted from the typed runtime
outputs embedded in the selected decision artifact. Indicator-owned signal event
metadata, such as breakout timing, confirmation counters, value-area references,
and distance-from-reference values, remains part of indicator output context;
reporting must not add strategy-specific signal fields that reinterpret an
indicator's private state.

Candidate lifecycle evidence comes from finalized report artifact indicator
output rows whose output type is `lifecycle`. Reporting flattens stage-change
events into a `candidate_lifecycle` dataset section and summarizes candidate
funnels, terminal outcomes, reasons, and family/side buckets. This is report and
research evidence only. It must not make lifecycle outputs strategy-visible,
rerun indicator logic, inspect indicator internals, or reinterpret candidate
meaning outside the generic lifecycle event contract.

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

Boolean fields such as `dataset_ready`, `results_ready`, and `safe_to_compare`
are derived decision conveniences. The status fields remain the canonical
readiness contract.

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

`data_snapshot_hash` identifies exact candle values consumed by every expected
runtime strategy/instrument/timeframe series. Reporting aggregates only the
runtime-produced `candle_series_snapshot.v1` evidence carried by canonical
terminal facts. It does not re-fetch candles or derive material identity from
catalog counts, successful traces, or gap metadata. The backend preflight
inventory is persisted in run configuration evidence and preserved across
worker aggregation independently of worker success. It must equal the terminal
snapshot inventory exactly. Missing snapshot coverage makes the hash
unavailable and readiness exposes `missing_data_snapshot_hash`. Continuity,
provenance, warmup, confidence, and caveats continue through the existing
quality/readiness contract independently of the value hash.

Independent indicator-source continuity uses the existing
`indicator_source_candle_continuity.v1` payload. Runtime series snapshots and
both standalone and worker-aggregated config snapshots persist the payload with
canonical strategy, instrument, symbol, timeframe, datasource, exchange, and
indicator identity. `RunResearchDataset v1` exposes it as
`context.indicator_source_diagnostics`, includes it in operational identity,
and projects its status into data-quality readiness, caveats, diagnostics, and
golden blocking. It does not enter the semantic fingerprint. An explicitly
captured empty list means not applicable; a missing list is unavailable
evidence. Malformed payloads fail report construction.

## What Reporting Publishes

Reporting publishes report API payloads, compare payloads, compact research and
comparison summaries for CLI/agent workflows, downloadable export bundles,
normalized diagnostics, paged signal/decision/trade/context/candidate-lifecycle
datasets, readiness/caveat explanations, and optional research exports with
candle files. Diagnostics preserve the original full response when no page
limit is requested and return explicit `total`, `limit`, and `offset` when a
bounded operator page is requested. Paging changes delivery only; diagnostic
identity, summary, and ordering remain canonical. These are downstream products
of the dataset, not new execution semantics.

## Failure And Recovery

- Missing durable run/trade/event truth blocks readiness.
- Terminal open trades block safe comparison unless explicitly modeled.
- Reports should explain which section is missing instead of returning optimistic partial truth.
- A completed run with unclassified `RUN_FAILED` or `FAULT_RECORDED` facts blocks
  golden-run certification. Recoverable watchdog expired-lease facts are
  reported as degraded lifecycle health instead of lifecycle contradiction.
  Recoverable startup container-ownership ambiguity is treated the same way
  when it is explicitly classified and later runtime truth proves the run
  continued.
- Accepted decisions and wallet/margin rejections without decision-time wallet
  snapshots block golden-run certification because margin verdicts cannot be
  replayed or explained.

## Invariants

- Reporting is downstream of runtime truth.
- Report instrument accounting semantics cannot contradict canonical fill
  evidence, and missing spot semantics are completed from those fills.
- `GET /run-report` is side-effect free. Materialization starts only through
  `POST /run-report/build`; unsupported GET query parameters fail validation.
- Legacy dataset compare uses canonical dataset readiness, not ad hoc
  report-file existence. Materialized run-report compare additionally requires
  both RunReportDTO contract (`run_report.v2`) artifacts to be `ready` so the comparison UI can use
  the same artifact truth source as single-run reports.
- Repeatability equivalence is eligible only when execution semantics and the
  available dataset, strategy, and material configuration identities match.
  Equal decision/trade counts alone are descriptive evidence, never equivalence
  proof.
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
  reclassify an unknown gap only from canonical `market.gap_evidence` explicitly
  classified `provider_missing_data` for the same instrument, timeframe, and
  window. Generic missing and ingestion-failure evidence cannot reclassify it. Unknown continuity is a data quality caveat, and
  unclassified/runtime/projection/ingestion gaps block golden readiness.
- `reports/candle_continuity.py` owns the pure reporting-time transformation of
  unknown gap rows against already-loaded closure evidence. The dataset builder
  owns evidence loading, caching, and diagnostics and delegates classification
  without reordering gaps or reconstructing candle history.
- Headless research runs must emit canonical `run_final` continuity evidence
  without requiring BotLens to be opened.
- A run-level data snapshot hash is available only when exact runtime-produced
  candle snapshots cover every expected series.
- Observer continuity and diagnostic gap metadata cannot change exact material
  candle identity.
- Indicator source-candle diagnostics preserve their existing payload through
  runtime series metadata, run artifacts, report context, readiness,
  diagnostics, and operational fingerprinting.
- Provider-source caveats degrade quality; source defects marked for
  investigation and missing source-diagnostic evidence block golden
  certification.
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
- [ADR 0043: Canonical accounting reconciliation](../decisions/0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md)
- [ADR 0046: Exact candle inputs and separate quality](../decisions/0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md)
- [ADR 0055: Bounded BotLens hot state and durable inspection](../decisions/0055-separate-bounded-botlens-hot-state-from-durable-inspection.md)

## Known Gaps

- Large report payloads still require full typed-dataset assembly and JSON-safe
  serialization on a true cold build. Terminal materialization and bounded
  request caching remain the serving optimization; reporting must not omit
  canonical sections merely to reduce serialization time.
- Indicator/world-state context depends on structured runtime capture. When it
  is absent from decision artifact `observed_outputs`/`referenced_outputs`,
  reports expose explicit unavailable sections rather than replaying hidden
  indicator state.
- Candle windows require instrument identity and candle storage coverage. The
  catalog reports unavailable sections when either is missing.
