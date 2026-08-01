---
component: adr-dataset-bound-backtests
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - backtest
  - datasets
  - determinism
  - known-at
  - reporting
code_paths:
  - src/market_data/backtest.py
  - portal/backend/service/market/backtest_dataset_service.py
  - portal/backend/service/market/candle_service.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/reports/artifacts.py
  - portal/backend/service/reports/run_research_dataset.py
  - cli/main.py
  - docs/architecture/data/DATA_BOUNDARY.md
  - docs/architecture/reporting/REPORTING_BOUNDARY.md
---
# ADR 0051: Require Frozen Datasets For Canonical Backtests

## Status

Accepted on 2026-07-26.

## Context

A causal commit watermark prevents a running backtest from observing later
revisions, but it does not by itself prove that the requested ranges were
complete, that required warmup was loaded, or that the exact source provenance
and quality evidence were admitted before execution. Allowing startup to fetch
missing data also couples provider throughput and mutable external state to the
execution result.

## Decision

Dataset preparation and backtest execution are separate operations. Preparation
resolves the transitive indicator and runtime requirements, optionally performs
explicit provider acquisition, validates canonical storage coverage, and freezes
one immutable `market_dataset.v1` manifest.

Every canonical backtest start requires a `dataset_id`. Admission creates a
`backtest_dataset_binding.v1` that binds that source dataset to the exact
strategy, indicator graph, execution and exit policy, instrument snapshots,
evaluation range, warmup range, and run-effective configuration. The binding is
persisted in run configuration, inherited by workers and nested reads, included
in semantic report identity, and displayed as dataset ID and hash in compact and
full reports.

Execution reads only the admitted half-open ranges at the frozen commit scope.
It cannot call a provider, select mutable latest state, expand the window, swap
an instrument, or accept a post-freeze correction. Reports independently retain
the exact runtime-consumed candle snapshot as downstream evidence; that snapshot
complements rather than replaces the source dataset manifest.

## Invariants

- A canonical backtest cannot start without a known, admitted dataset ID.
- Preparation may acquire data only when the operator explicitly enables it;
  execution never acquires data.
- Warmup is derived from declared transitive requirements and produces no
  strategy decisions.
- Evaluation, warmup, materialization, and replay windows are half-open and
  cannot expand after admission.
- Every runtime and nested market-data read uses the same frozen commit scope.
- Dataset, strategy, indicator, execution-policy, instrument, and run-config
  substitutions fail loudly.
- Dataset ID and hash survive artifact finalization and participate in report
  semantic identity and golden-readiness evidence.
- Missing dataset identity is reported as unavailable evidence, never inferred
  from a mutable candle query.

## Consequences

Provider ingestion speed and backtest execution speed can be measured and
optimized independently. Repeated runs can reuse one audited dataset without
repeating provider calls, and delayed execution remains bound to the same input
truth. Operators must perform preparation before startup, and old runs without a
frozen binding cannot be promoted as canonical golden evidence.

Paper intake remains an unpinned append-and-consume workflow, and live order
submission remains closed. ADR 0052 adds a durable typed-fact collector and
mixed candle/OI datasets while preserving this preparation/execution split.

## Rejected Alternatives

- Fetch missing candles automatically during backtest startup.
- Treat a requested date range or global database watermark as dataset identity.
- Re-resolve mutable latest instrument or strategy state inside workers.
- Infer dataset identity during report generation from whatever rows are current.
- Use the runtime-derived candle snapshot as a substitute for source provenance,
  quality, and frozen-range admission.

## Enforcing Tests Or Evidence

- `tests/test_market_data/test_backtest_dataset.py` covers plan derivation, binding
  identity, range validation, warmup, and substitution rejection.
- `tests/test_market_data/test_runtime_scope.py` proves scoped reads reject
  unsupported series, ranges, and post-freeze revisions.
- `tests/test_market_data/test_backtest_dataset.py` also covers explicit
  preparation, admission, acquisition defaults, and dataset reuse.
- `tests/test_portal/test_bot_startup_orchestrator.py` and
  `tests/test_portal/test_container_runtime_transport.py` enforce startup and
  worker bindings.
- `tests/test_cli/test_market_data_cli.py`,
  `tests/test_cli/test_experiments_orchestration.py`, and
  `tests/test_cli/test_mcp_server.py` preserve preparation/start separation.
- `tests/test_portal/test_report_artifact_bundle_workers.py`,
  `tests/test_portal/test_run_research_dataset.py`, and
  `tests/test_portal/test_report_execution_mode_contract.py` preserve and expose
  dataset identity through artifacts, fingerprints, trust, and compact reports.
- The campaign acceptance dataset contains 8,804 ordered Coinbase BTC/USD
  one-hour candles for the declared 2024 evaluation plus 20 warmup bars. A
  repeated preparation reused the same dataset without provider acquisition.

## References

- [ADR 0044: Enforce Known-At Prefix Invariance](0044-enforce-known-at-prefix-invariance.md)
- [ADR 0046: Fingerprint Exact Candle Inputs And Keep Quality Separate](0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md)
- [ADR 0050: Use One Canonical Append-Only Market-Data Store](0050-use-one-canonical-append-only-market-data-store.md)
- [ADR 0052: Typed Fact Collectors And Explicit Instrument Roles](0052-use-typed-fact-collectors-and-explicit-instrument-roles.md)
