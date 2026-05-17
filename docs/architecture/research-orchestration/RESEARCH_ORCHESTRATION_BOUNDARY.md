---
component: research-orchestration-boundary
subsystem: research-orchestration
layer: boundary
doc_type: architecture
status: active
tags:
  - cli
  - experiments
  - agent
  - reporting
  - api
code_paths:
  - cli
  - cli/experiments
  - pyproject.toml
  - Makefile
  - portal/backend/controller/bots.py
  - portal/backend/controller/reports.py
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/reports/contract.py
  - portal/backend/service/reports/comparison.py
  - docs/engineering/developer-audit-workflow.md
---
# Research Orchestration Boundary

## Purpose

The research orchestration boundary gives humans and future agents a small,
deterministic way to operate the existing system: start bot runs, wait for
terminal lifecycle state, materialize/export reports, and compare completed
runs.

It is an interface boundary, not a new strategy engine.

## Boundary Contract

The backend API owns the semantic contract for orchestration. The CLI is a thin
adapter over that API.

The boundary may:

- call bot control API routes,
- call bot/window data preflight routes,
- call report readiness, materialization, export, and comparison API routes,
- compose API calls into small workflows,
- print structured JSON for automation,
- write downloaded report exports to local ignored paths,
- write CLI invocation audit logs for command/API/artifact provenance.
- run file-backed sequential experiment plans that compose those API routes.

The boundary must not:

- import strategy, wallet, order, trade, fee, slippage, or indicator internals
  for normal workflows,
- resolve strategy variants outside the backend,
- rebuild reports through a parallel path,
- infer run truth from UI state,
- mutate runtime or reporting semantics.
- treat local experiment state as canonical runtime truth.

## Local Log Partitioning

CLI-generated logs and report exports are local operator artifacts and are not
repo source.

Default partitions:

- CLI invocation audit logs:
  `logs/cli/YYYY/MM/DD/<command>/<subcommand>/<operation_id>.json`
- experiment records:
  `logs/experiments/YYYY/MM/DD/<experiment_id>/experiment.json`
- report export zips:
  `logs/reports/YYYY/MM/DD/run_<run_id>/<export_zip>`

Each CLI audit file records the command path, argv, parsed args, HTTP calls,
written artifacts, timing, and exit code. These logs are for replayability of
research operations, not runtime truth. Runtime facts, report materializations,
wallet/order/trade semantics, and report DTOs remain owned by backend/runtime
services.

Experiment records are resumable local pointers. They record the bot id, run id,
optional baseline run id, start payload, collect result, report export path,
materialization status, and comparison summary. They do not become canonical run
truth and must not be read by runtime services.

Plan-based experiment suites use a richer but still local layout:

- `plan.json` is the immutable normalized `experiment_plan.v1` plus hash.
- `state.json` is the mutable `experiment_suite_state.v1` resume pointer.
- `events.ndjson` is append-only orchestration intent/result evidence.
- `runs/<window_id>__<variant_id>.json` records run ids and compact artifact refs.
- `artifacts/reports/` stores report export zips for the suite.
- `artifacts/comparisons/` stores compact comparison summaries.
- `artifacts/summaries/` stores research summaries and pass gate results.
- `notifications.json` records terminal notification attempts.

Plan validation includes a data preflight when the backend is reachable. The
preflight checks the bot's resolved strategy instruments for each requested
window and returns provider, exchange, symbol, timeframe, requested range,
available range, missing ranges, and candle continuity status. These checks use
the shared candle continuity summary model, but they are pre-run coverage
evidence rather than post-run report truth.

`validate-plan` reports data warnings without failing. `run-plan` performs the
same validation internally and requires explicit acknowledgement before starting
runs when data warnings or errors are present.

The plan runner may update a bot's `backtest_start` and `backtest_end` through
the backend update API before starting a run for a specific window. The run's
backend snapshot remains the execution truth for what actually ran.

## Layer Roles

| Layer | Role |
| --- | --- |
| Backend API | Formal semantic boundary for bot control, reports, comparison, and future experiment operations. |
| CLI | API-backed research adapter for humans and agents. |
| Makefile | Local development, Docker, DB, validation, and forensic audit command index. |
| QuantLab/UI | Visual debugging and inspection surface, especially for candles, indicators, overlays, and reports. |

## Current CLI Surface

`qt` exposes API-backed commands for:

- backend health checks,
- bot listing, inspection, start, stop, active-run, and recent runs through
  compact run-context contracts,
- bot strategy/variant selection updates,
- strategy listing, detail inspection, compilation, and preview,
- strategy variant listing, creation, update, and deletion through output
  filters,
- run lifecycle waiting through compact run status API state,
- report listing, readiness, compact research summary, diagnostics,
  materialization status/build, export, and materialized report comparison
  summary,
- `experiments start-bot`, `experiments status`, and `experiments collect` for
  long-running resumable research work,
- `experiments run-bot` as a one-shot wrapper over the same start/collect flow.
- `experiments validate-plan`, `run-plan`, `resume`, `watch`, `events`, and
  `doctor` for sequential, file-backed experiment suites.

The experiment layer is intentionally file-backed and small. It proves the
automation seam without introducing a separate experiment database, scheduler,
or variant generation system.

## Plan-Based Experiment Contracts

The sequential suite contracts are artifact-reference based:

- `experiment_plan.v1`
- `experiment_suite_state.v1`
- `experiment_step_state.v1`
- `experiment_event.v1`
- `pass_gate_result.v1`
- `comparison_result_ref.v1`
- `notification_policy.v1`
- `experiment_data_preflight.v1`
- `bot_data_preflight.v1`
- `candle_coverage_preflight.v1`

These contracts intentionally avoid embedding full report DTOs. Reports,
research summaries, materialized `RunReportDTO v2`, and comparison semantics are
still owned by backend reporting routes.

Pass gates are registry-backed evaluators. Shorthand plan keys such as
`max_drawdown_pct` normalize to explicit gate specs with metric, source, scope,
operator, and threshold. If a requested compact field is unavailable, the gate
returns unsupported/failed rather than inventing a metric.

## Invariants

- Runtime semantics stay in runtime services.
- Research views stay in reporting services.
- The CLI calls API contracts instead of importing backend services for normal
  workflows.
- CLI output should be machine-readable JSON so agent workflows can store and
  compare artifacts.
- CLI audit logs should remain local under `logs/` and should not be committed.
- Normal CLI research commands should prefer compact API contracts over full UI
  projection payloads.
- Plan-based experiments must stay sequential until real pressure justifies
  concurrency.
- Data preflight warnings must be surfaced with provider/symbol/window context
  before a run starts.
- Pass gate evaluation must be deterministic and explain which source fields
  were used or missing.
- Make commands can still exist for direct local diagnostics, but direct storage
  access is an explicit forensic path, not the default orchestration path.

## Known Gaps

- CLI authentication is not modeled because the local backend currently has no
  auth boundary.
- Detached/background orchestration is intentionally deferred until foreground
  plan execution proves insufficient.
- Email/SMS notification sinks are deferred; the current sinks are console/file.
