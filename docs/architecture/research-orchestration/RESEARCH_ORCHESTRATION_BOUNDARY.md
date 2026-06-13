---
component: research-orchestration-boundary
subsystem: research-orchestration
layer: boundary
doc_type: architecture
status: active
tags:
  - cli
  - mcp
  - experiments
  - indicators
  - agent
  - reporting
  - api
code_paths:
  - cli
  - cli/mcp_server.py
  - cli/experiments
  - pyproject.toml
  - Makefile
  - portal/backend/controller/bots.py
  - portal/backend/controller/indicators.py
  - portal/backend/controller/reports.py
  - portal/backend/controller/research.py
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/indicators/indicator_service
  - portal/backend/service/research
  - portal/backend/service/reports/contract.py
  - portal/backend/service/reports/comparison.py
  - docs/engineering/developer-audit-workflow.md
---
# Research Orchestration Boundary

## Purpose

The research orchestration boundary gives agent/tool workflows a small,
deterministic way to operate the existing system: start bot runs, wait for
terminal lifecycle state, materialize/export reports, and compare completed
runs.

Humans can still use the CLI for reproducible operations, but the product UI is
the human visualization and inspection surface. UI state is never workflow
truth.

MCP is the protocol adapter for agent hosts. It exposes read-only resources and
guarded tools over the same `qt` CLI and backend API contracts; it is not a new
runtime or reporting authority.

It is an interface boundary, not a new strategy engine.

## Boundary Contract

The backend API owns the semantic contract for orchestration. The CLI is a thin
adapter over that API.

The boundary may:

- call bot control API routes,
- call bot/window data preflight routes,
- call direct instrument/window candle coverage routes,
- call canonical instrument read/profile routes when an experiment needs to
  bind source instruments to explicit execution semantics,
- call indicator catalog, config validation, create, edit, delete, enable,
  disable, strategy binding, and runtime validation API routes,
- call report readiness, materialization, export, and comparison API routes,
- compose API calls into small workflows,
- print structured JSON for automation,
- write downloaded report exports to local ignored paths,
- write CLI invocation audit logs for command/API/artifact provenance.
- run file-backed sequential experiment plans that compose those API routes.
- expose MCP resources and tools that call these same API and CLI contracts.
- create research-memory observations, checks, hypotheses, studies, and links
  through the research API.

The boundary must not:

- import strategy, wallet, order, trade, fee, slippage, or indicator internals
  for normal workflows,
- resolve strategy variants outside the backend,
- rebuild reports through a parallel path,
- infer run truth from UI state,
- mutate runtime or reporting semantics.
- treat local experiment state as canonical runtime truth.
- treat MCP resources as cached truth or introduce MCP-only workflow semantics.
- treat research-memory items or check outputs as runtime, report, or execution
  truth.

## Local Log Partitioning

CLI-generated logs and report exports are local operator artifacts and are not
repo source.

Default partitions:

- CLI invocation audit logs:
  `logs/cli/YYYY/MM/DD/<command>/<subcommand>/<operation_id>.json`
- MCP-drafted experiment plans:
  `logs/experiments/plans/YYYY/MM/DD/<timestamp>__<plan_name>.json`
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
- `artifacts/summaries/` stores research summaries, pass gate results, data
  preflight, and optional suite summaries.
- `notifications.json` records terminal notification attempts.

Plan validation includes a data preflight when the backend is reachable. The
preflight checks the bot's resolved strategy instruments for each requested
window and returns provider, exchange, symbol, timeframe, requested range,
available range, missing ranges, and candle continuity status. These checks use
the shared candle continuity summary model, but they are pre-run coverage
evidence rather than post-run report truth.

`qt data coverage` exposes the same `candle_coverage_preflight.v1` check for a
single explicit instrument/window before a bot or experiment plan exists.

`qt research` captures the reasoning trail around research work. Observations,
lightweight research checks, hypotheses, studies, and links to strategies,
variants, runs, reports, and experiments live in the research-memory boundary.
Research checks may request source candles, persisted indicator output evidence,
or completed run report evidence through existing backend contracts and produce
analytical evidence. `qt research check sweep` runs non-persisted
indicator-backed check variants, ranks explicit emitted metric paths, and can
render either JSON or a compact metric table. It does not create research
memory, execute strategies, or simulate trades.

`qt experiments summarize` reads the local suite artifacts and emits
`experiment_summary.v1`: suite status, compact run metrics, readiness caveats,
section row counts, comparison deltas, pass gate status, and data preflight
continuity. It is an operator/agent read model over already-written artifacts;
it does not rebuild report truth or inspect runtime internals.

`qt experiments prepare-instrument-matrix` prepares solo strategy and bot cases
from a source bot/strategy plus explicit instrument groups. It may dry-run the
planned strategy/bot mutations, or with `--apply --confirm` create one
single-instrument strategy and bot per case, validate each instrument runtime
profile, and write a normal `experiment_plan.v1`. The command is an
orchestration helper only: the resulting bots still own runtime execution, and
the existing plan runner still starts runs, exports reports, materializes report
truth, and compares completed runs.

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
| CLI | Primary API-backed workflow and operation surface for agents and tools. |
| MCP | Protocol adapter for agent hosts; exposes read-only resources and guarded tools over `qt` and backend API contracts. |
| Makefile | Local development, Docker, DB, validation, and forensic audit support index. |
| QuantLab/UI | Human visualization and inspection surface, especially for candles, indicators, overlays, BotLens, playback, and reports. |

## Current CLI Surface

`qt` is the primary agent/tool command surface. It exposes API-backed commands
for:

- backend health checks,
- bot listing, inspection, start, stop, active-run, and recent runs through
  compact run-context contracts,
- bot strategy/variant selection updates,
- strategy listing, detail inspection, compilation, and preview,
- strategy variant listing, creation, update, and deletion through output
  filters,
- indicator type/instance inspection, config validation, planned creation,
  clone/edit/delete/toggle operations, and runtime validation through
  `qt indicators ...`,
- instrument listing, detail inspection, and runtime profile compilation through
  `qt instruments ...`,
- direct candle coverage inspection through `qt data coverage`,
- research-memory item/link capture and lightweight historical checks through
  `qt research ...`; the check surface is intentionally compact:
  `qt research check raw`, `qt research check indicator`,
  `qt research check audit`, `qt research check lifecycle`,
  `qt research check signal`, `qt research check decision`, and
  `qt research check sweep`,
- research evidence read models through `qt research run`, `qt research trail`,
  and `qt research compare`,
- run lifecycle waiting through compact run status API state,
- report listing, readiness, compact research summary, diagnostics,
  materialization status/build, export, and materialized report comparison
  summary,
- `experiments start-bot`, `experiments status`, and `experiments collect` for
  long-running resumable research work,
- `experiments run-bot` as a one-shot wrapper over the same start/collect flow.
- `experiments validate-plan`, `run-plan`, `resume`, `watch`, `events`, and
  `doctor` for sequential, file-backed experiment suites.
- `experiments summarize` for compact suite-level read models over local
  experiment artifacts.
- `experiments prepare-instrument-matrix` for creating solo bot/strategy cases
  and a plan for truthful spot-proxy versus derivative comparisons.

The experiment layer is intentionally file-backed and small. It proves the
automation seam without introducing a separate experiment database, scheduler,
or variant generation system.

## Current MCP Surface

`qt mcp serve` starts the stdio MCP server for agent hosts. The server exposes:

- read-only `quanttrad://` resources for health, bots, strategies, providers,
  indicators, reports, report sections, and local experiment state/events,
- read tools for the same API-backed inspection routes,
- indicator config validation, runtime validation, and data coverage tools that
  delegate to the matching `qt indicators ...` and `qt data coverage` commands,
- instrument tools that delegate to the matching `qt instruments ...` commands,
- experiment tools that delegate to `qt experiments ...`,
- controlled mutation tools for starting/stopping runs, updating bot backtest
  windows, setting bot strategy variants, creating/updating strategy variants,
  and creating manifest-backed indicator instances.

Run-starting and write tools require explicit confirmation. Tools with useful
previews default to planned mutations and require both `apply=true` and
`confirm=true` before calling backend write routes.

Indicator creation follows that same preview-first contract. MCP can validate
and create configured instances from registered indicator types; it does not
author or load new Python indicator implementations. Runtime validation uses
the backend indicator engine timeline and checks every declared output on every
bar before summarizing readiness.

## Plan-Based Experiment Contracts

The sequential suite contracts are artifact-reference based:

- `experiment_plan.v1`
- `experiment_suite_state.v1`
- `experiment_step_state.v1`
- `experiment_event.v1`
- `pass_gate_result.v1`
- `comparison_result_ref.v1`
- `experiment_summary.v1`
- `notification_policy.v1`
- `experiment_data_preflight.v1`
- `bot_data_preflight.v1`
- `candle_coverage_preflight.v1`
- `instrument_matrix_experiment_request.v1`

These contracts intentionally avoid embedding full report DTOs. Reports,
research summaries, materialized RunReportDTO contract (`run_report.v2`), and comparison semantics are
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
- Humans inspect runtime and research state through UI views; agents and tools
  operate workflows through `qt`.
- MCP clients operate through `qt mcp serve`; MCP must stay a protocol adapter
  over `qt` and backend contracts.
- Indicator research workflows must treat backend indicator config validation,
  runtime validation, and output evidence collection as the truth surface for
  agent-visible indicator readiness and research checks.
- Strategy-bound indicator parameter/dependency changes must be clone-first;
  metadata edits may still use the edit path.
- Plan-based experiments default to sequential execution. Bounded run-step
  parallelism belongs in the runner's `run_policy`, not in MCP-only workflow
  behavior.
- Data preflight warnings must be surfaced with provider/symbol/window context
  before a run starts.
- Pass gate evaluation must be deterministic and explain which source fields
  were used or missing.
- Make commands can still exist for direct local diagnostics, but direct storage
  access is an explicit forensic path, not the default orchestration path.
- Run-aware Make diagnostics should use a `forensic-` prefix so they are not
  confused with normal `qt` workflows.

## Known Gaps

- CLI authentication is not modeled because the local backend currently has no
  auth boundary.
- Detached/background orchestration and bounded parallel run execution are
  deferred until foreground plan execution proves insufficient.
- Email/SMS notification sinks are deferred; the current sinks are console/file.
