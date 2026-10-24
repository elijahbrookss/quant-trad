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
  - portal/backend/run_backend.py
  - portal/backend/controller/bots.py
  - portal/backend/controller/indicators.py
  - portal/backend/controller/reports.py
  - portal/backend/controller/research.py
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/indicators/indicator_service
  - portal/backend/service/research
  - portal/backend/workers/research_worker.py
  - portal/backend/service/reports/contract.py
  - portal/backend/service/reports/comparison.py
  - docs/engineering/developer-workflow.md
---
# Research Orchestration Boundary

## Offline Research Authority Ceiling

The offline research surface separates three provider-free boundaries:

- [Scientific Research Authority](SCIENTIFIC_RESEARCH_AUTHORITY.md) owns
  immutable protocols, role-derived frozen datasets, attempt/search accounting,
  candidate freezing, one-use holdouts, and S0-S4 evidence.
- [Typed Strategy and Action Graph](TYPED_STRATEGY_GRAPH.md) owns bounded
  data-only strategy invention and canonical action-intent compilation; graph
  creation consumes family search budget.
- [Offline Research Governance](OFFLINE_RESEARCH_GOVERNANCE.md) owns separate
  proposal/decision records and registry promotion through
  `RESEARCH_CERTIFIED` only.

These are logical components inside the current application and PostgreSQL
deployment. They do not require allocator, researcher, certifier, governance,
or auditor microservices. Stronger identity/custody services are future seams,
not a prerequisite for honest controlled-workflow claims.

[Check Evidence Boundary](CHECK_EVIDENCE_BOUNDARY.md) defines the canonical
preview, frozen evidence, replay, Observation, and trail workflow. It uses versioned QT-owned evaluators rather than embedding an objective
in orchestration code. An external
research objective does not become QT domain vocabulary or product authority.

The autonomous research workflow cannot choose provider data, resolve a sealed
holdout through public operations, disable durable audit, mutate a frozen
candidate, self-authorize a transition, or progress to shadow, paper, live,
deployment, external submission, or capital states.

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

Compact run-status polling joins persisted terminal truth with the latest hot
runtime health. A terminal persisted run status dominates a stale non-terminal
lifecycle phase; while active, the newest projected event time and progress
drive checkpoint output. Therefore qt runs wait observes forward progress and
termination without reading frontend state or a stale initial lifecycle row.

Complete research evidence is terminal work. An active-run research request
returns a typed deferred summary from cheap run/lifecycle fields and never
constructs the growing signals/decisions/trades dataset. Terminal requests
continue through the canonical report research dataset.

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
- dispatch supported long-running research operations through shared async jobs,
  then read status/result contracts by job id.
- sequence Check requirement planning, explicit preparation, Dataset freeze,
  provider-free execution, replay, Observation creation, and trail inspection
  through shared application operations.

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
- calculate indicators, features, event populations, causal fact joins,
  outcomes, folds, statistics, assertions, pass gates, or evidence hashes.
- trust caller-supplied scientific evidence projections instead of resolving
  and replaying canonical Check or Backtest references.

## Canonical Research Journey

```text
objective
  -> Check requirements
  -> explicit preparation (optional acquisition authority)
  -> preview OR Dataset freeze
  -> provider-free Indicator + Check
  -> durable result
  -> replay
  -> evidence-backed Observation
  -> evidence trail/report projection
```

Preview is ephemeral mutable-store feedback and cannot create durable evidence.
Evidence requires a frozen Dataset. Backtest remains a separate path requiring
an exact versioned Strategy. Check statistics and assertions have no promotion,
certification, paper, or live authority; Scientific Protocol owns advanced
validation and certification.

## Local Log Partitioning

CLI-generated logs, requests, experiment plans and report exports are private
operator/research artifacts, not repository source. Keep them in authorized
private storage. An ignored local path reduces accidental staging but is not an
access-control or durable-record guarantee. Actual findings and interpretations
belong in QT's private record stores under the
[public infrastructure/private research contract](../../contracts/platform/00_system_contract.md#public-infrastructure-and-private-research).
Public documentation and fixtures use synthetic material or placeholders.

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
Current analytical execution uses registered raw-candle and event-and-fact
procedures with explicit frozen bindings and canonical Indicator/Fact inputs.
Legacy diagnostic/audit/sweep interfaces do not override current registry
admission or promise that retired procedures execute.

For heavier admitted operations, `qt research check ... --dispatch` submits a
research async job and returns a receipt. `qt research jobs status` and
`qt research jobs result` read the same backend job contract. Dispatch changes
where work runs, not its scientific eligibility or accounting.

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

The offline governance service enforces its proposal/decision, evidence and
state-transition contract. General CLI/MCP confirmation is a different boundary:
not every local command has a plan/apply envelope, and local invocation logs are
not governance audit. The local backend does not provide a remote authenticated
principal boundary. Use authorized operations and the owning service's admission;
do not infer deployment or trading permission from a confirmation flag.

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
- the primary Check workflow through `qt research check requirements`,
  `preview`, `prepare`, `run`, and `replay`, plus evidence-backed Observation
  creation and `qt research trail`,
- explicitly supported preview/diagnostic operations exposed by current CLI
  help and registry admission; dormant compatibility commands do not promise
  executable historical methods,
- asynchronous research check dispatch through `qt research check ... --dispatch`
  plus `qt research jobs status/result <job_id>`,
- research evidence read models through `qt research run`, `qt research trail`,
  and `qt research compare`,
- run lifecycle waiting through compact run status API state that combines
  fresh hot progress with persisted terminal precedence,
- report listing, readiness, compact research summary, diagnostics,
  materialization status/build, export, and materialized report comparison
  summary,
- `experiments start-bot`, `experiments status`, and `experiments collect` for
  long-running resumable research work,
- deprecated `experiments run-bot` as a compatibility wrapper with a structured
  replacement message; primary workflows use start/collect or a plan.
- `experiments validate-plan`, `run-plan`, `resume`, `watch`, `events`, and
  `doctor` for sequential, file-backed experiment suites.
- `experiments summarize` for compact suite-level read models over local
  experiment artifacts.
- `experiments prepare-instrument-matrix` for creating solo bot/strategy cases
  and a plan for truthful spot-proxy versus derivative comparisons.

The experiment layer is intentionally file-backed and small. It proves the
automation seam without introducing a separate experiment database, scheduler,
or variant generation system.

Collected run and experiment summaries preserve the report dataset's canonical
identity hashes, repeatability state, data/execution quality, blockers, and
caveats. They do not define another quality model or infer promotion readiness
from performance metrics.

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
- CLI audit logs remain private under `logs/` and must not be committed.
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
- Confirmation, local invocation audit, scientific authority and offline
  governance have distinct admission contracts. None grants external-order,
  credential, capital or deployment authority.

## Known Gaps

- CLI authentication is not modeled because the local backend currently has no
  auth boundary.
- Detached/background orchestration and bounded parallel run execution are
  deferred until foreground plan execution proves insufficient.
- General local CLI confirmation does not create an authenticated security
  boundary; offline governance retains its separately enforced contract.
- Email/SMS notification sinks are deferred; the current sinks are console/file.


### Bounded catalog discovery

Use `qt data series --metadata-only` or the MCP resource
`quanttrad://data/series?instrument_id=<id>` to discover registered series.
Both use the backend metadata contract and avoid full-history counting.
Requirements and frozen-input resolution use the same store method, then
retain exact window/source/quality admission. The presence of a series is not
proof of usable research coverage. The existing `qt data series` count catalog
remains available when exact accepted-history statistics are explicitly needed.
