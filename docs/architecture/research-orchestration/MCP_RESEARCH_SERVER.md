---
component: mcp-research-server
subsystem: research-orchestration
layer: boundary
doc_type: architecture
status: active
tags:
  - mcp
  - cli
  - experiments
  - indicators
  - agent
  - reporting
  - api
code_paths:
  - cli/mcp_server.py
  - cli/main.py
  - cli/api.py
  - cli/audit.py
  - cli/experiments
  - portal/backend/controller/candles.py
  - portal/backend/controller/indicators.py
  - portal/backend/service/market/candle_service.py
  - portal/backend/service/indicators/indicator_service
  - pyproject.toml
  - docs/architecture/research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md
  - docs/engineering/developer-audit-workflow.md
---
# MCP Research Server

## Purpose

The Quant-Trad MCP server is the protocol adapter for agent/tool hosts. It gives
MCP clients read access to research state and guarded tools for research
operations without creating a second orchestration model.

Launch it with:

```bash
qt mcp serve
```

`qt-mcp` is also installed as a convenience entrypoint for MCP host
configuration, but `qt mcp serve` is the canonical project command.

`make up` prints MCP readiness after the Docker stack starts. It reports the
exact stdio command and whether the Codex MCP alias is configured. It does not
run `qt mcp serve` as a background daemon because stdio MCP servers must be
launched by the MCP host that owns their stdin/stdout.

Useful Make targets:

- `make mcp-ready`
- `make mcp-smoke`
- `make mcp-register-codex`

## Boundary

The MCP server sits on top of the same contracts as the `qt` CLI:

- read-only resources call backend API routes or local experiment state files,
- report and comparison tools call backend reporting routes,
- indicator resources read backend indicator contracts,
- indicator and data-coverage tools delegate to `qt indicators ...` and
  `qt data coverage ...`,
- instrument tools delegate to `qt instruments ...`,
- experiment tools delegate to `qt experiments ...`,
- controlled mutation tools call backend write routes only after explicit
  guardrails are satisfied.

It must not import strategy, runtime, indicator, report-builder, wallet, order,
or trade internals for normal workflow behavior. If a workflow needs runtime or
report truth, that truth must come from the backend API or the local experiment
artifacts already written by `qt experiments`.

## Resources

MCP resources are dynamic read endpoints, not static docs. They expose current
state through `quanttrad://` URIs:

- `quanttrad://health`
- `quanttrad://bots`
- `quanttrad://bots/{bot_id}`
- `quanttrad://bots/{bot_id}/runs?limit={limit}`
- `quanttrad://bots/{bot_id}/active-run`
- `quanttrad://strategies`
- `quanttrad://strategies/{strategy_id}`
- `quanttrad://strategies/{strategy_id}/variants`
- `quanttrad://indicators`
- `quanttrad://indicators/{indicator_id}`
- `quanttrad://indicators/{indicator_id}/strategies`
- `quanttrad://indicators/types`
- `quanttrad://indicators/types/{type_id}`
- `quanttrad://instruments`
- `quanttrad://instruments/{instrument_id}`
- `quanttrad://instruments/{instrument_id}/runtime-profile?execution_semantics={execution_semantics}`
- `quanttrad://providers`
- `quanttrad://reports`
- `quanttrad://reports/{run_id}/summary`
- `quanttrad://reports/{run_id}/diagnostics`
- `quanttrad://reports/{run_id}/metrics`
- `quanttrad://reports/{run_id}/run-report-status`
- `quanttrad://experiments/{experiment_id}/state`
- `quanttrad://experiments/{experiment_id}/summary`
- `quanttrad://experiments/{experiment_id}/events?tail={tail}`

These resources are read-only. They should remain compact and contract-shaped so
agents can decide the next operation without scraping UI projections.

## Tools

The v0 tool surface is grouped by operational risk.

Read tools:

- `health_check`
- `list_bots`
- `get_bot`
- `list_bot_runs`
- `get_active_run`
- `list_strategies`
- `get_strategy`
- `list_strategy_variants`
- `list_indicator_types`
- `get_indicator_type`
- `list_indicators`
- `get_indicator`
- `list_indicator_strategies`
- `list_reports`
- `get_report_section`
- `compare_reports`
- `list_providers`
- `list_instruments`
- `get_instrument`
- `get_instrument_runtime_profile`

Indicator validation tools:

- `validate_indicator_config`
- `validate_indicator_runtime`
- `check_data_coverage`

`validate_indicator_config` resolves the same manifest-backed params,
dependencies, color, and output preferences as create, but does not persist an
instance. `validate_indicator_runtime` runs the indicator through the backend
runtime graph over the requested candle window. It validates that every declared
output is present on every bar, then summarizes readiness and optional
assertions such as "ready by end" or minimum ready bars. Warmup bars are allowed
to return `ready=false`; missing outputs are not. `check_data_coverage` calls
`qt data coverage` to run the same pre-run candle coverage contract used by
experiment planning against an explicit instrument/window.

Experiment tools:

- `draft_experiment_plan`
- `validate_experiment_plan`
- `run_experiment_plan`
- `resume_experiment`
- `get_experiment_status`
- `get_experiment_events`
- `doctor_experiment`
- `summarize_experiment`
- `collect_experiment`
- `prepare_instrument_matrix_experiment`

Controlled mutation tools:

- `start_bot_run`
- `stop_bot_run`
- `update_bot_backtest_window`
- `set_bot_strategy_variant`
- `create_strategy_variant`
- `update_strategy_variant`
- `create_indicator`

Actual run-starting or write operations require `confirm=true`. Tools that can
be usefully previewed default to planned mutations with `apply=false`; applying
them requires both `apply=true` and `confirm=true`. Paper/live starts are
blocked unless the caller also passes `allow_non_backtest=true`.
`create_indicator` follows the same pattern: it validates the config first,
returns a planned mutation by default, and persists only when both `apply=true`
and `confirm=true` are supplied.

`prepare_instrument_matrix_experiment` follows the same guarded shape for
mixed-instrument research. Dry runs return the solo strategy/bot mutations and
normal experiment plan that would be produced. Applying creates one
single-instrument strategy and bot per case, validates the requested execution
profile for each instrument, writes an `experiment_plan.v1`, and leaves runtime
truth to bot execution and report materialization.

`summarize_experiment` delegates to `qt experiments summarize` and returns the
compact `experiment_summary.v1` read model for a suite. It is safe for agents to
call after a run because it reads local suite artifacts: run records, research
summaries, comparison summaries, pass gates, and data preflight. It does not
rebuild reports or inspect runtime internals.

## Invariants

- MCP is not a runtime service and does not own market, strategy, execution, or
  report semantics.
- MCP resources are read views over backend contracts or local experiment
  artifacts.
- MCP tools use backend API routes or `qt` workflows; any tool-level workflow
  exposed for agents should also have a matching `qt` command.
- Indicator runtime validation must use the backend runtime graph and engine
  timeline, not MCP-side reconstruction.
- Long-running experiment tools may block until the underlying `qt` command
  finishes or times out.
- Mutations must fail loud when required IDs, confirmations, or allowed run
  types are missing.
- UI state remains human visualization state and is not read as workflow truth.

## Known Gaps

- Authentication is not modeled because the local backend has no auth boundary.
- The server is stdio-only; no remote MCP transport is implemented.
- Detached/background MCP orchestration is deferred. Long operations currently
  rely on the same foreground `qt experiments` behavior as the CLI.
