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
  - agent
  - reporting
  - api
code_paths:
  - cli/mcp_server.py
  - cli/main.py
  - cli/api.py
  - cli/audit.py
  - cli/experiments
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
- `quanttrad://providers`
- `quanttrad://reports`
- `quanttrad://reports/{run_id}/summary`
- `quanttrad://reports/{run_id}/diagnostics`
- `quanttrad://reports/{run_id}/metrics`
- `quanttrad://reports/{run_id}/run-report-status`
- `quanttrad://experiments/{experiment_id}/state`
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
- `list_reports`
- `get_report_section`
- `compare_reports`
- `list_providers`

Experiment tools:

- `draft_experiment_plan`
- `validate_experiment_plan`
- `run_experiment_plan`
- `resume_experiment`
- `get_experiment_status`
- `get_experiment_events`
- `doctor_experiment`
- `collect_experiment`

Controlled mutation tools:

- `start_bot_run`
- `stop_bot_run`
- `update_bot_backtest_window`
- `set_bot_strategy_variant`
- `create_strategy_variant`
- `update_strategy_variant`

Actual run-starting or write operations require `confirm=true`. Tools that can
be usefully previewed default to planned mutations with `apply=false`; applying
them requires both `apply=true` and `confirm=true`. Paper/live starts are
blocked unless the caller also passes `allow_non_backtest=true`.

## Invariants

- MCP is not a runtime service and does not own market, strategy, execution, or
  report semantics.
- MCP resources are read views over backend contracts or local experiment
  artifacts.
- MCP tools use backend API routes or `qt experiments` workflows.
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
