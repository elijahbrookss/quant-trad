---
component: adr-api-backed-research-cli
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - cli
  - agent
  - research
  - experiments
  - api
code_paths:
  - cli
  - pyproject.toml
  - portal/backend/controller
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/reports/contract.py
  - portal/backend/service/reports/comparison.py
  - docs/architecture/research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md
---
# ADR 0017: Use An API-Backed CLI For Research Orchestration

## Status

Accepted on 2026-05-17.

## Context

Quant-Trad needs an interface that a human or future agent can use to start
runs, inspect lifecycle state, export reports, and compare completed research
artifacts without reimplementing backend semantics.

The existing Makefile is useful for local development, Docker, database, and
forensic audit helpers. Those commands are not the right long-term contract for
agent research because some helpers intentionally read local storage or run
diagnostic scripts.

## Decision

Add `qt` as a pyproject console script backed by the `cli/` package. The CLI is
an adapter over the backend API by default.

The backend API remains the formal semantic boundary for bot control, report
materialization, report export, comparison, and future experiment orchestration.
The CLI may compose multiple API calls into a small workflow, but it must not
change wallet, order, trade, fee, slippage, strategy, indicator, or report
semantics.

Long-running research runs use explicit resumable steps:

- `start` creates a bot run through a compact backend start contract and writes
  a local experiment record,
- `status` polls compact run lifecycle/report state,
- `collect` exports reports, materializes `RunReportDTO v2` when needed, and
  compares against a selected baseline.

The normal CLI path uses compact API contracts such as `bot_run_context.v1`,
`bot_run_status.v1`, `run_research_summary.v1`, and
`run_report_comparison_summary.v1`. Full UI/debug payloads may continue to
exist, but they are not the agent research contract.

Make remains the repo-native development and forensic audit surface. The CLI
does not replace Make; it provides a cleaner research/operator interface over
the same backend contracts.

## Consequences

- Agent-facing workflows can call a stable command surface without importing
  backend internals.
- Backend DTOs and API routes stay the source of truth for research artifacts.
- CLI workflows are attributable because they print structured JSON results.
- Resumable experiment records live under ignored `logs/experiments/` partitions
  and point at the run, report export, materialization status, and comparison
  artifacts they created.
- Direct local scripts remain available for diagnostics, but they are not the
  default experiment orchestration contract.
- Future experiment specs can target the CLI/API layer without adding a new
  runtime engine or broad materialization framework.

## References

- [Research orchestration boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
- [Developer audit workflow](../../engineering/developer-audit-workflow.md)
