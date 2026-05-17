---
component: adr-file-backed-sequential-experiment-plans
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - cli
  - experiments
  - agent
  - research
  - reporting
code_paths:
  - cli/experiments
  - cli/main.py
  - portal/backend/controller/bots.py
  - docs/architecture/research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md
---
# ADR 0019: Use File-Backed Sequential Experiment Plans

## Status

Accepted on 2026-05-17.

## Context

Quant-Trad needs repeatable research validation across paired strategy variants
and fresh date windows. The immediate workflow is sequential:

1. run baseline and candidate bots over one or more windows,
2. wait for terminal run state,
3. export and materialize reports,
4. compare compact report summaries,
5. evaluate pass gates,
6. notify the operator or agent when terminal.

This does not require a distributed scheduler, queue, or new backend service.
The existing backend already owns the semantic operations for bot starts, run
status, report exports, report materialization, research summaries, and compact
comparisons.

## Decision

Implement experiment plans as a CLI-owned, file-backed orchestration layer under
`cli/experiments`.

The CLI runner:

- reads `experiment_plan.v1` YAML or JSON,
- validates the plan before every plan run,
- performs non-blocking data preflight against backend candle coverage contracts,
- normalizes the plan into immutable `plan.json`,
- writes resumable `experiment_suite_state.v1` to `state.json`,
- appends operation audit events to `events.ndjson`,
- runs steps sequentially,
- stores only artifact references and compact summaries,
- evaluates pass gates from compact report/research artifacts,
- writes terminal notification attempts to `notifications.json`.

Experiment records live under:

```text
logs/experiments/YYYY/MM/DD/<experiment_id>/
  plan.json
  state.json
  events.ndjson
  runs/
  artifacts/
  notifications.json
```

The runner may update `backtest_start` and `backtest_end` through the existing
backend bot update API before starting each window run. It must not mutate bot
storage directly.

Data preflight warnings do not make `validate-plan` fail. `run-plan` surfaces
the same warnings and requires explicit operator/agent acknowledgement before
starting runs.

The first version is foreground and sequential only. A backend-owned detached
worker can be added later if the foreground process becomes operationally
insufficient.

## Consequences

- Agents and humans get a durable plan/run/compare contract without a scheduler.
- Resume is local and deterministic because state is explicit on disk.
- Experiments remain research orchestration artifacts, not runtime truth.
- Pass gates are deterministic evaluators over declared compact fields; missing
  fields produce unsupported/failed gate results instead of invented metrics.
- Candle coverage checks identify provider, exchange, symbol, timeframe,
  requested range, available range, missing ranges, and continuity status before
  long runs are started.
- The CLI can run long experiments while preserving intent/result events for
  later audit.
- Backend business logic remains behind backend routes.
- This does not implement dynamic strategy invention, auto-promotion,
  concurrency, MCP tools, or a general log explorer.

## References

- [Research orchestration boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
- [Use an API-backed CLI for research orchestration](0017-use-api-backed-cli-for-research-orchestration.md)
- [Use output filters as the strategy variant contract](0018-use-output-filters-as-strategy-variant-contract.md)
