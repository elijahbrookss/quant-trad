---
component: adr-live-trading-closed-boundary
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - live-trading
  - safety
  - agents
  - credentials
  - cleanup
code_paths:
  - portal/backend/service/bots/runtime_composition.py
  - portal/backend/service/bots/execution_behavior.py
  - portal/backend/service/bots/observe_only_runtime.py
  - src/engines/bot_runtime/core/execution_adapter.py
  - docs/architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md
  - docs/architecture/security/SECURITY_LAYER.md
---
# ADR 0049: Keep Live Order Submission Closed

## Status

Accepted on 2026-07-25.

This records QT's existing no-live-trading boundary explicitly.

## Context

The repository uses `live` both for an active runtime lifecycle state and for a
reserved runtime-composition mode. Neither label proves that production exchange
order submission is implemented or authorized. Paper market-data ingestion,
provider credentials, and deterministic fill adapters must not accidentally
combine into a live trading path.

## Decision

Quant-Trad has no authorized production order-submission capability. Backtest
and paper execution use deterministic simulated
fills; paper may also run `observe-only`, which forbids order, fill, trade, fee,
slippage, and wallet mutation semantics.

`RuntimeMode.LIVE` is a reserved composition seam and `LIVE` lifecycle means a
runtime is actively producing facts. Neither may select a brokerage/exchange
order adapter. Credential references grant data/provider access only; they do
not grant trading authority. Agent contracts cannot start or mutate live order
state.

A future live-trading capability requires a separate accepted ADR and explicit
admission controls, scoped trade credentials, idempotent venue order identity,
pre-trade risk limits, reconciliation, kill switches, operator authorization,
and failure-recovery evidence.

## Invariants

- No production code path submits, amends, or cancels an external order.
- Simulated and observe-only behavior are explicit and fail on unknown values.
- Paper mode never places a live order.
- A `live` lifecycle/composition label cannot confer venue-trading capability.
- Credentials alone cannot authorize execution.
- CLI, MCP, workers, and agents cannot bypass the boundary.

## Consequences

Current research and paper validation remain safe from external order placement.
The reserved `live` seam may be misleading and should stay clearly documented
until it is either removed or implemented behind a separately reviewed boundary.
Live-trading features remain outside the authorized product boundary.

## Rejected Alternatives

- Reuse the simulated adapter shape and switch providers by configuration.
- Infer trading authority from a live credential environment.
- Let a generic MCP confirmation enable live orders.
- Add a dormant venue adapter before reconciliation and risk controls exist.
- Treat paper stability as sufficient evidence for production trading.

## Enforcing Tests Or Evidence

- `tests/test_cli/test_mcp_server.py::
  test_mcp_start_bot_run_is_guarded_and_defaults_to_backtest` verifies guarded
  starts and a backtest default.
- `tests/test_portal/test_bot_runtime_control_service.py::
  test_start_observe_only_paper_run_uses_docker_runner_with_effective_snapshot`
  verifies explicit observe-only paper behavior.
- `portal/backend/service/bots/execution_behavior.py` accepts only simulated or
  observe-only execution behavior.
- A repository-wide production-code audit found no provider order-submission
  implementation. This negative audit is current evidence, not a substitute
  for the required safety tests if live trading is ever proposed.

## References

- [ADR 0012: Runtime Composition Root](0012-use-runtime-composition-root-for-mode-aware-wiring.md)
- [ADR 0024: Provider Credential References](0024-use-provider-credential-references.md)
- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Security Layer](../security/SECURITY_LAYER.md)
