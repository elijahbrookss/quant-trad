---
component: adr-agent-mutation-promotion-gates
subsystem: research-orchestration
layer: decision
doc_type: adr
status: proposed
tags:
  - adr
  - agents
  - cli
  - mcp
  - mutation
  - promotion
  - audit
  - proposed
code_paths:
  - cli/main.py
  - cli/mcp_server.py
  - cli/audit.py
  - cli/experiments
  - portal/backend/service/research
  - docs/architecture/research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md
---
# ADR 0048: Gate Agent Mutation And Research Promotion

## Status

Proposed on 2026-07-25. Existing CLI/MCP guards are partial enforcement; the
complete mutation and promotion policy is not yet implemented.

## Context

Agents need to move from observations through hypotheses, checks, backtests,
comparison, and promotion without unrestricted access to runtime or production
state. Today many MCP mutations default to dry-run and require confirmation,
and the CLI writes audit files, but guards are not uniform, audit can be
disabled, actor identity is weak, and promotion is not one enforced gate.

## Decision

Agent actions will use approved CLI, MCP, service, and durable job contracts.
Every mutation will have a plan/read phase, explicit apply intent, idempotency
key, actor/request identity, bounded authority, and durable audit evidence for
inputs, parameters, provenance, validation, outputs, caveats, and result.

Research promotion will be an explicit state transition over immutable evidence.
It will require declared gates for dataset identity/quality, known-at validity,
repeatability, accounting reconciliation, supported simulator assumptions, and
human approval where policy requires it. Agents may recommend promotion but
cannot bypass the gate or mutate live/production state directly.

## Invariants

- Read operations cannot mutate state as a side effect.
- Agent mutations default to dry-run and require explicit apply/confirmation.
- Direct database, runtime-memory, credential, and production-state mutation is
  outside the agent contract.
- Every applied action is attributable, idempotent, inspectable, and replayable
  from durable evidence.
- Missing data, unsupported capability, failed validation, and caveats remain
  visible through promotion.
- Promotion never means live-trading authorization.

## Consequences

Some current CLI paths will need a stricter agent mode or a common mutation
service. `--no-audit-log` cannot be available to an agent mutation workflow.
Promotion becomes slower but reviewable and safe to automate.

## Rejected Alternatives

- Give agents direct ORM, shell, or runtime-object mutation access.
- Treat MCP confirmation alone as complete authorization.
- Promote from profitability metrics without causal and accounting evidence.
- Allow audit logging to be optional for applied agent mutations.
- Encode promotion rules separately in each command.

## Enforcing Tests Or Evidence

Current partial evidence:

- `tests/test_cli/test_mcp_server.py` verifies experiment plans default to
  dry-run and controlled mutations require apply/confirmation.
- `cli/audit.py` and experiment state/event stores record local command and
  orchestration evidence.
- Research checks and report readiness expose evidence and caveats without
  performing strategy promotion.

Required before acceptance:

- a common mutation contract across every agent-writable surface;
- actor, request, idempotency, and durable audit assertions;
- proof that agent mode cannot disable audit;
- an enforced promotion state machine and negative bypass tests;
- explicit denial of runtime and live-state direct mutation.

## References

- [ADR 0017: API-Backed CLI For Research Orchestration](0017-use-api-backed-cli-for-research-orchestration.md)
- [ADR 0019: File-Backed Sequential Experiment Plans](0019-use-file-backed-sequential-experiment-plans.md)
- [Research Orchestration Boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
- [Security Layer](../security/SECURITY_LAYER.md)
