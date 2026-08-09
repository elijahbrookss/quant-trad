---
component: adr-agent-mutation-promotion-gates
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - agents
  - cli
  - mcp
  - mutation
  - promotion
  - audit
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

Accepted on 2026-08-06 for offline autonomous research through
`RESEARCH_CERTIFIED`. Operational deployment remains outside this ADR and is
still closed by ADR 0049.

## Context

Agents need to move from observations through hypotheses, checks, backtests,
comparison, and promotion without unrestricted access to runtime or production
state. Today many MCP mutations default to dry-run and require confirmation,
and the CLI writes audit files, but guards are not uniform, audit can be
disabled, actor identity is weak, and promotion is not one enforced gate.

## Decision

Agent research actions use approved CLI, API, service, and durable contracts.
Every mutation will have a plan/read phase, explicit apply intent, idempotency
key, actor/request identity, bounded authority, and durable audit evidence for
inputs, parameters, provenance, validation, outputs, caveats, and result.

Research promotion is an explicit state transition over immutable evidence.
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

The optional CLI audit file may be disabled, but that cannot disable the durable
database proposal, decision, authority-event, attempt, holdout, certificate, or
governance records. Applied offline research mutations use common service
contracts. Non-research mutation surfaces remain outside this acceptance scope.

## Rejected Alternatives

- Give agents direct ORM, shell, or runtime-object mutation access.
- Treat MCP confirmation alone as complete authorization.
- Promote from profitability metrics without causal and accounting evidence.
- Allow audit logging to be optional for applied agent mutations.
- Encode promotion rules separately in each command.

## Enforcing Tests Or Evidence

Implemented evidence:

- Phase 4 authority records actor, request, idempotency, protocol, dataset,
  attempt, candidate, holdout, certificate, and append-only event evidence.
- Phase 5 graph creation is schema-bound, family-bound, and charged to the
  scientific search budget.
- Phase 6 persists a proposal and a separate authorization decision for every
  transition and rejects self-authorization and stale state versions.
- Persisted end-to-end tests reach `RESEARCH_CERTIFIED` and negative tests prove
  shadow, paper, controlled-live, live, deployment, and capital states are
  structurally absent.
- [ADR 0059](0059-use-in-app-scientific-authority-and-offline-certification-ceiling.md)
  records the slim in-application authority and honest assurance boundary.

## References

- [ADR 0017: API-Backed CLI For Research Orchestration](0017-use-api-backed-cli-for-research-orchestration.md)
- [ADR 0019: File-Backed Sequential Experiment Plans](0019-use-file-backed-sequential-experiment-plans.md)
- [Research Orchestration Boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
- [Security Layer](../security/SECURITY_LAYER.md)
