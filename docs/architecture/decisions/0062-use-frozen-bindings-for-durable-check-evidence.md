---
component: adr-frozen-bindings-durable-check-evidence
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - research
  - checks
  - datasets
  - replay
  - evidence
  - provider-free
code_paths:
  - src/market_data/frozen.py
  - src/research_science/check.py
  - portal/backend/service/market/frozen_dataset_service.py
  - portal/backend/service/research
  - portal/backend/controller/research.py
  - cli/research_operations.py
  - cli/main.py
  - cli/mcp_server.py
  - docs/architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md
---
# ADR 0062: Use Frozen Bindings For Durable Check Evidence

## Status

Accepted on 2026-08-08.

## Context

Backtests already had strict frozen Dataset reads, but the read machinery was
coupled to a Strategy. Research orchestration and historical scripts could
therefore calculate events, joins, outcomes, gates, and hashes outside the
canonical Check boundary. Mutable-store Check records also lacked one clear
distinction between exploratory feedback and replayable evidence.

Dataset freeze had additionally accumulated consumer-readiness pressure. A
known provider gap could block materialization globally even though Dataset
identity should describe what QT knows and each consumer owns its own readiness
decision.

## Decision

Extract `FrozenMarketDataReadBinding` as a Strategy-independent, provider-free
binding of exact Dataset identity, subjects, series/source identities, ranges,
revisions, material/provenance/quality/gap hashes, recorded gaps, watermark,
and causal reads.

Use two Check modes:

- preview reads a commit/watermark-pinned current-store view, remains
  ephemeral, and cannot support an evidence-bearing Observation;
- evidence requires a frozen binding, executes without providers, persists all
  material definitions/inputs/hashes, and supports exact replay.

Every durable Check uses a versioned, hashed definition and registered bounded
evaluator. Source requirements separate provider-neutral fact meaning from
exact or deterministic-allowlist source binding and row provenance.
Unconstrained source selection is forbidden for evidence.

Dataset freezes facts and gaps. Indicator owns indicator state/readiness and
event emission across explicit gap policy. Check owns analytical eligibility,
outcomes, statistics, assertions, and optional verdict. Strategy owns trading
decisions, and the existing strict Strategy-bound Backtest contract remains
unchanged.

Legacy records remain byte-for-byte historical evidence and receive a derived
compatibility classification. They are never silently promoted.

## Consequences

Checks no longer need fake Strategies, and Backtests retain all Strategy
invariants. A generic event-and-fact Check can express provider comparisons by
configuration without a provider-specific Check family. Preparation may be
slower because durable work must explicitly freeze input, but exploratory
preview remains available.

Durable evidence now refuses dirty or source-mismatched execution. Container
images carry a build-time source-tree attestation; clean host execution verifies
Git HEAD and worktree state. Reports, dossiers, CLI, MCP, and orchestration can
render and sequence evidence but cannot calculate a parallel result.

## Rejected Alternatives

- Requiring a Strategy for every Check: confuses analysis with decisions.
- Allowing mutable-store Check results to become durable Observations: cannot
  guarantee exact replay.
- Adding provider-specific Check families: embeds source identity in semantics.
- Adding arbitrary expressions or caller Python: creates an unreviewed second
  research engine.
- Treating Dataset freeze as universal consumer certification: conflates known
  reality with consumer readiness.
- Adding a Campaign resource: orchestration objectives are not QT domain state.

## Enforcing Evidence

Contract, planning, frozen binding, evaluator, authority, adapter, gap, outcome,
legacy compatibility, replay, and Backtest regression tests cover the decision.
See [Check Evidence Boundary](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md).
