# Architecture Docs

This folder is the systems-engineering map for Quant-Trad.

The architecture docs are organized by conceptual boundary, not by the current Python or frontend package layout. A boundary doc explains what owns truth, what is a projection, which identifiers cross the boundary, what the upstream and downstream contracts are, and which source modules implement the behavior.

## Start Here

1. [Current system](../current-system.md) - the plain-language product, flow,
   promises, checks, and current limits.
2. [System model](system/SYSTEM_MODEL.md) - the end-to-end truth flow.
3. [Engine state model](engine/ENGINE_STATE_MODEL.md) - the `initialize -> apply_bar -> snapshot` discipline and known-at causality.
4. [Identity and correlation boundary](identity/IDENTITY_AND_CORRELATION_BOUNDARY.md) - how `run_id`, `bot_id`, `strategy_hash`, `instrument_id`, `signal_id`, `decision_id`, and `trade_id` connect the system.
5. [Architecture decision records](decisions/README.md) - durable decisions and tradeoffs.
6. [Security layer](security/SECURITY_LAYER.md) - credential handling, trust boundaries, known gaps, and PQ risk points.
7. [Architecture component index](ARCHITECTURE_COMPONENT_INDEX.md) - generated frontmatter/code-path index.

## Fastest Diagrams

Mermaid sources live in a `diagrams/` folder beside the boundary doc they
support. They are not decoration; keep them for flows, clocks, and boundaries
that are easier to understand at a glance than in prose.

- [System runtime truth flow](system/diagrams/system-runtime-truth-flow.mmd)
- [Engine boundaries](engine/diagrams/engine-boundaries.mmd)
- [Engine known-at timeline](engine/diagrams/engine-known-at-timeline.mmd)
- [Identity key relationships](identity/diagrams/identity-key-relationships.mmd)
- [Data boundary flow](data/diagrams/data-boundary-flow.mmd)
- [Candle continuity flow](data/diagrams/candle-continuity-flow.mmd)
- [Indicator runtime contract](indicator-runtime/diagrams/indicator-runtime-contract.mmd)
- [Indicator surfaces](indicator-runtime/diagrams/indicator-surfaces.mmd)
- [Decision flow](decision-layer/diagrams/decision-flow.mmd)
- [Signal consumption contract](decision-layer/diagrams/signal-consumption-contract.mmd)
- [Runtime hot path](execution-runtime/diagrams/runtime-hot-path.mmd)
- [Runtime lifecycle state](execution-runtime/diagrams/runtime-lifecycle-state.mmd)
- [Resolved execution context](execution-runtime/diagrams/resolved-execution-context.mmd)
- [Canonical order lifecycle](execution-runtime/diagrams/canonical-order-lifecycle.mmd)
- [Wallet and capital flow](execution-runtime/diagrams/wallet-capital-flow.mmd)
- [BotLens projection flow](botlens-projections/diagrams/botlens-projection-flow.mmd)
- [Runtime event ledger flow](persistence/diagrams/runtime-event-ledger-flow.mmd)
- [RunResearchDataset flow](reporting/diagrams/run-research-dataset-flow.mmd)
- [Observability flow](observability/diagrams/observability-flow.mmd)

Render SVGs with:

```bash
make architecture-svgs
```

Generated architecture SVGs are written beside the `.mmd` files in each local
`diagrams/` folder. The `.mmd` files remain the diagram sources of record; the
SVGs are the quick-reference files to open while reading nearby docs.

Repository-wide asset lineage, including retained files with unknown sources,
is recorded in [documentation asset lineage](../assets/README.md).

## Boundary Map

| Boundary | Canonical Entry | Owns |
| --- | --- | --- |
| System | [system/SYSTEM_MODEL.md](system/SYSTEM_MODEL.md) | End-to-end truth, projections, hot path, cold path |
| Engine | [engine/ENGINE_STATE_MODEL.md](engine/ENGINE_STATE_MODEL.md) | Known-at state-machine discipline across indicators, decisions, and runtime |
| Identity | [identity/IDENTITY_AND_CORRELATION_BOUNDARY.md](identity/IDENTITY_AND_CORRELATION_BOUNDARY.md) | Stable IDs, event correlation, lineage keys |
| Data | [data/DATA_BOUNDARY.md](data/DATA_BOUNDARY.md) | Providers, candles, instruments, cache, continuity, source gaps |
| CLI setup | [cli/CLI_SETUP_BOUNDARY.md](cli/CLI_SETUP_BOUNDARY.md) | Local readiness, provider onboarding, and operator-facing command composition |
| Indicator runtime | [indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md](indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md) | Indicator state, typed outputs, overlays, detail projections |
| Decision layer | [decision-layer/DECISION_LAYER_BOUNDARY.md](decision-layer/DECISION_LAYER_BOUNDARY.md) | Strategy compilation/evaluation, signals, guards, decision artifacts |
| Regime context | [decision-layer/REGIME_CONTEXT_BOUNDARY.md](decision-layer/REGIME_CONTEXT_BOUNDARY.md) | Regime as indicator-produced decision context |
| Execution runtime | [execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md](execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md) | Deterministic execution, FAST/FULL semantics, lifecycle, events |
| Economic execution | [execution-runtime/ECONOMIC_EXECUTION_CONTRACT.md](execution-runtime/ECONOMIC_EXECUTION_CONTRACT.md) | Immutable claim intent, versioned bar assumptions, X0-X2 evidence, cost stress |
| Venue-neutral execution context | [execution-runtime/VENUE_NEUTRAL_EXECUTION_CONTEXT.md](execution-runtime/VENUE_NEUTRAL_EXECUTION_CONTEXT.md) | Immutable instrument/venue/fee/model contracts, run pinning, conformance, fill evidence |
| Canonical order lifecycle | [execution-runtime/DURABLE_CANONICAL_ORDER_LIFECYCLE.md](execution-runtime/DURABLE_CANONICAL_ORDER_LIFECYCLE.md) | Immutable order/attempt identity, append-only transitions, residuals, replacement lineage, replay and projections |
| Replay-certified book execution | [execution-runtime/REPLAY_CERTIFIED_BOOK_EXECUTION.md](execution-runtime/REPLAY_CERTIFIED_BOOK_EXECUTION.md) | Historical book-tape replay, execution-quality limits, and X3 evidence |
| Passive queue and latency bounds | [execution-runtime/PASSIVE_QUEUE_BOUNDS_AND_LATENCY.md](execution-runtime/PASSIVE_QUEUE_BOUNDS_AND_LATENCY.md) | Queue-position and latency semantics, explicit trust ceilings, and X4-X5 evidence |
| Runtime composition | [execution-runtime/RUNTIME_COMPOSITION_ROOT.md](execution-runtime/RUNTIME_COMPOSITION_ROOT.md) | Backend runtime wiring and mode-aware collaborator selection |
| Wallet and capital | [execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md](execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md) | Capital reservation, fees, margin, settlement, shared-wallet ordering |
| BotLens projections | [botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md](botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md) | Debug/read models over runtime truth |
| Operator console | [frontend/OPERATOR_CONSOLE_V2.md](frontend/OPERATOR_CONSOLE_V2.md) | Bounded human inspection over backend read models, not alternate workflow truth |
| Persistence | [persistence/PERSISTENCE_BOUNDARY.md](persistence/PERSISTENCE_BOUNDARY.md) | Durable ledgers, repositories, replay support |
| Reporting | [reporting/REPORTING_BOUNDARY.md](reporting/REPORTING_BOUNDARY.md) | RunResearchDataset, reports, compare, exports, diagnostics |
| Research orchestration | [research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md](research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md) | API-backed CLI and future agent research workflows |
| Research memory | [research-memory/RESEARCH_MEMORY_BOUNDARY.md](research-memory/RESEARCH_MEMORY_BOUNDARY.md) | Observations, hypotheses, studies, and linked check evidence as a reasoning trail, not runtime truth |
| Security | [security/SECURITY_LAYER.md](security/SECURITY_LAYER.md) | Credential references, trust boundaries, hardening backlog |
| Observability | [observability/OBSERVABILITY_BOUNDARY.md](observability/OBSERVABILITY_BOUNDARY.md) | Logs, metrics, diagnostics, Grafana/Loki surfaces |

## Decision Records

[Architecture decision records](decisions/README.md) capture the durable choices
behind the current boundary model. They are explanatory: contracts remain the
source of truth when there is disagreement.

Use an ADR when a durable choice changes lifecycle truth, accounting, known-at
semantics, execution or exit behavior, dataset identity or quality, async
ownership, agent authority, recovery, security, or external-order boundaries.
An accepted ADR records the decision and rationale; changing a platform
contract still requires an explicit contract update.

## Reading Paths

For the runtime hot path:

`system -> engine -> data -> indicator-runtime -> decision-layer -> execution-runtime -> persistence -> botlens-projections/reporting`

For debugging a confusing trade:

`identity -> execution-runtime -> persistence -> botlens-projections -> observability -> reporting`

For adding extension points:

`data` for providers, `indicator-runtime` for indicators, and `decision-layer` for strategy rules.

## Rules

- Contracts under `docs/contracts/platform/` remain the source of truth.
- Follow the
  [component documentation standard](../engineering/documentation/component-documentation-standard.md)
  for required frontmatter and readable boundary documentation.
- The generated component index is a navigation aid. Shared `code_paths` are
  coverage links, not exclusive file ownership.
- Boundary docs should prefer supported current behavior over future language.
- Diagrams should be budgeted. Add one only when it makes the intent easier to
  see than prose alone.
- Projections are never execution truth.
- Heavy debug/history belongs on cold paths.
- Runtime hot-path payloads stay bounded and typed.
- Signals are documented inside the decision layer.
- ADRs record durable architectural or safety choices, not routine file moves
  or mechanical refactors.
- Accepted ADRs identify context, decision, invariants, consequences, rejected
  alternatives, and the tests or evidence that protect the decision.
- After changing architecture frontmatter, run:

```bash
python3 scripts/docs/build_architecture_index.py
```

Then run:

```bash
make sync-docs
```

## Retained Documentation History

[Architecture documentation model](ARCHITECTURE_DOCS_MODEL.md) preserves the
earlier boundary-first modernization record. It is useful when investigating
why files were consolidated, but it is not required reading for understanding
the current system.
