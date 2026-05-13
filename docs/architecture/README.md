# Architecture Docs

This folder is the systems-engineering map for Quant-Trad.

The architecture docs are organized by conceptual boundary, not by the current Python or frontend package layout. A boundary doc explains what owns truth, what is a projection, which identifiers cross the boundary, what the upstream and downstream contracts are, and which source modules implement the behavior.

## Start Here

1. [System model](system/SYSTEM_MODEL.md) - the end-to-end truth flow.
2. [Engine state model](engine/ENGINE_STATE_MODEL.md) - the `initialize -> apply_bar -> snapshot` discipline and known-at causality.
3. [Identity and correlation boundary](identity/IDENTITY_AND_CORRELATION_BOUNDARY.md) - how `run_id`, `bot_id`, `strategy_hash`, `instrument_id`, `signal_id`, `decision_id`, and `trade_id` connect the system.
4. [Architecture documentation model](ARCHITECTURE_DOCS_MODEL.md) - audit, consolidation decisions, and remaining gaps.
5. [Architecture decision records](decisions/README.md) - compact backfilled decisions and tradeoffs.
6. [Architecture component index](ARCHITECTURE_COMPONENT_INDEX.md) - generated frontmatter/code-path index.

## Fastest Diagrams

Mermaid sources live in a `diagrams/` folder beside the boundary doc they support.

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
- [Wallet and capital flow](execution-runtime/diagrams/wallet-capital-flow.mmd)
- [BotLens projection flow](botlens-projections/diagrams/botlens-projection-flow.mmd)
- [Runtime event ledger flow](persistence/diagrams/runtime-event-ledger-flow.mmd)
- [RunResearchDataset flow](reporting/diagrams/run-research-dataset-flow.mmd)
- [Observability flow](observability/diagrams/observability-flow.mmd)

SVG outputs are not checked in yet because Mermaid CLI is not available in the current environment. The `.mmd` files are the diagram sources of record.

## Boundary Map

| Boundary | Canonical Entry | Owns |
| --- | --- | --- |
| System | [system/SYSTEM_MODEL.md](system/SYSTEM_MODEL.md) | End-to-end truth, projections, hot path, cold path |
| Engine | [engine/ENGINE_STATE_MODEL.md](engine/ENGINE_STATE_MODEL.md) | Known-at state-machine discipline across indicators, decisions, and runtime |
| Identity | [identity/IDENTITY_AND_CORRELATION_BOUNDARY.md](identity/IDENTITY_AND_CORRELATION_BOUNDARY.md) | Stable IDs, event correlation, lineage keys |
| Data | [data/DATA_BOUNDARY.md](data/DATA_BOUNDARY.md) | Providers, candles, instruments, cache, continuity, source gaps |
| Indicator runtime | [indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md](indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md) | Indicator state, typed outputs, overlays, detail projections |
| Decision layer | [decision-layer/DECISION_LAYER_BOUNDARY.md](decision-layer/DECISION_LAYER_BOUNDARY.md) | Strategy compilation/evaluation, signals, guards, decision artifacts |
| Regime context | [decision-layer/REGIME_CONTEXT_BOUNDARY.md](decision-layer/REGIME_CONTEXT_BOUNDARY.md) | Regime as indicator-produced decision context |
| Execution runtime | [execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md](execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md) | Deterministic execution, FAST/FULL semantics, lifecycle, events |
| Runtime composition | [execution-runtime/RUNTIME_COMPOSITION_ROOT.md](execution-runtime/RUNTIME_COMPOSITION_ROOT.md) | Backend runtime wiring and mode-aware collaborator selection |
| Wallet and capital | [execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md](execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md) | Capital reservation, fees, margin, settlement, shared-wallet ordering |
| BotLens projections | [botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md](botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md) | Debug/read models over runtime truth |
| Persistence | [persistence/PERSISTENCE_BOUNDARY.md](persistence/PERSISTENCE_BOUNDARY.md) | Durable ledgers, repositories, replay support |
| Reporting | [reporting/REPORTING_BOUNDARY.md](reporting/REPORTING_BOUNDARY.md) | RunResearchDataset, reports, compare, exports, diagnostics |
| Observability | [observability/OBSERVABILITY_BOUNDARY.md](observability/OBSERVABILITY_BOUNDARY.md) | Logs, metrics, diagnostics, Grafana/Loki surfaces |

## Decision Records

[Architecture decision records](decisions/README.md) capture the durable choices
behind the current boundary model. They are explanatory: contracts remain the
source of truth when there is disagreement.

## Reading Paths

For the runtime hot path:

`system -> engine -> data -> indicator-runtime -> decision-layer -> execution-runtime -> persistence -> botlens-projections/reporting`

For debugging a confusing trade:

`identity -> execution-runtime -> persistence -> botlens-projections -> observability -> reporting`

For adding extension points:

`data` for providers, `indicator-runtime` for indicators, and `decision-layer` for strategy rules.

## Rules

- Contracts under `docs/contracts/platform/` remain the source of truth.
- Boundary docs should prefer supported current behavior over future language.
- Projections are never execution truth.
- Heavy debug/history belongs on cold paths.
- Runtime hot-path payloads stay bounded and typed.
- Signals are documented inside the decision layer.
- After changing architecture frontmatter, run:

```bash
python3 scripts/docs/build_architecture_index.py
```

Then run:

```bash
make sync-docs
```
