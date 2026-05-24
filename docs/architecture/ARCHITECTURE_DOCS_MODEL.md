---
component: architecture-docs-model
subsystem: architecture-docs
layer: reference
doc_type: architecture
status: active
tags:
  - documentation
  - architecture
  - audit
  - diagrams
  - boundaries
code_paths:
  - docs/architecture
  - scripts/docs/build_architecture_index.py
---
# Architecture Documentation Model

## Purpose

This document records the architecture-doc modernization pass. It explains what was kept, consolidated, rewritten, or removed so future docs do not drift back into package-shaped notes.

The target model is boundary-first. A systems engineer should be able to open one folder and understand the ownership boundary, upstream inputs, downstream outputs, truth source, projection rules, failure behavior, and implementation paths.

## Audit Result

The prior architecture tree had useful facts, but the shape was legacy:

- folders mirrored source packages more than system boundaries,
- several files were AI accretion logs with future/north-star language,
- BotLens, persistence, observability, and runtime execution were mixed together,
- signals were treated as their own subsystem even though they are decision-layer inputs,
- long files repeated contract material instead of explaining boundaries,
- diagrams were missing or not referenced by the docs that needed them.

The modernization keeps the useful current facts and removes the competing package-shaped docs.

## Consolidation Decisions

| Legacy Area | Classification | New Home | Decision |
| --- | --- | --- | --- |
| `providers/` and `market/` | useful, too implementation-shaped | `data/` | Consolidated into one data boundary covering providers, candles, instruments, cache, and gaps. |
| `indicators/` | useful, too component/tutorial shaped | `indicator-runtime/` | Rewritten as one runtime boundary. Indicator-specific deep dives are deferred. |
| `strategy/` and `signals/` | useful, conceptually split incorrectly | `decision-layer/` | Consolidated. Signals are decision-layer inputs, not a standalone architecture subsystem. |
| `runtime/` | useful, too large and mixed | `execution-runtime/`, `botlens-projections/`, `persistence/`, `observability/` | Split by truth owner and projection owner. |
| `storage/` | useful, mixed persistence and observability | `persistence/`, `observability/` | Consolidated into durable ledgers and operational diagnostics. |
| `reporting/` | useful, duplicated dataset/artifact details | `reporting/` | Reduced to the reporting boundary with RunResearchDataset as canonical. |
| `engine/` | useful but vague | `engine/` | Rewritten as the cross-cutting state-engine model. |
| root indexes | useful but stale | root docs | Rewritten to navigate boundaries and diagrams. |

## Target Tree

```text
docs/architecture/
  README.md
  ARCHITECTURE_DOCS_MODEL.md
  ARCHITECTURE_COMPONENT_INDEX.md
  decisions/
    README.md
    0001-use-boundary-first-architecture-docs.md
    ...
  system/
    SYSTEM_MODEL.md
    diagrams/
  engine/
    ENGINE_STATE_MODEL.md
    diagrams/
  identity/
    IDENTITY_AND_CORRELATION_BOUNDARY.md
    diagrams/
  data/
    DATA_BOUNDARY.md
    diagrams/
  indicator-runtime/
    INDICATOR_RUNTIME_BOUNDARY.md
    diagrams/
  decision-layer/
    DECISION_LAYER_BOUNDARY.md
    REGIME_CONTEXT_BOUNDARY.md
    diagrams/
  execution-runtime/
    EXECUTION_RUNTIME_BOUNDARY.md
    RUNTIME_COMPOSITION_ROOT.md
    WALLET_AND_CAPITAL_BOUNDARY.md
    diagrams/
  botlens-projections/
    BOTLENS_PROJECTION_BOUNDARY.md
    diagrams/
  persistence/
    PERSISTENCE_BOUNDARY.md
    diagrams/
  reporting/
    REPORTING_BOUNDARY.md
    diagrams/
  observability/
    OBSERVABILITY_BOUNDARY.md
    diagrams/
```

## Diagram Model

Every boundary folder that needs diagrams owns a local `diagrams/` folder. A diagram should clarify a boundary, lifecycle, event flow, state model, projection path, or identity relationship. It should not exist only for decoration.

Diagram sources are Mermaid `.mmd` files. SVG generation is deferred until Mermaid CLI is available in the environment.

## Entry Points

- [Architecture README](README.md) is the navigation hub.
- [System model](system/SYSTEM_MODEL.md) is the end-to-end architecture overview.
- [Engine state model](engine/ENGINE_STATE_MODEL.md) explains the deterministic walk-forward contract.
- [Architecture decision records](decisions/README.md) explain durable decisions and tradeoffs.
- [Architecture component index](ARCHITECTURE_COMPONENT_INDEX.md) is generated from frontmatter and should not be edited by hand.

## Deprecated Or Removed Docs

The following legacy docs were consolidated into boundary docs and are no longer canonical:

- old provider bootstrap and market readiness docs,
- old indicator authoring, overlay, state-engine, and market-profile stack docs,
- old strategy and signal pipeline docs,
- old runtime hub, runtime service, startup, event, sharding, wallet, configuration, and BotLens debugger docs,
- old storage runtime, BotLens observability persistence, and migration checklist docs,
- old reporting seam, artifact bundle, and dataset-specific architecture docs.

Important current facts from those files were preserved in the relevant boundary docs. Historical/north-star sections were intentionally not carried forward unless the code or platform contracts currently support them.

## Remaining Gaps

- Frontend operator surfaces beyond BotLens are not modeled as a dedicated architecture boundary yet.
- Paper/live runtime mode behavior is documented only as a composition seam because backtest is the implemented runtime shape today.
- Identity keys are spread across strategy, runtime events, database rows, and projection code. The new identity doc explains current relationships, but the code does not yet have a single identity registry.
- Provider session/calendar truth is still limited. Unknown gaps should remain unknown until explicit session evidence exists.
- SVG diagram assets are not generated until Mermaid tooling is available.

## Maintenance Rules

- Keep contracts authoritative.
- Prefer boundary docs over package docs.
- Keep docs short enough to read, but complete enough to debug ownership and flow.
- Use ADRs for durable cross-boundary decisions; keep operational or incident-specific narratives in incident docs.
- Link to source paths through frontmatter `code_paths`.
- Rebuild [ARCHITECTURE_COMPONENT_INDEX.md](ARCHITECTURE_COMPONENT_INDEX.md) after frontmatter changes.
