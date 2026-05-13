---
component: system-architecture-model
subsystem: platform
layer: architecture
doc_type: architecture
status: active
tags:
  - system
  - runtime
  - contracts
  - projections
  - reporting
  - boundaries
code_paths:
  - src/engines/indicator_engine
  - src/strategies
  - src/engines/bot_runtime
  - src/data_providers
  - portal/backend
  - portal/frontend
  - docs/architecture/system/diagrams/system-runtime-truth-flow.mmd
---
# System Architecture Model

## Purpose

This is the top-level systems-engineering model for Quant-Trad. It explains how source market facts move through indicators, decision logic, execution, event storage, projections, reporting, and operator surfaces.

Canonical diagram source: [system-runtime-truth-flow.mmd](diagrams/system-runtime-truth-flow.mmd).

## System Thesis

Quant-Trad is a deterministic walk-forward runtime. The system is trustworthy only when every downstream artifact can be explained from what the runtime knew at the time.

```text
Data -> Indicators -> Decisions -> Execution -> Events -> BotLens / Reports
```

Reports, charts, fleet cards, exports, and narrative summaries are views. They do not create execution truth.

## Diagram Walkthrough

[system-runtime-truth-flow.mmd](diagrams/system-runtime-truth-flow.mmd) shows the platform as a truth pipeline:

1. Provider adapters and caches supply source candle and instrument facts.
2. Indicator runtime advances state and publishes typed outputs.
3. The decision layer evaluates strategy rules against typed outputs and bounded history.
4. Execution runtime applies deterministic ordering, FAST/FULL semantics, fees, margin, wallet, settlement, and trade lifecycle changes.
5. Runtime/domain events and trade rows become durable truth.
6. BotLens, reports, compare views, and frontend state project from durable/runtime truth.
7. Observability describes health and failure, but does not alter trading truth.

## Boundary Ownership

| Boundary | Owns | Must Not Own |
| --- | --- | --- |
| Data | Provider access, candles, instruments, cache, gap classification | Decisions or fills |
| Indicator runtime | Private indicator state, typed outputs, overlays, details | Strategy rule evaluation |
| Decision layer | Signals, guards, rules, decision artifacts, rejected reasons | Wallet, fills, settlement |
| Execution runtime | Ordering, FAST/FULL execution, fees, margin, wallet, lifecycle, events | UI playback semantics |
| Persistence | Durable ledgers and read-model support rows | Alternate execution reconstruction |
| BotLens projections | Debug/read models over runtime facts | Execution truth |
| Reporting | Research datasets, compare, exports, diagnostics, narrative summaries | Runtime mutation |
| Observability | Logs, metrics, diagnostics, fallback/degrade visibility | Domain truth |
| Frontend | Operator commands and inspection state | Canonical runtime state |

## Hot Path And Cold Path

Hot path:

```text
bar -> indicator snapshot -> decision artifact -> execution outcome -> runtime event
```

The hot path should carry bounded, typed payloads needed for execution, audit, and projection.

Cold path:

```text
event ledger / trade rows -> BotLens forensics -> reports -> compare -> export
```

Cold paths may page history, assemble heavy debug payloads, or produce analyst summaries. They must not feed back into execution for the same historical bar.

## Canonical Truth

Canonical truth includes:

- provider-backed candle facts,
- indicator typed outputs at their known-at time,
- strategy decision artifacts,
- accepted and rejected decision events,
- trade lifecycle rows,
- runtime/domain events,
- wallet, fee, margin, and settlement effects.

Projection/cache/view state includes:

- indicator overlays and details,
- BotLens selected-symbol projections,
- frontend status cards,
- report summaries,
- narrative insight text,
- observability dashboards.

Projection failure should produce unavailable/degraded state. It should not produce fabricated valid execution state.

## System Invariants

- All derived outputs respect `initialize -> apply_bar -> snapshot`.
- No artifact appears retroactively before its known-at time.
- Strategy logic consumes typed indicator outputs, not overlays or mutable indicator internals.
- Signals are decision-layer inputs and decision provenance, not an independent execution subsystem.
- Execution mode is separate from UI playback and animation.
- Domain events and stable IDs connect runtime truth to BotLens and reports.
- Heavy debug/history stays off the hot path.
- Contracts beat explanatory docs when they disagree.

## Related Docs

- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Identity and correlation boundary](../identity/IDENTITY_AND_CORRELATION_BOUNDARY.md)
- [Data boundary](../data/DATA_BOUNDARY.md)
- [Indicator runtime boundary](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md)
- [Decision layer boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)
- [Execution runtime boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)

## Known Gaps

- Frontend-wide architecture beyond BotLens needs a later operator-surface doc.
- Paper/live runtime mode details should be documented when their collaborators diverge from backtest behavior.
- SVG diagram generation is pending Mermaid tooling.
