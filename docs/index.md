# Quant-Trad Documentation

Quant-Trad documentation is layered so readers do not have to start in deep architecture notes. Use this page as the portal, then move into contracts or component docs when you need source-of-truth detail.

## Start Here

- [Overview](overview.md): what Quant-Trad is, why it exists, and how the main layers fit together.
- [Getting started](getting-started.md): local setup, stack commands, endpoints, and common checks.
- [README](../README.md): short project portal.

## Core Concepts

- [Runtime timeline](concepts/runtime-timeline.md): the `initialize -> apply_bar -> snapshot` model, known-at timing, and why runtime snapshots are the source of truth.
- [Execution model](concepts/execution-model.md): FAST/FULL execution, pessimistic same-bar handling, intrabar fallback, and playback separation.
- [Strategies and signals](concepts/strategies-and-signals.md): how typed indicator outputs become strategy decisions.
- [BotLens](concepts/botlens.md): playback and runtime inspection as debugging surfaces.
- [Reporting datasets](concepts/reporting-datasets.md): RunResearchDataset v1 and report/compare views.

## Engineering Architecture

- [Architecture overview](engineering/architecture.md): backend, runtime, frontend, storage, and observability relationships.
- [Runtime engine](engineering/runtime-engine.md): concise runtime internals summary with links to deep design docs.
- [Data layer](engineering/data-layer.md): provider adapters, candle cache, sparse candles, and gap classification.
- [Observability](engineering/observability.md): logs, metrics, runtime events, BotLens diagnostics, Grafana, and Loki.
- [Security layer](architecture/security/SECURITY_LAYER.md): credential handling, trust boundaries, known gaps, and post-quantum risk points.
- [Engineering standards](engineering/README.md): testing, CI, observability doctrine, and documentation standards.

## Guides

- [Creating an indicator](guides/creating-an-indicator.md): minimal authoring checklist and example links.
- [Creating a strategy](guides/creating-a-strategy.md): typed-output decision flow and test expectations.
- [Adding a provider](guides/adding-a-provider.md): adapter, candle, cache, gap, and test expectations.
- [Binance futures public data setup](guides/binance-futures-public-data.md): target setup and ingestion contract for archive-backed USD-M perpetual research data.
- [Coinbase derivatives paper setup](guides/coinbase-derivatives-paper-setup.md): target setup for Coinbase product metadata, WebSockets, and provider-backed paper runs.

## Contracts

Contracts are normative. If code or explanatory docs disagree with these files, the contract wins until corrected.

- [Contracts README](contracts/README.md)
- [System contract](contracts/platform/00_system_contract.md)
- [Runtime contract](contracts/platform/01_runtime_contract.md)
- [Execution and playback contract](contracts/platform/02_execution_playback_contract.md)
- [Engineering contract](contracts/platform/03_engineering_contract.md)

## Deep Architecture

Deep architecture docs describe implementation boundaries and component-specific rules. They are not the first read for most contributors.

- [Architecture folder guide](architecture/README.md)
- [System architecture model](architecture/system/SYSTEM_MODEL.md)
- [Engine state model](architecture/engine/ENGINE_STATE_MODEL.md)
- [Architecture decision records](architecture/decisions/README.md)
- [Execution runtime boundary](architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Paper engine v1 design](architecture/execution-runtime/PAPER_ENGINE_V1_DESIGN.md)
- [BotLens projection boundary](architecture/botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [Reporting boundary](architecture/reporting/REPORTING_BOUNDARY.md)
- [Architecture component index](architecture/ARCHITECTURE_COMPONENT_INDEX.md)
- [Incident records](incidents/README.md)

## Status

Quant-Trad is in active development. Keep docs precise and honest:

- Document behavior that exists or is explicitly contracted.
- Link to deep notes instead of copying large sections.
- Keep unfinished areas as short caveats or deferred work, not broad tutorials.
- Update contracts and architecture docs in the same pass when runtime, provider, storage, reporting, or observability behavior materially changes.
