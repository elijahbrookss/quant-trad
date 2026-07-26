# Quant-Trad Documentation

These docs are meant to help a competent engineer rebuild the system model in
their head without pretending they remember every detail. Start with the intent,
then drop into contracts or component notes only when the exact behavior
matters.

## Start Here

- [Overview](overview.md): the shortest explanation of what the system is trying
  to preserve.
- [Getting started](getting-started.md): local setup, stack commands, endpoints,
  and first checks.
- [README](../README.md): project portal and capability summary.

## Core Concepts

The concept pages explain the normal shape before the edge cases. Read these
when behavior feels surprising and you want to know what the system intended.

- [Runtime timeline](concepts/runtime-timeline.md): `initialize -> apply_bar -> snapshot`, known-at timing, and runtime truth.
- [Execution model](concepts/execution-model.md): FAST/FULL execution, pessimistic same-bar handling, intrabar fallback, and playback separation.
- [Strategies and signals](concepts/strategies-and-signals.md): how typed indicator outputs become strategy decisions.
- [BotLens](concepts/botlens.md): runtime inspection and playback as projection surfaces, not execution engines.
- [Reporting datasets](concepts/reporting-datasets.md): RunResearchDataset v1 and report/compare views.

## Engineering Architecture

Architecture docs explain ownership: what owns truth, what is only a view, and
which identifiers carry meaning across boundaries.

- [Architecture overview](engineering/architecture.md): backend, runtime, frontend, storage, and observability relationships.
- [Runtime engine](engineering/runtime-engine.md): concise runtime internals summary with links to deep design docs.
- [Data layer](engineering/data-layer.md): provider adapters, canonical candle intake, causal reads, provenance, and gap classification.
- [Observability](engineering/observability.md): logs, metrics, runtime events, BotLens diagnostics, Grafana, and Loki.
- [Security layer](architecture/security/SECURITY_LAYER.md): credential handling, trust boundaries, known gaps, and post-quantum risk points.
- [Engineering standards](engineering/README.md): testing, CI, observability doctrine, and documentation writing guidance.

## Guides

- [Creating an indicator](guides/creating-an-indicator.md): minimal authoring checklist and example links.
- [Creating a strategy](guides/creating-a-strategy.md): typed-output decision flow and test expectations.
- [Adding a provider](guides/adding-a-provider.md): adapter, explicit intake, known-at, provider-gap, and test expectations.
- [Binance futures public data setup](guides/binance-futures-public-data.md): target setup and ingestion contract for archive-backed USD-M perpetual research data.
- [Coinbase derivatives paper setup](guides/coinbase-derivatives-paper-setup.md): target setup for Coinbase product metadata, WebSockets, and provider-backed paper runs.

## Contracts

Contracts are normative. If code or explanatory docs disagree with these files,
the contract wins until corrected. Use them when you need the exact rule, not as
the first page for learning the system.

- [Contracts README](contracts/README.md)
- [System contract](contracts/platform/00_system_contract.md)
- [Runtime contract](contracts/platform/01_runtime_contract.md)
- [Execution and playback contract](contracts/platform/02_execution_playback_contract.md)
- [Engineering contract](contracts/platform/03_engineering_contract.md)

## Deep Architecture

Deep architecture docs are for the moments when the concept is clear but the
boundary matters. They should explain the current intent and source paths
without becoming a pile of disconnected requirements.

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
- Explain the system intent before listing exceptions.
- Link to deep notes instead of copying large sections across files.
- Keep unfinished areas as short caveats, not broad future tutorials.
- Update contracts and architecture docs in the same pass when runtime, provider, storage, reporting, or observability behavior materially changes.
