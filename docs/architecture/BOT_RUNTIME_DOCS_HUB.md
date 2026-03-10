---
component: bot-runtime-docs-hub
subsystem: portal-runtime
layer: reference
doc_type: architecture
status: active
tags:
  - runtime
  - docs-hub
code_paths:
  - docs/architecture/BOT_RUNTIME_DOCS_HUB.md
---
# Bot Runtime Docs Hub

This note is the entry point for bot runtime architecture in Obsidian.

## Start Here

- [[BOT_RUNTIME_ENGINE_ARCHITECTURE]]: runtime package layout, lifecycle, runtime state, and read models.
- [[BOT_RUNTIME_SERVICE_ARCHITECTURE]]: API start/stop flow, docker runner, container runtime, and persistence boundaries.
- [[RUNTIME_COMPOSITION_ROOT]]: explicit portal runtime composition root and injected collaborator seams.
- [[BOT_RUNTIME_SYMBOL_SHARDING_ARCHITECTURE]]: symbol workers, shared wallet proxy, merged `view_state`, and degrade behavior.

## Deeper Contracts

- [[RUNTIME_EVENT_MODEL_V1]]: canonical append-only runtime event schema and causality rules.
- [[WALLET_GATEWAY_ARCHITECTURE]]: shared wallet projection, reservations, and capital safety.
- [[INSTRUMENT_CONTRACT_FUTURES_V1_READINESS]]: runtime readiness and execution profile prerequisites.

## Related Platform Contracts

- [[00_system_contract]]
- [[01_runtime_contract]]
- [[ENGINE_OVERVIEW]]
- [[SIGNAL_PIPELINE_ARCHITECTURE]]

## Ownership Rules

- Runtime execution semantics live in [[BOT_RUNTIME_ENGINE_ARCHITECTURE]].
- Service/container orchestration lives in [[BOT_RUNTIME_SERVICE_ARCHITECTURE]].
- Multi-symbol fanout and merge behavior live in [[BOT_RUNTIME_SYMBOL_SHARDING_ARCHITECTURE]].
- Event taxonomy does not get redefined elsewhere; it lives in [[RUNTIME_EVENT_MODEL_V1]].
- Wallet reservation and projection semantics do not get redefined elsewhere; they live in [[WALLET_GATEWAY_ARCHITECTURE]].

## Intentional Cleanup

Older BotLens/snapshot explainer notes that repeated or contradicted the current runtime have been removed so the docs spine stays small and current.


## Discovery Index

- [[ARCHITECTURE_COMPONENT_INDEX]] maps `component/subsystem/code_paths` tags to architecture docs for targeted updates.
