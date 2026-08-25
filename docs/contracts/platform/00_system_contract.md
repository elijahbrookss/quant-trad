# System Contract

## Purpose

Define non-negotiable platform behavior shared by QuantLab, Strategy preview, Bot runtime, and Playback.

## Core Invariants

1. Live-equivalent evaluation: behavior is valid under sequential candle arrival.
2. Layer integrity: research, decision, and execution responsibilities stay separated.
3. Known-at causality: derived artifacts are usable when `known_at <= evaluation_time`.
4. Determinism: fixed inputs/params/versions produce stable outputs.
5. Explainability: artifacts and decisions must be auditable.

## Layer Ownership

- QuantLab: research and indicator exploration.
- Strategy: decision logic from indicator outputs.
- Bot: execution realism, fills, risk, costs, and lifecycle outcomes.

## Operator-Surface Authority

Platform, runtime, and service contracts own lifecycle, market-data, collector,
research, report, strategy, and execution truth. Operator and browser surfaces
own presentation, browser-local interaction state, and composition of typed read
models only.

The Frontend V2 primary rooms, Overview and Operations, are read-only. Their
adapters use GET, SSE, or equivalent read contracts and must not start runs,
mutate research or strategies, operate collectors, or write lifecycle state. A
displayed command remains copy-only unless an independently owned operator
surface explicitly executes it.

Frontend code must render lifecycle, health, readiness, coverage, and diagnostic
states emitted by their owning backend contracts. It must not infer those states
from cache presence, enabled flags, recent timestamps, database-shaped payloads,
provider responses, or locally joined subsystem data. Empty, partial, stale,
unavailable, invalid, and failed reads remain distinct visible states.

Where an operator mutation is separately supported, the surface invokes the
owning backend action contract and remains non-authoritative. Confirmation in
the UI does not bypass server-side admission, preconditions, fencing,
idempotency, audit, or authorization.

Collector lifecycle is owned exclusively by `CollectorOperationsService`.
Frontend, CLI, and MCP consumers must not derive lifecycle from collector
tables, mutate those tables directly, create executable collector definitions,
or introduce provider-specific operational behavior.

Dormant or unsupported mutation paths fail loud. A frontend route, control, or
locally cached object cannot become product authority merely because it exists.

## Userflow Continuity

Artifacts move forward semantically:
indicator outputs -> strategy decisions -> bot execution -> playback inspection.
