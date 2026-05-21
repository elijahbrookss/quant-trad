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

## Userflow Continuity

Artifacts move forward semantically:
indicator outputs -> strategy decisions -> bot execution -> playback inspection.
