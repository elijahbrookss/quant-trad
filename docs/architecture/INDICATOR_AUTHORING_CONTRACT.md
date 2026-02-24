# Indicator Authoring Contract

This document defines how to add or modify indicators without creating drift.

## Purpose

Every indicator must produce consistent outputs across:
- QuantLab overlays
- Indicator signals
- Strategy preview
- Bot runtime
- Playback

Consistency requirement:
- All derived outputs must come from one runtime timeline:
`initialize -> apply_bar -> snapshot`

## Required Structure

Each indicator should own its behavior in one module tree:

- `src/indicators/<name>/indicator.py`
- `src/indicators/<name>/state_engine.py` (if custom runtime state behavior is needed)
- `src/indicators/<name>/signals/`
- `src/indicators/<name>/overlays/`
- `src/indicators/<name>/plugin.py`

Shared runtime contracts and orchestration live in:
- `src/engines/indicator_engine/`

## Registration Contract

Use a single manifest registration point in `plugin.py`:
- `@indicator_plugin_manifest(...)`

Manifest is the source of truth for:
- `indicator_type`
- `engine_factory`
- `evaluation_mode`
- `signal_emitter`
- `overlay_projector`
- `signal_overlay_adapter`

Do not register indicator logic through alternate decorator discovery paths.

## Snapshot-First Rules

1. Signals and overlays must read from snapshot payload.
2. If a required field is missing from snapshot payload, extend snapshot contract.
3. Do not read mutable engine internals from outside engine/state logic.
4. Missing required snapshot data must fail loud with actionable context.

## Runtime Signal Semantics (Canonical)

Signals are runtime per-bar only:
- `signal_emitter(snapshot_payload, candle, previous_candle)`

Batch/research signal generation is legacy and must not be used for platform behavior.

Any consumer needing signals must use runtime snapshot semantics so strategy preview,
bot runtime, overlays, and playback remain aligned.

## Logging Contract

At minimum, indicator logs should include when available:
- `indicator_id`, `indicator_type`, `indicator_version`
- `symbol`, `timeframe`
- `bar_time` / `known_at`
- `strategy_id`, `run_id`, `bot_id` (in runtime contexts)

Log lifecycle boundaries, not per-candle noise by default.

## No-Fallback Policy

For each artifact class (signals, overlays, projections):
- one canonical computation path
- one canonical contract
- no hidden fallback reconstruction paths

If data is invalid or missing:
- fail loud
- include IDs/context
- do not silently patch or substitute

## Author Checklist (Use Before Merge)

1. Indicator has a single `plugin.py` manifest.
2. Signal/overlay logic is indicator-local (not in shared engine modules).
3. Runtime outputs are derived from snapshot payload only.
4. Required snapshot fields are explicit and validated.
5. No alternate registration/discovery path was introduced.
6. Logs are structured and include correlation context.
7. `py_compile` (or tests) passes for touched modules.
8. Strategy preview and bot runtime use the same underlying indicator semantics.

## Anti-Patterns (Reject)

- Indicator business logic inside shared engine package.
- Multiple registration systems for the same indicator behavior.
- Separate overlay/signal timelines with different source semantics.
- Silent fallback logic that masks contract violations.
