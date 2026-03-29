---
component: indicator-state-engine-design-guide
subsystem: indicators
layer: architecture
doc_type: architecture
status: active
tags:
  - indicators
  - runtime
  - overlays
  - state-engine
  - known-at
code_paths:
  - src/indicators/candle_stats
  - src/indicators/market_profile
  - src/indicators/regime
  - src/engines/indicator_engine
---
# Indicator State Engine Design Guide

## Purpose

This guide captures the practical indicator design pattern that emerged from `candle_stats`, `market_profile`, and `regime`.

Use it when authoring a new runtime indicator or refactoring an existing one into the canonical walk-forward model.

This is a companion to [INDICATOR_AUTHORING_CONTRACT.md](docs/architecture/indicators/INDICATOR_AUTHORING_CONTRACT.md). The authoring contract defines what indicators must expose. This guide defines how indicators should usually be structured internally to get the most out of the runtime architecture.

## Core Pattern

Quant-Trad indicators should usually be built as one canonical state engine with five explicit layers:

1. source facts or dependencies
2. evidence or feature derivation
3. committed state or signal policy
4. trust / maturity semantics when decisions depend on the state
5. overlay projection

Those layers should all live on the same runtime timeline:

`initialize -> apply_bar -> snapshot -> overlay_snapshot`

The main design rule is simple:

- one indicator owns one authoritative internal timeline
- typed outputs are the strategy/runtime truth surface
- overlays are indicator-owned visual read models of that same state

Do not split the same indicator meaning across separate reconstruction paths.

## What The Good Examples Share

### Candle Stats

`candle_stats` is the clean feature-engine example.

- It owns rolling bar history and derived statistics in [runtime.py](/home/elijah/dev/quant-trad/src/indicators/candle_stats/runtime.py).
- It publishes flat metric outputs as the truth surface.
- Its ATR overlays are cheap reads of incrementally maintained points.

What to copy:

- keep metrics flat and typed
- maintain rolling history inside the indicator
- keep overlays lightweight and incremental

### Market Profile

`market_profile` is the clean source-facts + structural-state example.

- It precomputes immutable source facts outside the per-bar runtime loop, then consumes them through one walk-forward runtime indicator in [typed_indicator.py](/home/elijah/dev/quant-trad/src/indicators/market_profile/runtime/typed_indicator.py).
- It derives profile state, balance state, and signals from one authoritative runtime step.
- It treats `formed_at` / `known_at` semantics as part of the product, not chart polish.

What to copy:

- separate immutable source facts from per-bar state mutation
- make structural state and signal emission come from one runtime step
- model known-at timing explicitly

### Regime

`regime` is the clean evidence + transition policy + trust example.

- It computes structural evidence in [engine.py](/home/elijah/dev/quant-trad/src/indicators/regime/engine.py).
- It commits state and hysteresis in [stabilizer.py](/home/elijah/dev/quant-trad/src/indicators/regime/stabilizer.py).
- It exposes a safer actionable context surface plus raw metrics in [runtime.py](/home/elijah/dev/quant-trad/src/indicators/regime/runtime.py).
- Its overlays are derived from committed blocks, not raw local score noise.

What to copy:

- separate evidence from committed state
- separate committed state from trustworthy/actionable state
- let overlays show the committed timeline, not every intermediate candidate

## Recommended Internal Shape

For most non-trivial indicators, prefer this internal layout:

```text
indicator/
  manifest.py
  definition.py
  runtime.py or runtime/
  engine.py          # evidence / math / feature competition
  state.py           # canonical runtime state model
  signals.py         # if signal payload shaping is non-trivial
  overlays.py        # canonical overlay read model
```

You do not need every file for every indicator. The point is separation of responsibilities, not package ceremony.

Use these seams when the pressure is real:

- `engine.py`: evidence, scores, raw derived facts
- `state.py`: canonical runtime state containers and transitions
- `signals.py`: event payload shaping when signals are complex
- `overlays.py`: rendering payload assembly from committed runtime state only

## Authoring Principles

### 1) Build Indicators As State Engines

Do not think of indicators as helpers that recompute a full answer every bar.

Think of them as:

- state owned by the indicator
- mutated once per bar
- observed through typed outputs and overlays

That keeps runtime, strategy preview, BotLens, and replay aligned.

### 2) Prefer Typed Outputs By Intent

Use output types intentionally:

- `metric` for raw numeric evidence or drivers
- `context` for committed strategy-facing state
- `signal` for event emission

Do not overload one output with every concern.

Good pattern:

- one stable context output
- one numeric metric output
- one or more signal outputs if the indicator emits events

### 3) Separate Evidence From Policy

If the indicator contains scoring or raw pattern evidence, keep that separate from the logic that commits a public state.

Examples:

- raw trend/range scores are not the same thing as committed regime
- a value-area touch is not the same thing as a discretionary retest signal
- raw rolling ATR is not the same thing as a volatility state

This separation is where most trust problems are solved.

### 4) Model Trust Explicitly When The Indicator Gates Decisions

If strategies or bots will consume the indicator as a filter, do not force them to infer trust from raw internal details.

Instead expose trust semantics directly, such as:

- `known_at_epoch`
- `age_since_known_bars`
- `bars_in_regime`
- `recent_switch_count`
- `is_known`
- `is_mature`
- `is_trustworthy`
- `trust_score`

This is especially important for structural indicators like regime and market profile.

### 5) Overlays Must Be Read Models, Not Alternate Truth

Overlays should answer:

- what does the indicator want the chart to show now?

They should not define state semantics independently.

Rules:

- build overlays from committed indicator-owned state
- do not reconstruct alternate history in the overlay layer
- do not make the chart infer logic the indicator never published
- if consumers need more information, extend the runtime payload

### 6) Known-At Is A Product Surface

If an artifact would not exist yet in live trading, it must not exist yet in the indicator.

That applies to:

- profiles
- regime blocks
- breakouts
- reclaims
- retests
- any state that requires persistence or confirmation

Known-at timing should be visible in the runtime truth surface, not just implied in docs.

## Practical Design Checklist

When authoring a new indicator, ask these questions:

1. What are the true source facts?
2. What internal evidence is derived from those facts?
3. What public state should be committed from that evidence?
4. Does strategy need the raw evidence, the committed state, or both?
5. Does strategy need trust/maturity metadata?
6. What should the chart show from that committed state?
7. What must be gated by known-at timing?

If those answers are not clear, the indicator design is not ready yet.

## Anti-Patterns To Avoid

Avoid these recurring mistakes:

- rebuilding full chart-history overlays inside `apply_bar()`
- mixing raw evidence and public state in one opaque payload
- making strategies depend on overlay artifacts
- emitting visually strong annotations for weak or candidate states
- allowing the chart layer to invent semantics the engine does not compute
- hiding trust/maturity inside threshold folklore instead of explicit fields
- using alternate reconstruction paths for the same indicator meaning

## A Good Default Template

For most serious indicators in this codebase, the default target should be:

- one manifest declaring params, outputs, overlays, and dependencies
- one runtime indicator owning incremental state
- one evidence layer for raw features or scores
- one transition/signal policy layer for committed state
- one typed context output for strategy truth
- one typed metric output for evidence and debugging
- one overlay layer that reads committed state only

That template is simple enough for early work and strong enough to scale into bot/runtime use.

## When To Go Simpler

Not every indicator needs the full pattern.

If the indicator is just a rolling metric family like `candle_stats`, simpler is better:

- metric output
- minimal internal history
- lightweight overlays only if they add real value

Do not add a trust layer or transition state machine unless the indicator actually makes structural commitments or decision-driving claims.

## When To Go Richer

Use the richer pattern when the indicator:

- produces structural states
- emits event semantics that traders will reason about discretionarily
- will be used for strategy gating
- needs known-at correctness to preserve trust

That is the category where `market_profile` and `regime` live.

## Practical Rule Of Thumb

If the indicator can change what a strategy is allowed to do, it should usually expose:

- evidence
- committed state
- trust or maturity

If it cannot change what a strategy is allowed to do and only provides context, flat metrics may be enough.

## Summary

The most effective Quant-Trad indicators follow one pattern:

- one canonical state engine
- explicit separation between evidence and commitment
- explicit known-at semantics
- typed outputs for truth
- overlays as cheap read models of that same truth

That is the path that lets indicators stay explainable, replay-safe, and strategy-safe as the platform grows.
