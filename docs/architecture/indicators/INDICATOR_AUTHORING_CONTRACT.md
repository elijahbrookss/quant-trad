---
component: indicator-authoring-contract
subsystem: indicators
layer: contract
doc_type: architecture
status: active
tags:
  - indicators
  - contract
  - runtime
  - overlays
code_paths:
  - src/engines/indicator_engine
  - src/indicators
  - src/strategies/evaluator.py
---
# Indicator Authoring Contract

This document defines the v1 indicator contract used by the rebuilt runtime indicator engine.

## Documentation Header

- `Component`: Indicator authoring and runtime integration contract
- `Owner/Domain`: Indicators / Runtime Contracts
- `Doc Version`: 2.0
- `Related Contracts`: [[00_system_contract]], [[01_runtime_contract]], [[BOT_RUNTIME_ENGINE_ARCHITECTURE]], `src/engines/indicator_engine/contracts.py`, `src/engines/indicator_engine/runtime_engine.py`

## 1) Problem and scope

Quant-Trad indicators are now authored around one public contract:

- indicators are computation units with internal state,
- typed outputs are the only strategy-visible truth surface,
- overlays are an optional chart-only surface owned by the indicator,
- the runtime engine executes indicators synchronously in dependency order.

In scope:
- manifest authoring,
- typed output authoring,
- optional indicator-owned overlay authoring,
- runtime execution rules shared by bot runtime and any future replay/preview consumers.

Out of scope:
- UI authoring,
- overlay transport/delta logic,
- any legacy signal-emitter or plugin-compatibility behavior.

## 2) Canonical model

```mermaid
flowchart LR
    A[Indicator internal state] --> B[apply_bar(bar, inputs)]
    B --> C[snapshot]
    B --> D[overlay_snapshot]
    C --> E[Typed outputs]
    D --> F[Canonical overlays]
    E --> G[Strategy evaluator]
    F --> H[BotLens / chart runtime]
```

The indicator owns both surfaces, but they remain separate:

- `snapshot()` publishes typed outputs for runtime truth,
- `overlay_snapshot()` publishes canonical chart overlays as the complete current overlay state for that bar, not a visual delta,
- strategies consume outputs only,
- overlay consumers consume overlays only.

## 3) Manifest contract

`IndicatorManifest` is the single source of truth for an indicator's public contract.

Required manifest fields:
- `id`
- `version`
- `dependencies`
- `outputs`

Optional manifest fields:
- `engine_factory`
- `evaluation_mode`
- `overlays`

Core runtime semantics:
- `id` is the unique strategy-visible instance id, not the class name,
- dependencies are indicator-level and reference published outputs,
- outputs are named and typed as exactly one of `signal`, `context`, or `metric`,
- overlays are named chart surfaces and are not part of typed output semantics.

## 4) Typed output contract

Every declared output must be returned every bar.

Runtime shape:
- `RuntimeOutput(bar_time, ready, value)`

Rules:
- outputs are never omitted,
- `ready=False` means unusable on this bar,
- the engine never waits, retries, or substitutes values,
- if any dependency output is not ready, all outputs of the dependent indicator must be `ready=False`.

Output type semantics:

### Signal
- shape: `{"events": [{"key": str}, ...]}`
- zero or more events per bar
- empty `events` is valid and distinct from `ready=False`

### Context
- shape: `{"state_key": str}` or `{"state_key": str, "fields": {...}}`
- `state_key` is the primary strategy-facing value

### Metric
- shape: flat numeric mapping such as `{"body_pct": 0.72, "range_pct": 0.41}`
- nested structures are intentionally rejected in v1

## 5) Overlay contract

Overlays are optional and chart-only.

Runtime shape:
- `RuntimeOverlay(bar_time, ready, value)`

Rules:
- overlays are declared in `manifest.overlays`,
- if overlays are declared, `overlay_snapshot()` must return exactly those names every bar,
- no extra overlays,
- no omitted overlays,
- `RuntimeOverlay.value` is already the canonical normalized chart payload,
- runtime and BotLens do visibility filtering, trimming, delta streaming, transport, and replay only,
- overlays are not strategy inputs and are not dependency inputs.

If any dependency output is not ready:
- all typed outputs must be `ready=False`,
- all declared overlays must be `ready=False`.

## 6) Indicator implementation contract

Indicators own internal state. The engine does not pass mutable state holders in v1.

Required methods:
- `apply_bar(bar, inputs)` mutates internal indicator state
- `snapshot()` returns all declared typed outputs

Optional method:
- `overlay_snapshot()` returns all declared overlays

Authoring rules:
- do not expose private state to strategies or runtime consumers,
- do not peek into another indicator's internal state,
- do not publish unrelated outputs from one composite indicator,
- if an indicator feels messy, split it at the computation boundary.

## 7) Composite indicators

Composite indicators are allowed when outputs come from one coherent shared state model.

Good example:
- `MarketProfileIndicator`
  - metric: `value_area_metrics`
  - context: `value_location`
  - context: `balance_state`
  - signal: `balance_breakout`
  - overlays: `value_area`, `breakout_markers`

Bad example:
- mixing value-area state, generic volatility stats, and unrelated trend regime outputs in one indicator.

The split rule is simple:
- one coherent domain model per indicator,
- multiple outputs are fine,
- unrelated domains are not.

## 8) Engine boundary

`IndicatorExecutionEngine` owns:
- manifest validation,
- dependency validation,
- topological ordering,
- per-bar execution,
- output validation,
- overlay validation,
- flattened output and overlay maps.

Per bar:
1. gather same-bar dependency outputs,
2. call `apply_bar(bar, inputs)`,
3. call `snapshot()`,
4. validate exact output presence and shape,
5. call `overlay_snapshot()`,
6. validate exact overlay presence and canonical overlay payload,
7. flatten outputs and overlays for downstream consumers.

The engine is synchronous and deterministic:
- no waiting,
- no retries,
- no async semantics,
- fail fast on ambiguity.

## 9) Strategy boundary

Strategies consume only the flattened typed output map:
- `indicator_id.output_name -> RuntimeOutput`

Supported v1 condition nodes:
- `signal_match`
- `context_match`
- `metric_match`
- `all`
- `any`
- `not`

Strategies do not:
- inspect indicator internals,
- inspect overlay payloads,
- rely on legacy signal emitter catalogs,
- use separate candle-stats or regime filter systems.

## 10) Anti-patterns

Reject immediately:
- reintroducing `signal_emitter`, `signal_rules`, or `signal_overlay_adapter` into the new core design,
- omitting outputs or overlays for a bar,
- defaulting values instead of publishing `ready=False`,
- using overlays as strategy inputs,
- reading private indicator state from outside the indicator,
- adding async/wait/retry semantics to the engine.
