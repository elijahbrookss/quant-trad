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

This document defines the indicator authoring contract used by the rebuilt runtime indicator engine and the indicator create/edit surfaces.

## Documentation Header

- `Component`: Indicator authoring and runtime integration contract
- `Owner/Domain`: Indicators / Runtime Contracts
- `Doc Version`: 2.0
- `Related Contracts`: [[00_system_contract]], [[01_runtime_contract]], [[BOT_RUNTIME_ENGINE_ARCHITECTURE]], `src/engines/indicator_engine/contracts.py`, `src/engines/indicator_engine/runtime_engine.py`

## 1) Problem and scope

Quant-Trad indicators are now authored around one public contract:

- indicators are computation units with internal state,
- one authored `IndicatorManifest` is the full public contract for the indicator type,
- typed outputs are the only strategy-visible truth surface,
- overlays are an optional chart-only surface owned by the indicator,
- the runtime engine executes indicators synchronously in dependency order from a derived `IndicatorRuntimeSpec`,
- walk-forward runtime is the authoritative execution model for runtime-supported indicators across QuantLab, strategy preview, and BotLens.

In scope:
- manifest authoring,
- parameter authoring for create/edit flows,
- typed output authoring,
- optional indicator-owned overlay authoring,
- runtime execution rules shared by bot runtime and any future walk-forward/preview consumers.

Out of scope:
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

`IndicatorManifest` is the single source of truth for an indicator's full public contract.

Required authored manifest fields:
- `type`
- `version`
- `label`
- `description`

Optional authored manifest sections:
- `params`
- `outputs`
- `overlays`
- `dependencies`
- `runtime_inputs`

Core authored semantics:
- `type` is the stable indicator type id used by backend APIs and the frontend,
- `params` declare indicator-owned configuration only,
- `outputs` declare strategy-visible typed runtime outputs,
- `overlays` declare chart-visible overlay surfaces,
- `dependencies` declare logical indicator dependencies at the type level,
- `runtime_inputs` declare only source-data requirements such as source timeframe and lookback policy.

Frontend and backend create/edit flows read `IndicatorManifest.params` directly.
The frontend renders only params marked `editable=true`.
Execution context and orchestration metadata do not belong in manifest params.

Execution context examples:
- `symbol`
- `start`
- `end`
- `interval`
- `instrument_id`
- `datasource`
- `exchange`

QuantLab research execution must use a canonical instrument context before indicator runtime is allowed to execute.
In practice:
- chart/load flows discover or refresh the canonical instrument explicitly through the instrument validation seam,
- indicator runtime source requests fetch through the canonical candle service seam,
- and indicator runtime source fetches must not talk directly to provider persistence with an unresolved instrument context.

Orchestration metadata examples:
- `provider_id`
- `venue_id`
- `bot_id`
- `strategy_id`
- `run_id`

Those values are resolved outside indicator config and passed through runtime execution context or run/report artifacts as appropriate.

Support flags such as runtime availability are derived by backend wiring and are not authored in the manifest.

The runtime engine does not consume the full manifest. It consumes a derived `IndicatorRuntimeSpec` built from:
- `IndicatorManifest.outputs`
- `IndicatorManifest.overlays`
- resolved instance id
- resolved concrete dependency refs

This keeps one authored contract while preserving a narrow engine-facing execution contract.

Dependency semantics are split cleanly:
- the manifest declares dependency requirements by `indicator_type` + `output_name`,
- the persisted indicator instance stores explicit dependency bindings to concrete upstream indicator ids,
- create/edit flows must require the user to choose a concrete upstream instance when multiple compatible indicators exist,
- runtime consumers must never guess or auto-scan for a matching dependency when the instance binding is missing,
- and delete flows must block removal of an upstream indicator while any remaining indicator still binds to it.

## 4) Package layout

Each indicator package must provide:
- `manifest.py` with the authored `IndicatorManifest`
- `definition.py` with the indicator definition wrapper used by the registry

Typical package layout:

```text
indicators/<indicator_type>/
  manifest.py
  definition.py
  compute/
  runtime/
  overlays/
```

Rules:
- frontend/editor metadata must come from `manifest.py`,
- indicator construction and source-data request shaping must be owned by `definition.py`,
- compute/runtime implementations must consume the same declared parameter contract,
- signature introspection is not part of the canonical contract.

Definition methods are explicit:
- required: `resolve_config(params)`
- required for compute-supported indicators: `build_compute_data_request(execution_context, resolved_params)` and `build_compute_indicator(source_frame, execution_context, resolved_params)`
- required for runtime-supported indicators: `build_runtime_indicator(...)`
- optional for runtime-supported indicators that need alternate source data: `build_runtime_data_request(execution_context, resolved_params)`
- optional for runtime-supported indicators that need immutable precomputed inputs: `build_runtime_source_facts(source_frame, execution_context, resolved_params)`

This replaces the legacy `from_context(...)` construction pattern. Indicators do not fetch provider data directly through an ad hoc classmethod anymore.
Definitions shape requests only. The backend owns canonical source-data fetches and instrument resolution.
If a runtime-supported indicator needs source preprocessing, keep the seam explicit:
- `build_runtime_data_request(...)` shapes the alternate source-data request,
- backend fetches that source data through the canonical candle service,
- `build_runtime_source_facts(...)` derives immutable indicator-owned source facts,
- `build_runtime_indicator(...)` receives those facts and constructs the walk-forward runtime indicator.

`build_runtime_source_facts(...)` is the correct place for work like:
- Market Profile session/profile computation from `30m` candles,
- strategy-timeframe projection of profile boundaries,
- assignment of `formed_at` and `known_at` to those profiles.

It is not the place to build final chart-history overlay payloads.

`runtime_inputs` is intentionally narrow. It should not become a generic policy bucket for:
- alignment metadata,
- normalization metadata,
- runtime execution mode hints.

If an indicator needs special projection or timing semantics, that logic belongs inside the indicator seam. Market Profile is the canonical example:
- source profiles are built from `30m` data,
- merge policy is applied inside the indicator,
- resulting profiles are projected to the strategy timeframe inside the indicator runtime payload,
- `known_at` is gated to the closed strategy bar that can first observe the profile.

For dependent indicators, runtime graph construction must close over explicit upstream bindings before execution:
- QuantLab overlay requests,
- strategy preview,
- bot runtime,
- and signal preview
must all build one runtime engine graph from the requested root indicators plus their bound dependencies.

QuantLab overlay requests execute through the indicator worker queue rather than running walk-forward execution on the backend request loop. This preserves the same runtime graph semantics while keeping heavy walk-forward execution out of the API process.

Consumers may request one root indicator and return only that root indicator's overlays or signals, but execution still happens on the full dependency graph so runtime semantics remain identical across surfaces.

## 5) Typed output contract

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

## 6) Overlay contract

Overlays are optional and chart-only.

Runtime shape:
- `RuntimeOverlay(bar_time, ready, value)`

Rules:
- overlays are declared in `manifest.overlays`,
- if overlays are declared, `overlay_snapshot()` must return exactly those names every bar,
- no extra overlays,
- no omitted overlays,
- `RuntimeOverlay.value` is already the canonical normalized chart payload,
- indicators must maintain overlay state incrementally and must not rebuild history-length payloads inside `apply_bar()`,
- `overlay_snapshot()` should be a cheap read of current indicator-owned state,
- walk-forward/transport consumers may choose when to request overlays; they are not required to request them on every engine step,
- overlay chart routing is driven by overlay metadata including `pane_key`,
- `price` is the main candlestick pane, while auxiliary panes such as `volatility` and `oscillator` are declared once in shared pane registry infrastructure,
- panes should group overlays by unit and semantic family, so price-unit metrics and normalized oscillators must not share a pane,
- shared chart infrastructure activates and removes auxiliary panes from the set of pane keys present in the current overlay frame,
- adding a new pane-capable overlay should require only overlay registration metadata plus the indicator runtime payload,
- pane legends should be derived from pane registry metadata plus overlay `ui.label` / `ui_color`, not handwritten per indicator or chart,
- simple line/oscillator overlays should prefer shared overlay builders instead of hand-building `polylines` payload dictionaries inside indicators,
- runtime and BotLens do visibility filtering, trimming, delta streaming, transport, and replay only,
- overlays are not strategy inputs and are not dependency inputs.

If any dependency output is not ready:
- all typed outputs must be `ready=False`,
- all declared overlays must be `ready=False`.

## 7) Indicator implementation contract

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
- do not materialize full chart-history overlays inside `apply_bar()`,
- if a line or region overlay grows over time, append/advance indicator-owned overlay state incrementally and let `overlay_snapshot()` read that state when asked,
- if an indicator feels messy, split it at the computation boundary.

## 8) Composite indicators

Composite indicators are allowed when outputs come from one coherent shared state model.

Good example:
- `MarketProfileIndicator`
  - metric: `value_area_metrics`
  - context: `value_location`
  - context: `balance_state`
  - signal: `balance_breakout`
  - overlays: `value_area`

Bad example:
- mixing value-area state, generic volatility stats, and unrelated trend regime outputs in one indicator.

The split rule is simple:
- one coherent domain model per indicator,
- multiple outputs are fine,
- unrelated domains are not.

## 9) Engine boundary

`IndicatorExecutionEngine` owns:
- runtime spec validation,
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

The engine does not:
- infer parameter schemas,
- inspect UI metadata,
- construct indicators from signatures,
- treat product surfaces such as "preview" as separate computation modes,
- treat the engine-facing runtime spec as the authored public contract.

## 10) Strategy boundary

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

## 11) Anti-patterns

Reject immediately:
- reintroducing `signal_emitter`, `signal_rules`, or `signal_overlay_adapter` into the new core design,
- reintroducing constructor/signature-based parameter inference as a public contract,
- omitting outputs or overlays for a bar,
- defaulting values instead of publishing `ready=False`,
- using overlays as strategy inputs,
- reading private indicator state from outside the indicator,
- adding async/wait/retry semantics to the engine.
