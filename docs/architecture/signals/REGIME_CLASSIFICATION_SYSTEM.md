---
component: regime-classification-system
subsystem: signals
layer: domain
doc_type: architecture
status: active
tags:
  - regime
  - classification
code_paths:
  - src/indicators/regime
  - src/engines/indicator_engine/runtime_engine.py
  - src/strategies/evaluator.py
  - portal/backend/service/market/regime_blocks.py
  - portal/backend/service/reports/artifacts.py
---
# Regime Classification System

## Purpose

This document explains how regime classification works now that regime is a normal indicator contract rather than a background stats pipeline.

## TL;DR

- `regime` is an indicator with a canonical `IndicatorManifest`.
- It computes on the same `initialize -> apply_bar -> snapshot` timeline as every other indicator.
- Strategies consume both a stable context output and a numeric metric output directly from runtime indicator state.
- BotLens consumes regime overlays emitted by the indicator itself.
- Report bundles capture regime output history from runtime frames; there is no dedicated `regime_stats` table.

## Core Semantics

### 1) Regime Is Indicator-Owned

- Regime classification logic lives under `src/indicators/regime`.
- The indicator declares its editable and non-editable params through its manifest.
- Runtime and preview consumers must honor the same config contract.

### 2) Strategy Consumption

- The indicator engine evaluates `regime` in dependency order.
- Strategies read typed outputs such as `regime.market_regime` and `regime.regime_metrics`.
- `market_regime` is the stable structural context surface used for state gating.
- `regime_metrics` exposes numeric structure strength and maturity fields such as trend/range/transition scores, score margin, directional efficiency, and bars-in-regime.
- Entry/exit logic should combine those outputs with other indicator outputs in the strategy rule pipeline, not by querying separate regime tables.

### 3) Overlay Consumption

- Regime overlays are emitted by the regime indicator.
- Bot runtime and BotLens only trim, transport, and render those overlays.
- Main context-regime boxes, block boundary lines, and marker cues all derive from the same canonical block segmentation pass.
- Dashed boundary lines mark block starts.
- `known_at` is represented by a distinct confirmation marker inside the committed block rather than by labeling the boundary itself.
- Visible text labels are deterministic projections of the committed context-regime block model: every committed regime block gets a label that explains that block, while `known_at` remains a separate confirmation cue.
- Transition boxes use the local block price envelope rather than a pane-centered stripe.
- Overlays project from `context_regime_state` and `context_regime_direction` only. They do not fall back to lower committed structure fields; missing context-regime projection data is a runtime error.
- The public context-regime vocabulary is explicit and directional: `trend_up`, `trend_down`, `range`, `transition_up`, `transition_down`, and `transition_neutral`.
- Visibility remains gated by runtime timing and `known_at`; no overlay rebuild path is authoritative.

### 4) Report / Analysis Consumption

- If report artifact capture is enabled, regime outputs are written into the run bundle under `series/.../indicators/`.
- If regime overlay capture is enabled, overlay payloads are written under `series/.../overlays/`.
- Post-run analysis should use those run-scoped artifacts, not a parallel DB projection.

## Regime Blocks

`portal/backend/service/market/regime_blocks.py` remains a domain utility for grouping consecutive regime states into display/debug-oriented blocks.

- It is not a persistence pipeline.
- It does not imply a `regime_blocks` table.
- It is a thin wrapper over the canonical block builder in `src/indicators/regime/blocks.py`.
- It can be used by view/report shaping layers when they need summarized contiguous periods from indicator-emitted regime states.

## Current Classification Model

- Regime now follows a cleaner four-layer design:
  1. evidence: score competing `trend`, `range`, and `transition` structure hypotheses from candle-stats inputs,
  2. transition policy: apply hysteresis, dwell, and reversal friction to commit a structural state,
  3. context regime: promote committed structure into a slower higher-order `context_regime` state machine intended for review and gating,
  4. trust semantics: expose whether the committed context regime is known, mature, and trustworthy enough for downstream gating.
- The context regime layer intentionally carries more meaning than the lower structural layer. It distinguishes directional transitions (`transition_up`, `transition_down`) from neutral degradation/handoff (`transition_neutral`) so local counter-moves do not need to masquerade as full trend regimes.
- The engine computes normalized evidence from inputs such as directional efficiency, overlap, slope stability, range contraction, and range position.
- The stabilizer confirms structure separately from secondary axes, then promotes that structure into `context_regime` with its own dwell, known-at, maturity, and recent-switch tracking.
- `market_regime.state_key` is the explicit context-regime state. Trust and maturity are exposed as separate fields rather than collapsing `state_key` back to a coarser fallback state.
- Overlays intentionally review `context_regime`, not lower committed structure. If you want the noisier structural diagnostic layer, inspect the typed fields rather than the price overlay.
- Secondary axes (`volatility`, `liquidity`, `expansion`) remain useful contextual lenses, but they do not define the primary structural block model.

## Debug Checklist

If regime looks stale or wrong:

1. Confirm the `regime` indicator is actually attached to the active series.
2. Inspect both `market_regime` and `regime_metrics` on the affected bar, not an external table.
3. Inspect overlay payloads only after confirming the underlying typed outputs are correct.
4. Check the run artifact bundle for captured regime output history if the question is post-run analysis.
