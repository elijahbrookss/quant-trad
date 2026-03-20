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
- Strategies consume regime outputs directly from runtime indicator state.
- BotLens consumes regime overlays emitted by the indicator itself.
- Report bundles capture regime output history from runtime frames; there is no dedicated `regime_stats` table.

## Core Semantics

### 1) Regime Is Indicator-Owned

- Regime classification logic lives under `src/indicators/regime`.
- The indicator declares its editable and non-editable params through its manifest.
- Runtime and preview consumers must honor the same config contract.

### 2) Strategy Consumption

- The indicator engine evaluates `regime` in dependency order.
- Strategies read typed outputs such as `regime.market_regime`.
- Entry/exit logic should combine regime output with other indicator outputs in the strategy rule pipeline, not by querying separate regime tables.

### 3) Overlay Consumption

- Regime overlays are emitted by the regime indicator.
- Bot runtime and BotLens only trim, transport, and render those overlays.
- Visibility remains gated by runtime timing and `known_at`; no overlay rebuild path is authoritative.

### 4) Report / Analysis Consumption

- If report artifact capture is enabled, regime outputs are written into the run bundle under `series/.../indicators/`.
- If regime overlay capture is enabled, overlay payloads are written under `series/.../overlays/`.
- Post-run analysis should use those run-scoped artifacts, not a parallel DB projection.

## Regime Blocks

`portal/backend/service/market/regime_blocks.py` remains a domain utility for grouping consecutive regime states into display/debug-oriented blocks.

- It is not a persistence pipeline.
- It does not imply a `regime_blocks` table.
- It can be used by view/report shaping layers when they need summarized contiguous periods from indicator-emitted regime states.

## Debug Checklist

If regime looks stale or wrong:

1. Confirm the `regime` indicator is actually attached to the active series.
2. Inspect the indicator runtime output on the affected bar, not an external table.
3. Inspect overlay payloads only after confirming the underlying indicator output is correct.
4. Check the run artifact bundle for captured regime output history if the question is post-run analysis.
