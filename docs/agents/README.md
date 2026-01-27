# Quant-Trad — Agent Context & System Contract

This directory defines the **non-negotiable behavioral contract** of the Quant-Trad engine.

These documents do not describe features.
They describe **how the system must behave** in order to remain correct, explainable, and tradable.

If code or agent behavior conflicts with these documents, the implementation is wrong — even if it appears to work.

---

## How to Read These Docs

Agents and contributors MUST read these documents in order:

1. **00_core_principles.md**  
   The foundation: how Quant-Trad thinks about time, correctness, and determinism.

2. **01_layer_model.md**  
   Defines QuantLab vs Strategy vs Bot. This separation is strict.

3. **02_temporal_rules.md**  
   Walk-forward constraints, known-at semantics, and timing correctness.

4. **03_artifact_lifecycle.md**  
   How derived structures (indicators, regimes, profiles, overlays) are discovered and revealed.

5. **04_execution_and_playback.md**  
   Execution realism and BotLens playback rules.

6. **05_engineering_principles.md**  
   Software engineering standards for extending the system safely.

---

## Core Rule

> **Quant-Trad always behaves as if it is trading live.**

Backtests are simulated live runs.
Playback is a debugger.
Visualization is an audit surface.
Convenience never overrides correctness.

---

## Design Intent (Important)

Quant-Trad is a **framework first**, not a single strategy or indicator.

The engine is designed to:
- support incremental discovery of market structure
- preserve temporal causality
- remain explainable under replay
- scale in complexity without collapsing abstractions

Any shortcut that violates these goals is a bug, not an optimization.

---

## When in Doubt

If you are unsure whether a change is valid, ask:
- Would this be knowable at that candle in live trading?
- Which layer does this logic belong to?
- Does this preserve determinism and replayability?
- Are errors loud and observable?

If the answer is unclear, default to stricter behavior.
