# Quant-Trad Engine Overview (Plain Language)

This document explains the "engines" in Quant-Trad as simply as possible.
It is written for someone who has never coded before.

---

## Quick Mental Model

Think of Quant-Trad like a factory line:

1. Market data comes in (candles)
2. Indicator state is updated step-by-step
3. Signals and overlays are produced from that same state
4. Strategy decides whether conditions are met
5. Bot executes orders with risk/execution realism
6. Playback shows what happened and why

The key idea: all steps should use the same timeline of knowledge.

---

## The Engines We Have

## 1) Indicator State Engine

Purpose:
- Keeps indicator state up to date as each candle arrives.

How it works:
- `initialize` -> create empty state
- `apply_bar` -> update state with one new candle
- `snapshot` -> produce a safe "current view" of known data

Why it matters:
- Signals and overlays should come from this snapshot.
- This prevents drift between what we calculate and what we display.

---

## 2) Signal Runtime (Indicator Signal Evaluation)

Purpose:
- Reads indicator snapshots and emits indicator signals.

Important note:
- This is not the trade execution engine.
- It is signal evaluation built on top of indicator state snapshots.

Current status:
- Runtime per-bar snapshot signal emission is the canonical path.

Direction:
- Do not introduce batch/research signal generation paths for platform behavior.

---

## 3) Overlay Runtime (Indicator Overlay Projection)

Purpose:
- Turns indicator snapshot data into chart visuals (boxes, markers, bubbles, lines).

Rule of thumb:
- Overlays should be derived from the same snapshot timeline as signals.
- If signals and overlays use different timelines/timeframes, trust drops.

---

## 4) Strategy Evaluation Engine (Decision Layer)

Purpose:
- Combines indicator signals according to strategy rules.

Example:
- Rule can require one indicator condition, or multiple conditions (`all` / `any`).

What it does not do:
- It does not execute trades with fees/slippage/ATM realism.

---

## 5) Bot Runtime Engine (Execution Layer)

Purpose:
- Runs strategy decisions through realistic execution behavior over time.

Includes:
- risk controls
- fills/exits
- fees/slippage
- order lifecycle

This is the true execution engine.

---

## 6) Playback Engine (Audit/Debugger Surface)

Purpose:
- Replays outcomes so humans can verify timing and decisions.

Should show:
- what was known at each moment
- what signal/decision happened
- what execution outcome followed

Playback is not a demo; it is a correctness debugger.

---

## How They Work Together

Canonical flow:

1. Candle arrives
2. Indicator State Engine updates
3. Snapshot is produced
4. Signal Runtime reads snapshot
5. Overlay Runtime reads snapshot
6. Strategy Evaluation consumes signals
7. Bot Runtime executes decisions
8. Playback visualizes the same timeline

---

## Where Confusion Usually Happens

1. "Signal engine" vs "execution engine"
- Signal runtime evaluates indicator signals.
- Bot runtime executes trades.

2. Multiple paths for similar outputs
- If overlays and signals are computed by different paths/timeframes, they can disagree.

3. Indicator-specific logic outside indicator modules
- This makes ownership unclear and increases drift risk.

---

## Recommended Structure (Private Plugin-Style, No Marketplace)

For each indicator:

- `src/indicators/<name>/domain/`
- `src/indicators/<name>/signals/`
- `src/indicators/<name>/overlays/`
- `src/indicators/<name>/indicator.py`
- `src/indicators/<name>/plugin.py`

Benefits:
- One home per indicator
- Easier maintenance
- Cleaner onboarding
- Better consistency across signals/overlays/runtime

---

## Target Path (What We Are Standardizing To)

1. Indicator runtime behavior lives inside indicator modules.
2. Plugin files are thin wiring only (manifest + engine factory), not business logic.
3. Signals and overlays for an indicator come from the same indicator-local runtime code.
4. Strategy composes indicators; indicators do not own cross-indicator strategy composition.
5. No parallel fallback path for the same artifact type.

### No-Fallback Policy

For any artifact class (signal, overlay, projection):
- one canonical runtime path
- one canonical contract
- fail loud when required payload fields are missing

This prevents silent drift between "what fired" and "what was shown."

---

## Non-Negotiable Platform Rule

All derived outputs should come from one runtime timeline:

`initialize -> apply_bar -> snapshot`

When everything uses this timeline, the platform stays explainable and trustworthy.
