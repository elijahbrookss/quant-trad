# BotLens Playback & Visualization Rules

BotLens is not cosmetic.
It is a validation tool for strategy correctness.

---

## BotLens Purpose

During playback, users must be able to visually confirm:
- Indicators appear when valid
- Signals fire where expected
- Strategy reacts correctly
- Trades are executed properly
- Stops and targets behave as intended

---

## Playback Speed Rules

- Backtests may run fast overall
- Playback MUST slow down during open trades

Open trade emphasis:
- Entry
- Stop placement
- Target placement
- Stop adjustments
- Exit

---

## Intrabar Simulation (Critical)

If strategy timeframe is coarse (e.g., 1H):
- BotLens SHOULD optionally pull lower timeframe candles (e.g., 1m)
- This allows:
  - Realistic stop/target hits
  - No waiting for next HTF candle
  - Animated trade lifecycle

This provides vital context for validating strategy behavior.

---

## Overlay Timing

During playback:
- Only show overlays that are valid at that time
- Never reveal future indicator artifacts

Visualization must respect walk-forward constraints.

---

## Agent Warning

If BotLens playback looks correct but violates walk-forward timing,
the visualization is lying.

Correctness > convenience.
