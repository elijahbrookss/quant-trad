# Quant-Trad Layer Model

Quant-Trad is intentionally split into layers.
Each layer has a single responsibility.

Layers must never simulate behavior that belongs to another layer.

---

## QuantLab — Research Layer

Purpose:
- Explore candles
- Build indicators
- Visualize overlays
- Generate indicator signals

QuantLab is allowed to be idealized.
It does not simulate execution realism.

---

## Strategy — Decision Layer

Purpose:
- Attach indicators
- Interpret indicator signals
- Decide *when* a trade should occur

Strategies:
- define intent
- express logic
- do not apply fees, slippage, or execution rules

A strategy decides *what* to do — not *how* it is executed.

---

## Bot — Execution Layer

Purpose:
- Execute a strategy through time
- Apply risk, position sizing, fees, slippage, and order behavior
- Produce trades, metrics, and playback

Bots are never idealized.

If execution realism is required, it belongs here and nowhere else.

---

## Layer Integrity Rule

If logic crosses layers for convenience, it is a bug.
